import datetime
import ray
import typing
import operator_lib.util as util
from operator_lib.util import clock
from operator_lib.util.model import InputTopic
from operator_lib.util.helpers.timescale import get_timescale_dataset_local, get_timescale_dataset_remote
from operator_lib.util.helpers.timescale import get_export_dataset_local, get_export_dataset_remote
from operator_lib.util.helpers.kafka import get_kafka_dataset_local, get_kafka_dataset_remote
from operator_lib.util.helpers.ts_wrapper import get_ts_wrapper_dataset_local, get_ts_wrapper_dataset_remote
from operator_lib.util.helpers.ts_wrapper import get_ts_wrapper_export_dataset_local, get_ts_wrapper_export_dataset_remote
from operator_lib.util.helpers.exports import find_import_export
from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.logger import logger

ALWAYS_PREFER_KAFKA = False # Can be used to debug kafka data source

# Import topics whose chosen source has been logged already. The evaluation
# replay reads every topic once per message, and one line per read would bury
# the one line that says where a run's history comes from.
_logged_import_sources = set()

def provide_historic_data(duration: datetime.timedelta, require_full_duration: bool = False) -> typing.List[ray.ObjectRef[ray.data.Dataset]]:
    """
    This method can be used in the train method of your model to get historic data from the input topics. It will return a list of datasets, one for each input topic. The datasets will contain data from the specified duration time. If require_full_duration is set to True, the method will wait until it can provide data for the full duration. This can lead to long waiting times if there is not enough data in the input topics. Therefore, it should only be used if strictly necessary. It is genreally recommended to train with the available data and use the need_retraining method to trigger retraining if more data is available. Expect up to 10% shorter duration than requested with require_full_duration = True.
    """

    return __provide_historic_data(duration, require_full_duration, True)


def provide_historic_data_local(duration: datetime.timedelta, require_full_duration: bool = False) -> typing.List[ray.data.Dataset]:
    """
    This method can be used in the inference method of your operator to get historic data from the input topics. It will return a list of datasets, one for each input topic. The datasets will contain data from the specified duration time. If require_full_duration is set to True, the method will wait until it can provide data for the full duration. This can lead to long waiting times if there is not enough data in the input topics. Therefore, it should only be used if strictly necessary. Expect up to 10% shorter duration than requested with require_full_duration = True.
    Compared to provide_historic_data, this method has better performance, but comes with the drawback that it will load all data into memory. Therefore, it should only be used if the amount of data is small enough to fit into memory.
    """

    return __provide_historic_data(duration, require_full_duration, False)


def __provide_historic_data(duration: datetime.timedelta, require_full_duration: bool = False, remote: bool = True, end: typing.Optional[datetime.datetime] = None):
    # The bound comes from the clock, the clock from the deployment config.
    # Operator code -- which train()/infer() are -- never passes an end of its
    # own; it has nothing to pass. Resolved here in the driver rather than
    # inside the readers themselves: a Ray worker does not share the driver's
    # module-level clock state, so the bound has to travel as an argument.
    end = end if end is not None else clock.fixed()
    ds: typing.List[ray.ObjectRef[ray.data.Dataset]] = []
    dep_config = util.DeploymentConfig()
    config_json = util.load_operator_config_json(dep_config)
    opr_config = util.OperatorConfig(config_json)
    for topic in opr_config.inputTopics:
        ds.append(__read_topic(opr_config.config, dep_config, topic, duration, require_full_duration, remote, end))
    return ds


def read_input_window(start: datetime.datetime, end: datetime.datetime) -> typing.List[typing.Tuple[InputTopic, "pandas.DataFrame"]]:
    """
    Read every input topic over [start, end) and return one pandas frame per
    topic, in topic order, alongside the InputTopic it came from.

    For the evaluation replay in MLOperator.__evaluate, not for operator code:
    provide_historic_data and provide_historic_data_local take their bound from
    the clock, because operator code never sees training_end directly. This
    function takes the window explicitly instead, because the evaluation is the
    one caller allowed to move it, message by message, as it replays the test
    window.

    Always reads locally (duration = end - start, require_full_duration=False)
    and converts each dataset with to_pandas() before returning. Ray must
    already be initialised by the caller -- this function does not start or
    stop it, so that a replay which also needs Ray for infer()'s own
    provide_historic_data calls is not torn down between reads.
    """
    duration = end - start
    dep_config = util.DeploymentConfig()
    config_json = util.load_operator_config_json(dep_config)
    opr_config = util.OperatorConfig(config_json)
    result = []
    for topic in opr_config.inputTopics:
        dataset = __read_topic(opr_config.config, dep_config, topic, duration, False, False, end)
        result.append((topic, dataset.to_pandas()))
    return result


def __read_topic(config, dep_config, topic, duration, require_full_duration, remote, end):
    """
    Route one input topic to its reader -- timescale-backed or kafka-backed,
    local or remote. Shared by __provide_historic_data and read_input_window so
    that the evaluation's fixed-window reads go through exactly the same
    dispatch a deployed operator's do.

    An import topic (filterType ImportId) with an entry in the config's
    import_exports is read from the export the deployer named, because its
    Kafka topic keeps days where the export keeps everything. Without an entry
    it is read from Kafka, as it always was.
    """
    if topic.filterType == "ImportId":
        entry = None
        if ALWAYS_PREFER_KAFKA:
            reason = "ALWAYS_PREFER_KAFKA is set, reading Kafka"
        else:
            # Raises on an unparsable value: only here, for a topic that could
            # use it, so a device-only operator never trips on it.
            entry = find_import_export(config, topic)
            if entry is None:
                reason = "no entry in import_exports, reading Kafka"
            else:
                reason = f"reading export {entry.export_id} (table {entry.table})"
        _log_import_source_once(topic, reason)
        if entry is not None:
            return __read_export(config, dep_config, topic, entry, duration, require_full_duration, remote, end)

    if topic.name.startswith("urn_infai_ses_service") and not ALWAYS_PREFER_KAFKA:
        return __read_timescale(config, dep_config, topic, duration, require_full_duration, remote, end)

    f = get_kafka_dataset_local
    if remote:
        f = get_kafka_dataset_remote.remote
    return f(dep_config.config_bootstrap_servers,
        topic, dep_config.pipeline_id, duration, require_full_duration, end)


def _log_import_source_once(topic, reason):
    key = (topic.name, topic.filterValue)
    if key in _logged_import_sources:
        return
    _logged_import_sources.add(key)
    logger.info(f"import topic {topic.name} ({topic.filterValue}): {reason}")


def __read_export(config, dep_config, topic, entry, duration, require_full_duration, remote, end):
    """
    Pick the read path for an import topic that has an export, with the same
    choice as __read_timescale and for the same reasons: the DSN wins where both
    are present, timescale-wrapper otherwise.
    """
    if config.ts_conn:
        f = get_export_dataset_local
        if remote:
            f = get_export_dataset_remote.remote
        return f(config.ts_conn, topic, entry, duration, require_full_duration, end)

    if config.ts_wrapper_url and dep_config.senergy_token:
        f = get_ts_wrapper_export_dataset_local
        if remote:
            f = get_ts_wrapper_export_dataset_remote.remote
        return f(config.ts_wrapper_url, dep_config.senergy_token, topic, entry,
                 duration, require_full_duration, end)

    raise MissingConfigValueError(
        f"cannot read history for topic {topic.name} from export {entry.export_id}: "
        f"neither a database connection nor an authorised reader is configured. Set "
        f"'ts_conn' in the operator config, which is what the flow engine gives a "
        f"deployed operator, or set 'ts_wrapper_url' together with a SENERGY_TOKEN in "
        f"the environment, which is what an operator development environment gives a run")


def __read_timescale(config, dep_config, topic, duration, require_full_duration, remote, end):
    """
    Pick the read path for a timescale-backed topic.

    A direct database connection where the deployment was given one, and
    timescale-wrapper where it was given a platform token instead. The two differ
    in who is authorised: the DSN reaches every series regardless of who started
    the operator, while the wrapper checks the caller's own execute permission on
    the device. So the DSN belongs to a deployment whose code is a reviewed
    artefact, and the wrapper to one running code somebody is still writing.

    The DSN wins where both are present, because it is the faster path -- it
    shards the read across ray workers, which one HTTP response cannot.
    """
    if config.ts_conn:
        f = get_timescale_dataset_local
        if remote:
            f = get_timescale_dataset_remote.remote
        return f(config.ts_conn, topic, duration, require_full_duration, end)

    if config.ts_wrapper_url and dep_config.senergy_token:
        f = get_ts_wrapper_dataset_local
        if remote:
            f = get_ts_wrapper_dataset_remote.remote
        return f(config.ts_wrapper_url, dep_config.senergy_token, topic,
                 duration, require_full_duration, end)

    raise MissingConfigValueError(
        f"cannot read history for topic {topic.name}: neither a database connection "
        f"nor an authorised reader is configured. Set 'ts_conn' in the operator "
        f"config, which is what the flow engine gives a deployed operator, or set "
        f"'ts_wrapper_url' together with a SENERGY_TOKEN in the environment, which "
        f"is what an operator development environment gives a run")
