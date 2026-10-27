import copy
import datetime
import ray
import typing
import operator_lib.util as util
from operator_lib.util import clock
from operator_lib.util.model import InputTopic
from operator_lib.util.helpers.timescale import get_timescale_dataset_local, get_timescale_dataset_remote
from operator_lib.util.helpers.timescale import get_timescale_sql_datasets_local, get_timescale_sql_datasets_remote
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

# The values `role` takes below: "input" selects the config's inputTopics,
# "training" its trainingTopics (see OperatorConfig for what those are).
_ROLES = ("input", "training")

# Set by MLOperator.__evaluate for as long as its replay runs. A training topic
# is the ground truth the replay is scored against and something a deployed
# operator never receives, so infer() reading it through
# provide_historic_data would score the label against itself. Driver state,
# like the clock: infer() runs in the driver, and so does the topic selection
# that checks this.
_training_topics_withheld = False


def withhold_training_topics(withheld: bool):
    """
    Refuse (True) or allow again (False) every read of the training topics.
    For MLOperator.__evaluate, which reads them itself before its replay and
    withholds them from infer() during it; not for operator code.
    """
    global _training_topics_withheld
    _training_topics_withheld = withheld


def _check_role(role):
    # Checked before the config is parsed, so a misspelt role fails the same
    # way with or without a CONFIG to read.
    if role not in _ROLES:
        raise ValueError(
            f"unknown topic role {role!r}, expected one of "
            f"{', '.join(repr(r) for r in _ROLES)}")
    if role == "training" and _training_topics_withheld:
        raise RuntimeError(
            "training topics are withheld while the evaluation replays the test "
            "window: they are the ground truth infer() is scored against, and a "
            "deployed operator never receives them. Read them in train()")


def _topics_for_role(opr_config, role):
    return opr_config.trainingTopics if role == "training" else opr_config.inputTopics


_READERS = ("dataset", "sql")


def _check_reader(reader, changes_only, require_full_duration):
    # Checked before the config is parsed, like the role.
    if reader not in _READERS:
        raise ValueError(
            f"unknown reader {reader!r}, expected one of "
            f"{', '.join(repr(r) for r in _READERS)}")
    if changes_only and reader != "sql":
        raise ValueError("changes_only=True needs reader=\"sql\"")
    if reader == "sql" and require_full_duration:
        raise ValueError(
            "require_full_duration=True is not supported with reader=\"sql\": waiting "
            "for one input inside the single reading task would block every other input")


def provide_historic_data(duration: datetime.timedelta, require_full_duration: bool = False, role: str = "input", reader: str = "dataset", changes_only: bool = False) -> typing.List[ray.ObjectRef[ray.data.Dataset]]:
    """
    This method can be used in the train method of your model to get historic data from the input topics. It will return a list of datasets, one for each input topic. The datasets will contain data from the specified duration time. If require_full_duration is set to True, the method will wait until it can provide data for the full duration. This can lead to long waiting times if there is not enough data in the input topics. Therefore, it should only be used if strictly necessary. It is genreally recommended to train with the available data and use the need_retraining method to trigger retraining if more data is available. Expect up to 10% shorter duration than requested with require_full_duration = True.
    role selects the topics: "input" (the default) reads the input topics, "training" reads the training topics instead, one dataset each in config order. Training topics are series the operator may learn from but never receives at runtime, such as a label; the list is empty unless the launch assigned some, and a deployed operator has none. While the evaluation replays the test window, role="training" raises RuntimeError. Any other value raises ValueError.
    reader selects how inputs read from Timescale over a database connection (a DeviceId input named urn_infai_ses_service_*, with TS_CONN or 'ts_conn' set) are read: "dataset" (the default) as before, one lazy dataset per device; "sql" in a single Ray task over a single connection, one query per device, which is much faster for many small inputs. The return value has the same shape either way. Other inputs are read as before. reader="sql" with require_full_duration=True raises ValueError, and so does an unknown reader. A table that fails with a connection error is retried (3 attempts, 5 s apart); if it still fails, that input and the ones after it in config order fail when read, the ones before it are unaffected.
    changes_only=True (reader="sql" only, else ValueError) keeps per table just the rows whose value differs from the previous or the next row in at least one mapped column, ordered by time and then by the mapped columns. The first and the last row of the window stay, and so does every distinct value of rows that share a timestamp.
    """

    return __provide_historic_data(duration, require_full_duration, True, role=role, reader=reader, changes_only=changes_only)


def provide_historic_data_local(duration: datetime.timedelta, require_full_duration: bool = False, role: str = "input", reader: str = "dataset", changes_only: bool = False) -> typing.List[ray.data.Dataset]:
    """
    This method can be used in the inference method of your operator to get historic data from the input topics. It will return a list of datasets, one for each input topic. The datasets will contain data from the specified duration time. If require_full_duration is set to True, the method will wait until it can provide data for the full duration. This can lead to long waiting times if there is not enough data in the input topics. Therefore, it should only be used if strictly necessary. Expect up to 10% shorter duration than requested with require_full_duration = True.
    Compared to provide_historic_data, this method has better performance, but comes with the drawback that it will load all data into memory. Therefore, it should only be used if the amount of data is small enough to fit into memory.
    role selects the topics exactly as in provide_historic_data: "input" (the default) or "training".
    reader and changes_only as in provide_historic_data; with reader="sql" the query runs in the calling process.
    """

    return __provide_historic_data(duration, require_full_duration, False, role=role, reader=reader, changes_only=changes_only)


def __provide_historic_data(duration: datetime.timedelta, require_full_duration: bool = False, remote: bool = True, end: typing.Optional[datetime.datetime] = None, role: str = "input", reader: str = "dataset", changes_only: bool = False):
    # The bound comes from the clock, the clock from the deployment config.
    # Operator code -- which train()/infer() are -- never passes an end of its
    # own; it has nothing to pass. Resolved here in the driver rather than
    # inside the readers themselves: a Ray worker does not share the driver's
    # module-level clock state, so the bound has to travel as an argument.
    #
    # The role is resolved here for the same reason: the readers run on Ray
    # workers with whatever library version the cluster image carries, so they
    # receive one topic each, exactly as before, and never learn its role.
    _check_role(role)
    _check_reader(reader, changes_only, require_full_duration)
    end = end if end is not None else clock.fixed()
    ds: typing.List[ray.ObjectRef[ray.data.Dataset]] = []
    dep_config = util.DeploymentConfig()
    config_json = util.load_operator_config_json(dep_config)
    opr_config = util.OperatorConfig(config_json)
    topics = _topics_for_role(opr_config, role)
    if reader == "sql":
        return __read_topics_sql(opr_config.config, dep_config, topics, duration, remote, end, changes_only)
    for topic in topics:
        ds.append(__read_topic(opr_config.config, dep_config, topic, duration, require_full_duration, remote, end))
    return ds


def _sql_job(topic):
    """
    The entries of one input for the SQL reader, [(device or None, topic of that
    one device)], or None if the input is not read from Timescale by DSN. The
    device is only set for an input that names several, where it becomes the
    column device_id, exactly as in __read_devices.
    """
    if topic.filterType != "DeviceId" or not topic.name.startswith("urn_infai_ses_service"):
        return None
    devices = _split_devices(topic.filterValue)
    if not devices:
        return None
    if len(devices) == 1:
        return [(None, _topic_for_device(topic, devices[0]))]
    return [(device, _topic_for_device(topic, device)) for device in devices]


def __read_topics_sql(config, dep_config, topics, duration, remote, end, changes_only):
    """
    reader="sql": every input that would be read from Timescale over the DSN is
    read by one task over one connection; the rest goes through __read_topic as
    always. The result is in config order either way.
    """
    ts_conn = None if ALWAYS_PREFER_KAFKA else _resolve_ts_conn(config, dep_config)
    jobs = {}
    if ts_conn:
        for i, topic in enumerate(topics):
            job = _sql_job(topic)
            if job is not None:
                jobs[i] = job
    sql_results = []
    if jobs:
        job_list = list(jobs.values())
        if remote:
            refs = get_timescale_sql_datasets_remote.options(num_returns=len(job_list)).remote(
                ts_conn, job_list, duration, end, changes_only)
            # One return is one ref, not a list of one.
            sql_results = [refs] if len(job_list) == 1 else list(refs)
        else:
            sql_results = get_timescale_sql_datasets_local(ts_conn, job_list, duration, end, changes_only)
    by_index = dict(zip(jobs.keys(), sql_results))
    return [by_index[i] if i in by_index
            else __read_topic(config, dep_config, topic, duration, False, remote, end)
            for i, topic in enumerate(topics)]


def read_input_window(start: datetime.datetime, end: datetime.datetime, role: str = "input") -> typing.List[typing.Tuple[InputTopic, "pandas.DataFrame"]]:
    """
    Read every input topic over [start, end) and return one pandas frame per
    topic, in topic order, alongside the InputTopic it came from. With
    role="training", the training topics instead, in their own config order:
    the evaluation reads them for the metric alone.

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
    _check_role(role)
    duration = end - start
    dep_config = util.DeploymentConfig()
    config_json = util.load_operator_config_json(dep_config)
    opr_config = util.OperatorConfig(config_json)
    result = []
    for topic in _topics_for_role(opr_config, role):
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
    if topic.filterType == "DeviceId":
        devices = _split_devices(topic.filterValue)
        if len(devices) > 1:
            return __read_devices(config, dep_config, topic, devices, duration, require_full_duration, remote, end)
        if len(devices) == 1 and devices[0] != topic.filterValue:
            # "a, " or " a": one device, written the way the flow engine joined it.
            topic = _topic_for_device(topic, devices[0])

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


def _split_devices(filter_value):
    """
    The devices of a DeviceId topic. The flow engine joins the several devices of
    one cloud-operator input with commas into filterValue; the live path splits
    them in create_filter_handler, and so does the history here. Empty entries
    are dropped and repeated devices read once, in first-seen order.
    """
    devices = []
    for entry in (filter_value or "").split(","):
        entry = entry.strip()
        if entry and entry not in devices:
            devices.append(entry)
    return devices


def _topic_for_device(topic, device):
    single = copy.deepcopy(topic)
    single.filterValue = device
    return single


def _with_device_column(batch, device):
    batch["device_id"] = device
    return batch


def _union_device_datasets_impl(devices, *datasets):
    tagged = [d.map_batches(_with_device_column, batch_format="pandas", fn_kwargs={"device": device})
              for device, d in zip(devices, datasets)]
    return tagged[0].union(*tagged[1:])


@ray.remote
def _union_device_datasets_remote(devices, *datasets) -> ray.data.Dataset:
    # Top-level ObjectRef arguments arrive resolved, so these are the datasets
    # the per-device readers returned.
    return _union_device_datasets_impl(devices, *datasets)


def __read_devices(config, dep_config, topic, devices, duration, require_full_duration, remote, end):
    """
    Read a DeviceId topic with several devices as one dataset: each device goes
    through the ordinary dispatch as a topic of its own, and the rows of all of
    them come back together with a column device_id. A topic with one device
    never gets here and has no such column. duration, require_full_duration and
    end apply to every device alike.
    """
    parts = [__read_topic(config, dep_config, _topic_for_device(topic, device),
                          duration, require_full_duration, remote, end)
             for device in devices]
    if remote:
        return _union_device_datasets_remote.remote(devices, *parts)
    return _union_device_datasets_impl(devices, *parts)


def _log_import_source_once(topic, reason):
    key = (topic.name, topic.filterValue)
    if key in _logged_import_sources:
        return
    _logged_import_sources.add(key)
    logger.info(f"import topic {topic.name} ({topic.filterValue}): {reason}")


def _resolve_ts_conn(config, dep_config):
    """
    The database connection string: TS_CONN from the environment where set, else
    'ts_conn' of the operator config, which a flow engine that does not yet use a
    Secret still hands over.
    """
    return dep_config.ts_conn or config.ts_conn


def __read_export(config, dep_config, topic, entry, duration, require_full_duration, remote, end):
    """
    Pick the read path for an import topic that has an export, with the same
    choice as __read_timescale and for the same reasons: the DSN wins where both
    are present, timescale-wrapper otherwise.
    """
    ts_conn = _resolve_ts_conn(config, dep_config)
    if ts_conn:
        f = get_export_dataset_local
        if remote:
            f = get_export_dataset_remote.remote
        return f(ts_conn, topic, entry, duration, require_full_duration, end)

    if config.ts_wrapper_url and dep_config.senergy_token:
        f = get_ts_wrapper_export_dataset_local
        if remote:
            f = get_ts_wrapper_export_dataset_remote.remote
        return f(config.ts_wrapper_url, dep_config.senergy_token, topic, entry,
                 duration, require_full_duration, end)

    raise MissingConfigValueError(
        f"cannot read history for topic {topic.name} from export {entry.export_id}: "
        f"neither a database connection nor an authorised reader is configured. Set "
        f"TS_CONN in the environment, or 'ts_conn' in the operator config, which is what "
        f"the flow engine gives a deployed operator, or set 'ts_wrapper_url' together with a SENERGY_TOKEN in "
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
    ts_conn = _resolve_ts_conn(config, dep_config)
    if ts_conn:
        f = get_timescale_dataset_local
        if remote:
            f = get_timescale_dataset_remote.remote
        return f(ts_conn, topic, duration, require_full_duration, end)

    if config.ts_wrapper_url and dep_config.senergy_token:
        f = get_ts_wrapper_dataset_local
        if remote:
            f = get_ts_wrapper_dataset_remote.remote
        return f(config.ts_wrapper_url, dep_config.senergy_token, topic,
                 duration, require_full_duration, end)

    raise MissingConfigValueError(
        f"cannot read history for topic {topic.name}: neither a database connection "
        f"nor an authorised reader is configured. Set TS_CONN in the "
        f"environment, or 'ts_conn' in the operator config, which is what the flow "
        f"engine gives a deployed operator, or set 'ts_wrapper_url' together with a SENERGY_TOKEN in the environment, which "
        f"is what an operator development environment gives a run")
