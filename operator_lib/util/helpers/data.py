import datetime
import ray
import typing
import operator_lib.util as util
from operator_lib.util.helpers.timescale import get_timescale_dataset_local, get_timescale_dataset_remote
from operator_lib.util.helpers.kafka import get_kafka_dataset_local, get_kafka_dataset_remote
from operator_lib.util.helpers.ts_wrapper import get_ts_wrapper_dataset_local, get_ts_wrapper_dataset_remote
from operator_lib.util.config import MissingConfigValueError

ALWAYS_PREFER_KAFKA = False # Can be used to debug kafka data source

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


def __provide_historic_data(duration: datetime.timedelta, require_full_duration: bool = False, remote: bool = True):
    ds: typing.List[ray.ObjectRef[ray.data.Dataset]] = []
    dep_config = util.DeploymentConfig()
    config_json = util.load_operator_config_json(dep_config)
    opr_config = util.OperatorConfig(config_json)
    for topic in opr_config.inputTopics:
        if topic.name.startswith("urn_infai_ses_service") and not ALWAYS_PREFER_KAFKA:
            f = __read_timescale(
                opr_config.config, dep_config, topic, duration, require_full_duration, remote)

        else:
            f = get_kafka_dataset_local
            if remote:
                f = get_kafka_dataset_remote.remote
            f = f(dep_config.config_bootstrap_servers,
                topic, dep_config.pipeline_id, duration, require_full_duration)
        ds.append(f)
    return ds


def __read_timescale(config, dep_config, topic, duration, require_full_duration, remote):
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
        return f(config.ts_conn, topic, duration, require_full_duration)

    if config.ts_wrapper_url and dep_config.senergy_token:
        f = get_ts_wrapper_dataset_local
        if remote:
            f = get_ts_wrapper_dataset_remote.remote
        return f(config.ts_wrapper_url, dep_config.senergy_token, topic,
                 duration, require_full_duration)

    raise MissingConfigValueError(
        f"cannot read history for topic {topic.name}: neither a database connection "
        f"nor an authorised reader is configured. Set 'ts_conn' in the operator "
        f"config, which is what the flow engine gives a deployed operator, or set "
        f"'ts_wrapper_url' together with a SENERGY_TOKEN in the environment, which "
        f"is what an operator development environment gives a run")
