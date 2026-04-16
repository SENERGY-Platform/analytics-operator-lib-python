import datetime
import ray
import typing
import operator_lib.util as util
from operator_lib.util.helpers.timescale import get_timescale_dataset
from operator_lib.util.helpers.kafka import get_kafka_dataset

import json

ALWAYS_PREFER_KAFKA = False # Can be used to debug kafka data source

def provide_historic_data(duration: datetime.timedelta, require_full_duration: bool = False) -> typing.List[ray.ObjectRef[ray.data.Dataset]]:
    """
    This method can be used in the train method of your model to get historic data from the input topics. It will return a list of datasets, one for each input topic. The datasets will contain data from the specified duration time. If require_full_duration is set to True, the method will wait until it can provide data for the full duration. This can lead to long waiting times if there is not enough data in the input topics. Therefore, it should only be used if strictly necessary. It is genreally recommended to train with the available data and use the need_retraining method to trigger retraining if more data is available. Expect up to 10% shorter duration than requested with require_full_duration = True.
    """
    
    ds: typing.List[ray.ObjectRef[ray.data.Dataset]] = []
    dep_config = util.DeploymentConfig()
    config_json = json.loads(dep_config.config)
    opr_config = util.OperatorConfig(config_json)
    for topic in opr_config.inputTopics:
        if topic.name.startswith("urn_infai_ses_service") and not ALWAYS_PREFER_KAFKA:
            ds.append(get_timescale_dataset.remote(
                opr_config.config.ts_conn, topic, duration, require_full_duration))
        else:
            ds.append(get_kafka_dataset.remote(dep_config.config_bootstrap_servers,
                topic, dep_config.pipeline_id, duration, require_full_duration))
    return ds
