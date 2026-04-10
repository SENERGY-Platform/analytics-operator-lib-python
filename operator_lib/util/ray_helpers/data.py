

import datetime
import ray
import typing
import operator_lib.util as util
from operator_lib.util.model import InputTopic
from operator_lib.util.ray_helpers.timescale import get_timescale_dataset
from .init import init_ray_once
import json




def provide_historic_data(duration: datetime.timedelta, require_full_duration: bool = False) -> typing.List[ray.data.Dataset]:
    init_ray_once()
    ds: typing.List[ray.data.Dataset] = []
    dep_config = util.DeploymentConfig()
    config_json = json.loads(dep_config.config)
    opr_config = util.OperatorConfig(config_json)
    for topic in opr_config.inputTopics:
        if topic.name.startswith("urn_infai_ses_service"):
            ds.append(get_timescale_dataset.remote(opr_config.config.ts_conn, topic, duration, require_full_duration))
        else:
            ds.append(__get_kafka_dataset(
                topic, duration, require_full_duration))
    return ray.get(ds)





def __get_kafka_dataset(conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False) -> ray.data.Dataset:
    raise NotImplementedError # TODO
