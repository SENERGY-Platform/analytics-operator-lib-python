import json
import ray
import datetime
from operator_lib.util.model import InputTopic
import time


@ray.remote
def get_kafka_dataset(bootstrap: str, input_topic: InputTopic, pipeline_id: str, duration: datetime.timedelta, require_full_duration: bool = False) -> ray.data.Dataset:
    if duration > datetime.timedelta(days=365):
        raise ValueError("Duration too long, refusing to read from Kafka. Please use a more reasonable duration.")
    
    # Lazy import avoids importing operator_lib.util during package initialization.
    from operator_lib.util import gen_identifiers

    now = datetime.datetime.now()
    cutoff = now - duration
    filter = gen_identifiers(name=input_topic.name, f_type=input_topic.filterType,
                                       f_value=input_topic.filterValue, pipeline_id=pipeline_id)

    def __filter_kafka_msg(msg: dict) -> bool:
        msg_timestamp = datetime.datetime.fromtimestamp(msg["timestamp"] / 1000.0)
        if msg_timestamp < cutoff:
            return False        
        payload = json.loads(msg["value"])
        for f in filter:
            if payload.get(f["key"]) != f["value"]:
                return False
        return True        
    
    ds = ray.data.read_kafka(bootstrap_servers=bootstrap, topics=input_topic.name).filter(__filter_kafka_msg)
    if require_full_duration:
        msg = ds.take(1)
        msg_timestamp = datetime.datetime.fromtimestamp(msg["timestamp"] / 1000.0)
        time.sleep(max(0, (cutoff - msg_timestamp).total_seconds()))
    return ds


