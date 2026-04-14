import json
import ray
import datetime
from operator_lib.util.model import InputTopic
import time
import typing


@ray.remote
def get_kafka_dataset(bootstrap: str, input_topic: InputTopic, pipeline_id: str, duration: datetime.timedelta, require_full_duration: bool = False) -> ray.data.Dataset:
    while True:
        ds, cutoff = __get_kafka_dataset(bootstrap, input_topic, pipeline_id, duration)
        if require_full_duration:
            msg = ds.take(1)
            if len(msg) == 0:
                print(f"No messages found in Kafka, sleeping for {duration} before retrying...") # TODO use logger
                time.sleep(duration.total_seconds())
                continue
            msg_timestamp = datetime.datetime.fromtimestamp(msg[0]["timestamp"] / 1000.0)
            sleep_for = (cutoff - msg_timestamp).total_seconds()
            if sleep_for > 0:
                time.sleep(sleep_for)
                continue
        return ds


def __get_kafka_dataset(bootstrap: str, input_topic: InputTopic, pipeline_id: str, duration: datetime.timedelta) -> typing.Tuple[ray.data.Dataset, datetime.datetime]:
    if duration > datetime.timedelta(days=365):
        raise ValueError("Duration too long, refusing to read from Kafka. Please use a more reasonable duration.")
    
    # Lazy import avoids importing operator_lib.util during package initialization.
    from operator_lib.util import gen_identifiers

    now = datetime.datetime.now()
    cutoff = now - duration
    filter = gen_identifiers(name=input_topic.name, f_type=input_topic.filterType,
                                       f_value=input_topic.filterValue, pipeline_id=pipeline_id)
    print(f"Filtering for {json.dumps(filter, indent=2)}") # TODO use logger

    def __filter_kafka_msg(msg: dict) -> bool:
        msg_timestamp = datetime.datetime.fromtimestamp(msg["timestamp"] / 1000.0)
        if msg_timestamp < cutoff:
            return False        
        payload = json.loads(msg["value"])
        for f in filter:
            if payload.get(f["key"]) != f["value"]:
                print(f"Message {json.dumps(msg, indent=2)} does not match filter {json.dumps(f, indent=2)}") # TODO use logger
                return False
        return True        
    
    return ray.data.read_kafka(bootstrap_servers=bootstrap, topics=input_topic.name).filter(__filter_kafka_msg), cutoff
    