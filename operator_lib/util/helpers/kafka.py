import json
import ray
import datetime
from operator_lib.util.model import InputTopic
import time
import typing
from ray.data.expressions import col, udf, DataType
import pyarrow as pa


@udf(return_dtype=DataType.string())
def json_get(value_col, key: str):
    """Extract a value from JSON-encoded binary/string data.
    
    Args:
        value_col: PyArrow array of binary or string (JSON-encoded) data
        key: The key to extract from the JSON object
    
    Returns:
        PyArrow array of extracted values as strings
    """
    import json
    
    results = []
    for val in value_col:
        try:
            if val is None:
                results.append(None)
            else:
                # Decode bytes to string if needed
                if isinstance(val.as_py(), bytes):
                    json_str = val.as_py().decode('utf-8')
                else:
                    json_str = val.as_py()
                
                data = json.loads(json_str)
                results.append(str(data.get(key)))
        except (json.JSONDecodeError, AttributeError, TypeError):
            results.append(None)
    
    return pa.array(results, type=pa.string())


@ray.remote
def get_kafka_dataset_remote(bootstrap: str, input_topic: InputTopic, pipeline_id: str, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> ray.data.Dataset:
    return __get_kafka_dataset(bootstrap, input_topic, pipeline_id, duration, require_full_duration, end)

def get_kafka_dataset_local(bootstrap: str, input_topic: InputTopic, pipeline_id: str, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> ray.data.Dataset:
    return __get_kafka_dataset(bootstrap, input_topic, pipeline_id, duration, require_full_duration, end)

def __get_kafka_dataset(bootstrap: str, input_topic: InputTopic, pipeline_id: str, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> ray.data.Dataset:
    # Pre-existing bug fixed here: this function and __read_kafka_window below
    # used to share the one name __get_kafka_dataset. The second definition
    # replaced the first at import time, so this outer function -- the one
    # with the require_full_duration wait loop -- was dead code, and the call
    # below actually recursed into __read_kafka_window's four-parameter
    # signature, which get_kafka_dataset_local/_remote called with five
    # arguments and so raised TypeError on every real invocation. Restructuring
    # exactly these two functions for the `end` parameter is why this is fixed
    # here rather than left as a separate change.
    if input_topic.filterType == "OperatorId":
        for m in input_topic.mappings:
            if not m.source.startswith("analytics."):
                m.source = f"analytics.{m.source}"
    while True:
        ds, cutoff = __read_kafka_window(bootstrap, input_topic, pipeline_id, duration, end)
        if require_full_duration:
            if end is not None:
                # A fixed end cannot be waited past: probe once and refuse
                # instead of sleeping towards a window that will never arrive.
                msg = ds.take(1)
                if len(msg) == 0:
                    raise ValueError(
                        f"no messages for {input_topic.name} in the {duration} "
                        f"before {end.isoformat()}; require_full_duration cannot "
                        f"wait for a fixed end")
                msg_timestamp = datetime.datetime.fromtimestamp(
                    msg[0]["timestamp"] / 1000.0,
                    tz=datetime.timezone.utc
                )
                reach = end - msg_timestamp
                if reach < duration * 0.9:
                    raise ValueError(
                        f"{input_topic.name} reaches back only {reach} before "
                        f"{end.isoformat()}, short of the {duration} "
                        f"require_full_duration asked for; require_full_duration "
                        f"cannot wait for a fixed end")
                return ds.map_batches(lambda batch: __map_kafka_batch(batch, input_topic.mappings), batch_format="pandas")
            # Pre-existing bug, not fixed here (out of scope for this change):
            # with start_offset=cutoff the first message read can never be
            # earlier than cutoff, so `offset = cutoff - msg_timestamp` is never
            # positive and this wait never triggers -- require_full_duration
            # against the wall clock returns immediately instead of waiting.
            msg = ds.take(1)
            if len(msg) == 0:
                sleep_for = min(duration.total_seconds(), 15 * 60)
                print(f"No messages found in Kafka, sleeping for {sleep_for} before retrying...")
                time.sleep(sleep_for)
                continue
            msg_timestamp = datetime.datetime.fromtimestamp(
                msg[0]["timestamp"] / 1000.0,
                tz=datetime.timezone.utc
            )
            offset = cutoff - msg_timestamp
            if offset < duration / 10:
                return ds.map_batches(lambda batch: __map_kafka_batch(batch, input_topic.mappings), batch_format="pandas")
            sleep_for = min((offset).total_seconds(), 15 * 60)
            if sleep_for > 0:
                time.sleep(sleep_for)
                continue
        return ds.map_batches(lambda batch: __map_kafka_batch(batch, input_topic.mappings), batch_format="pandas")


def __map_kafka_batch(batch, mappings: typing.List):
    import pandas as pd

    result = {
        "time": pd.to_datetime(batch["timestamp"] / 1000.0, unit="s", utc=True).dt.tz_localize(None)
    }

    # Parse Kafka values - handle both string and bytes
    payloads = []
    for v in batch["value"]:
        try:
            if isinstance(v, bytes):
                v = v.decode('utf-8')
            payload = json.loads(v)
            payloads.append(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"Error parsing JSON from Kafka value: {e}, raw value: {v}")
            payloads.append(None)
    
    mapped_columns = []

    for mapping in mappings:
        source_path = str(mapping.source or "")
        dest = str(mapping.dest)
        mapped_columns.append(dest)
        result[dest] = [__extract_json_path(payload, source_path) if payload is not None else None for payload in payloads]

    frame = pd.DataFrame(result)
    
    if mapped_columns:
        frame = frame.dropna(subset=mapped_columns, how="all")
    return frame


def __extract_json_path(payload: typing.Any, path: str) -> typing.Any:
    if not path:
        return payload

    current = payload
    for segment in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
        if current is None:
            return None
    return current


def __read_kafka_window(bootstrap: str, input_topic: InputTopic, pipeline_id: str, duration: datetime.timedelta, end: typing.Optional[datetime.datetime] = None) -> typing.Tuple[ray.data.Dataset, datetime.datetime]:
    if duration > datetime.timedelta(days=365):
        raise ValueError("Duration too long, refusing to read from Kafka. Please use a more reasonable duration.")

    # Lazy import avoids importing operator_lib.util during package initialization.
    from operator_lib.util import gen_identifiers

    now = end if end is not None else datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - duration
    filter = gen_identifiers(name=input_topic.name, f_type=input_topic.filterType,
                                       f_value=input_topic.filterValue, pipeline_id=pipeline_id)

    # Build expression-based filter for performance
    expressions = []
    for f in filter:
        expressions.append((json_get(col("value"), f["key"]) == f["value"]))

    if end is not None:
        # A fixed bound: start_offset only seeks the lower edge, so the
        # window's top edge is a filter like every other field. Kafka message
        # time is stored as int64 milliseconds.
        expressions.append(col("timestamp") < int(end.timestamp() * 1000))

    '''Always use ray kafka reader, no performance benefit from local reading as in timescale.'''

    ds = ray.data.read_kafka(bootstrap_servers=bootstrap, topics=input_topic.name, timeout_ms=24*60*60*1000, override_num_blocks=10, start_offset=cutoff)
    if len(expressions) == 0:
        return ds, cutoff

    expr = expressions[0]
    for e in expressions[1:]:
        expr = expr & e

    return ds.filter(expr=expr), cutoff
