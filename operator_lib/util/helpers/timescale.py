import psycopg2
from psycopg2.extensions import connection as Psycopg2Connection
import ray
import datetime
import typing
from operator_lib.util.model import InputTopic
import base64
import time

@ray.remote
def get_timescale_dataset_remote(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> ray.data.Dataset:
    query = __get_timescale_dataset_query(conn_str, conf, duration, require_full_duration, end)
    '''
    Expect to have this function available in timescale: # TODO ensure with job

    CREATE OR REPLACE FUNCTION timestamptz_to_millis(ts timestamptz)
    RETURNS bigint AS $$
    BEGIN
        RETURN (EXTRACT(EPOCH FROM ts) * 1000)::bigint;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    '''
    ds = ray.data.read_sql(query, lambda: __create_timescale_connection(conn_str), shard_keys=["time"], shard_hash_fn="timestamptz_to_millis", concurrency=4)
    return ds

def get_timescale_dataset_local(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> ray.data.Dataset:
    query = __get_timescale_dataset_query(conn_str, conf, duration, require_full_duration, end)
    conn = __create_timescale_connection(conn_str)
    import pandas as pd
    import pandas.io.sql as sqlio
    data = sqlio.read_sql_query(query, conn)

    # Ray+PyArrow cannot infer timezone-aware pandas dtypes like datetime64[ns, UTC].
    # Normalize all tz-aware datetime columns to UTC-naive datetimes before ingestion.
    for col in data.columns:
        if pd.api.types.is_datetime64tz_dtype(data[col].dtype):
            data[col] = data[col].dt.tz_convert("UTC").dt.tz_localize(None)

    ds = ray.data.from_pandas(data)
    return ds

def __timestamptz_literal(dt: datetime.datetime) -> str:
    # Rendered from a datetime object, never from a string that arrived from
    # outside: ray.data.read_sql wraps this query for sharding and takes no
    # bind parameters, so the bound has to reach SQL as a literal, and a
    # literal built from anything less trusted than an already-parsed datetime
    # is how a malformed bound would become a malformed query.
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def __get_timescale_dataset_query(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> str:
    table_name = __quote_identifier(__get_table_name(
        conf.filterValue, conf.name.replace("_", ":")))
    columns = []

    for mapping in conf.mappings:
        source_path = ".".join(mapping.source.split(".")[1:]) # remove the first path element
        columns.append(
            f"{__quote_identifier(source_path)} AS {__quote_identifier(mapping.dest)}")

    if end is not None:
        # A fixed bound: read exactly [start, end) instead of [now - duration, now].
        start = end - duration
        where = (
            f"time >= TIMESTAMPTZ '{__timestamptz_literal(start)}' "
            f"AND time < TIMESTAMPTZ '{__timestamptz_literal(end)}'"
        )
    else:
        where = f"time >= NOW() - INTERVAL '{int(duration.total_seconds())}s'"

    query = f"""
        SELECT
            time,
            {", ".join(columns)}
        FROM
            {table_name}
        WHERE
            {where}
        ORDER BY time ASC
    """
    if require_full_duration:
        if end is not None:
            # A fixed end cannot be waited past: probe once and refuse instead
            # of sleeping towards a window that will never arrive.
            conn = __create_timescale_connection(conn_str)
            cursor = conn.cursor()
            cursor.execute(query + " LIMIT 1")
            result = cursor.fetchone()
            cursor.close()
            if result is None:
                raise ValueError(
                    f"no data for {conf.name} ({conf.filterValue}) in the "
                    f"{duration} before {end.isoformat()}; require_full_duration "
                    f"cannot wait for a fixed end")
            reach = end - result[0]
            if reach < duration:
                raise ValueError(
                    f"{conf.name} ({conf.filterValue}) reaches back only {reach} "
                    f"before {end.isoformat()}, short of the {duration} "
                    f"require_full_duration asked for; require_full_duration "
                    f"cannot wait for a fixed end")
        else:
            enough_data = False
            conn = __create_timescale_connection(conn_str)
            while not enough_data:
                cursor = conn.cursor()
                cursor.execute(query + " LIMIT 1")
                result = cursor.fetchone()
                cursor.close()
                if result is not None:
                    record_time = result[0]
                    time_diff = datetime.datetime.now(
                        datetime.timezone.utc) - record_time
                    enough_data = time_diff >= duration
                    if not enough_data:
                        time.sleep((duration - time_diff).total_seconds())
                else:
                    time.sleep(duration)  # currently no data -> sleep for full duration

    return query


def __quote_identifier(value: str) -> str:
    # Postgres identifiers are quoted with double quotes; internal quotes need escaping.
    return '"' + str(value).replace('"', '""') + '"'


def __shorten_id(long_id: str) -> str:
    no_prefix = str(long_id).split(":")[-1].replace("-", "")
    raw = bytes.fromhex(no_prefix)
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def __get_table_name(device_id: str, service_id: str) -> str:
    short_device_id = __shorten_id(device_id)
    short_service_id = __shorten_id(service_id)
    return f"device:{short_device_id}_service:{short_service_id}"



def __create_timescale_connection(conn_str: str) -> Psycopg2Connection:
    return psycopg2.connect(conn_str)
