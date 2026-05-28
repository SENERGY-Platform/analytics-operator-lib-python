import psycopg2
from psycopg2.extensions import connection as Psycopg2Connection
import ray
import datetime
from operator_lib.util.model import InputTopic
import base64
import time

@ray.remote
def get_timescale_dataset_remote(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False) -> ray.data.Dataset:
    query = __get_timescale_dataset_query(conn_str, conf, duration, require_full_duration)
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

def get_timescale_dataset_local(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False) -> ray.data.Dataset:
    query = __get_timescale_dataset_query(conn_str, conf, duration, require_full_duration)
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

def __get_timescale_dataset_query(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False) -> str:
    table_name = __quote_identifier(__get_table_name(
        conf.filterValue, conf.name.replace("_", ":")))
    columns = []

    for mapping in conf.mappings:
        source_path = ".".join(mapping.source.split(".")[1:]) # remove the first path element
        columns.append(
            f"{__quote_identifier(source_path)} AS {__quote_identifier(mapping.dest)}")

    query = f"""
        SELECT
            time,
            {", ".join(columns)}
        FROM
            {table_name}
        WHERE
            time >= NOW() - INTERVAL '{int(duration.total_seconds())}s'
        ORDER BY time ASC
    """
    if require_full_duration:
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
