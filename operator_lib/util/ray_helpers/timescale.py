import psycopg2
import ray
import datetime
from operator_lib.util.model import InputTopic
import base64
import time

@ray.remote
def get_timescale_dataset(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False) -> ray.data.Dataset:
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

    ds = ray.data.read_sql(query, lambda: __create_timescale_connection(conn_str))
    return ds


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



def __create_timescale_connection(conn_str: str):
    return psycopg2.connect(conn_str)
