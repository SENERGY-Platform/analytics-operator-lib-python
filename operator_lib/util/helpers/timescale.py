import psycopg2
from psycopg2.extensions import connection as Psycopg2Connection
import ray
import copy
import dataclasses
import datetime
import typing
from operator_lib.util.model import InputTopic
from operator_lib.util.helpers.exports import ImportExport, export_column_pairs
import base64
import time
from operator_lib.util.logger import logger

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


def __select_parts(conf: InputTopic, duration: datetime.timedelta, end: typing.Optional[datetime.datetime]) -> typing.Tuple[str, typing.List[typing.Tuple[str, str]], str]:
    """
    The pieces every timescale query of a topic is built from: the quoted table
    name, the (quoted source column, quoted dest alias) pairs, and the window
    clause. Shared by the dataset reader and the SQL reader, so both name the
    same table, quote the same way and cut the first path element alike.
    """
    table_name = __quote_identifier(__get_table_name(
        conf.filterValue, conf.name.replace("_", ":")))
    columns = []
    for mapping in conf.mappings:
        source_path = ".".join(mapping.source.split(".")[1:]) # remove the first path element
        columns.append((__quote_identifier(source_path), __quote_identifier(mapping.dest)))
    return table_name, columns, __window_clause(duration, end)


def __get_timescale_dataset_query(conn_str: str, conf: InputTopic, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> str:
    table_name, column_pairs, where = __select_parts(conf, duration, end)
    columns = [f"{source} AS {dest}" for source, dest in column_pairs]

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
    __guard_full_duration(conn_str, conf, query, duration, require_full_duration, end)
    return query


def __get_sql_reader_query(conf: InputTopic, duration: datetime.timedelta, end: typing.Optional[datetime.datetime], changes_only: bool, bucket: typing.Optional[datetime.timedelta] = None) -> str:
    """
    The query of the SQL reader: the plain window read, or with changes_only
    only the rows whose value differs from the previous or the next row in at
    least one mapped column.

    The first and the last row of the window always stay: they are found by the
    missing neighbour (lag/lead of time, which is never NULL), not by comparing
    values, so a NULL value in the first row does not make it disappear.
    The comparison is IS DISTINCT FROM, NULL-safe: NULL against NULL is equal.

    The rows are ordered by time and then by the mapped columns. Without the
    second key, rows sharing a timestamp come in any order, and the same query
    could return a different number of rows each time: in a burst of
    on, on, off the kept rows depend on where the off falls. Every maximal run
    of equal rows keeps its first and its last row whatever the order, so no
    distinct value of a burst is lost either way, but the result is only
    reproducible with a total order. Equal rows are indistinguishable, so
    ordering by the columns is total for everything that can be told apart.

    With bucket the table is first reduced to one row per time bucket,
    time_bucket(<bucket>, time) AS time and max(<column>) AS <dest>, grouped by
    the bucket (TimescaleDB's time_bucket; the columns must be numeric, max has
    no boolean). That reduction is the CTE base, and changes_only then compares
    the bucket rows, not the raw rows. The interval is a literal rendered from
    the timedelta, never from a string from outside.
    """
    table_name, column_pairs, where = __select_parts(conf, duration, end)
    if bucket is not None:
        interval = __interval_literal(bucket)
        bucket_columns = [f"max({source}) AS {dest}" for source, dest in column_pairs]
        base_select = f"""SELECT
                time_bucket({interval}, time) AS time,
                {", ".join(bucket_columns)}
            FROM
                {table_name}
            WHERE
                {where}
            GROUP BY 1"""
        if not changes_only:
            return f"""
        {base_select}
        ORDER BY 1 ASC
    """
    else:
        base_select = None
    if not changes_only:
        columns = [f"{source} AS {dest}" for source, dest in column_pairs]
        return f"""
        SELECT
            time,
            {", ".join(columns)}
        FROM
            {table_name}
        WHERE
            {where}
        ORDER BY time ASC
    """
    dests = [dest for _, dest in column_pairs]
    if base_select is None:
        base_columns = [f"{source} AS {dest}" for source, dest in column_pairs]
        base_select = f"""SELECT
                time,
                {", ".join(base_columns)}
            FROM
                {table_name}
            WHERE
                {where}"""
    order = ", ".join(["time"] + dests)
    neighbours = ["lag(time) OVER w AS __prev_time", "lead(time) OVER w AS __next_time"]
    differs = []
    for i, dest in enumerate(dests):
        neighbours.append(f"lag({dest}) OVER w AS __prev_{i}")
        neighbours.append(f"lead({dest}) OVER w AS __next_{i}")
        differs.append(f"{dest} IS DISTINCT FROM __prev_{i}")
        differs.append(f"{dest} IS DISTINCT FROM __next_{i}")
    return f"""
        WITH base AS (
            {base_select}
        ), neighbours AS (
            SELECT
                time,
                {", ".join(dests)},
                {", ".join(neighbours)}
            FROM
                base
            WINDOW w AS (ORDER BY {order})
        )
        SELECT
            time,
            {", ".join(dests)}
        FROM
            neighbours
        WHERE
            __prev_time IS NULL
            OR __next_time IS NULL
            OR {" OR ".join(differs)}
        ORDER BY {order}
    """


def __interval_literal(value: datetime.timedelta) -> str:
    # Rendered from a whole number of seconds taken out of a timedelta, so
    # nothing but digits reaches the query.
    seconds = value.total_seconds()
    if seconds <= 0 or seconds != int(seconds):
        raise ValueError(f"an interval must be a positive whole number of seconds, got {value!r}")
    return f"INTERVAL '{int(seconds)} seconds'"


def __window_clause(duration: datetime.timedelta, end: typing.Optional[datetime.datetime]) -> str:
    if end is not None:
        # A fixed bound: read exactly [start, end) instead of [now - duration, now].
        start = end - duration
        return (
            f"time >= TIMESTAMPTZ '{__timestamptz_literal(start)}' "
            f"AND time < TIMESTAMPTZ '{__timestamptz_literal(end)}'"
        )
    else:
        return f"time >= NOW() - INTERVAL '{int(duration.total_seconds())}s'"


def __guard_full_duration(conn_str: str, conf: InputTopic, query: str, duration: datetime.timedelta, require_full_duration: bool, end: typing.Optional[datetime.datetime]):
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


@ray.remote
def get_export_dataset_remote(conn_str: str, conf: InputTopic, entry: ImportExport, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> ray.data.Dataset:
    query = __get_export_dataset_query(conn_str, conf, entry, duration, require_full_duration, end)
    ds = ray.data.read_sql(query, lambda: __create_timescale_connection(conn_str), shard_keys=["time"], shard_hash_fn="timestamptz_to_millis", concurrency=4)
    return ds


def get_export_dataset_local(conn_str: str, conf: InputTopic, entry: ImportExport, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> ray.data.Dataset:
    query = __get_export_dataset_query(conn_str, conf, entry, duration, require_full_duration, end)
    conn = __create_timescale_connection(conn_str)
    import pandas as pd
    import pandas.io.sql as sqlio
    data = sqlio.read_sql_query(query, conn)

    for col in data.columns:
        if pd.api.types.is_datetime64tz_dtype(data[col].dtype):
            data[col] = data[col].dt.tz_convert("UTC").dt.tz_localize(None)

    return ray.data.from_pandas(data)


def __get_export_dataset_query(conn_str: str, conf: InputTopic, entry: ImportExport, duration: datetime.timedelta, require_full_duration: bool = False, end: typing.Optional[datetime.datetime] = None) -> str:
    # The table is the one the deployer named, not derived: an export table's
    # name embeds the owner's database id, which is not in the input topic.
    table_name = __quote_identifier(entry.table)
    columns = [
        f"{__quote_identifier(column)} AS {__quote_identifier(dest)}"
        for column, dest in export_column_pairs(entry, conf)
    ]

    # DISTINCT over every selected column, not over time. An export does not set
    # TimestampUnique, so it can hold the same row more than once, and the
    # duplicates are exact. Rows that merely share a timestamp -- a forecast export holds one row per
    # forecasted_for under the same time -- differ in some column and stay.
    query = f"""
        SELECT DISTINCT
            time,
            {", ".join(columns)}
        FROM
            {table_name}
        WHERE
            {__window_clause(duration, end)}
        ORDER BY time ASC
    """
    __guard_full_duration(conn_str, conf, query, duration, require_full_duration, end)
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



# Reading with the SQL reader: one task, one connection, one query per device
# table. A dropped connection (a DNS hiccup, a restarted database) costs one
# table another try, not the whole read.
SQL_READ_ATTEMPTS = 3
SQL_RETRY_PAUSE_S = 5

@dataclasses.dataclass(frozen=True)
class SqlOverride:
    """
    Per-input settings for reader="sql", keyed by the name of the operator input
    (mapping.dest) in provide_historic_data's overrides.

    duration replaces the history window of that input.
    bucket reads the input as one row per time bucket of that size: the bucket
    start as time and the maximum of the value in it. Only for numeric columns.
    Both are whole, positive timedeltas for bucket (whole seconds), positive for duration.
    """
    duration: typing.Optional[datetime.timedelta] = None
    bucket: typing.Optional[datetime.timedelta] = None

    def __post_init__(self):
        _check_sql_override(self)

    @property
    def is_empty(self) -> bool:
        return self.duration is None and self.bucket is None


def _check_sql_override(override: "SqlOverride"):
    for name in ("duration", "bucket"):
        value = getattr(override, name)
        if value is None:
            continue
        if not isinstance(value, datetime.timedelta) or isinstance(value, bool):
            raise ValueError(f"SqlOverride.{name} must be a timedelta, got {value!r}")
        if value <= datetime.timedelta(0):
            raise ValueError(f"SqlOverride.{name} must be positive, got {value!r}")
    if override.bucket is not None:
        __interval_literal(override.bucket)  # whole seconds, or ValueError


SqlInputJob = typing.List[typing.Tuple]
"""One input: its entries in order, each (device or None, topic of that single
device) or (device or None, topic, overrides), overrides being {dest: SqlOverride}
for the mappings of that topic that have one.
The device is set, and becomes the column device_id, only for an input that names several."""


def __redact(text: str, conn_str: str) -> str:
    # The DSN, and its password on its own, never leave in a message: a
    # connection error may quote what it was given.
    redacted = str(text).replace(conn_str, "***")
    try:
        password = psycopg2.extensions.parse_dsn(conn_str).get("password")
    except Exception:
        password = None
    if password:
        redacted = redacted.replace(password, "***")
    return redacted


def __query_to_frame(connection, query: str):
    import pandas as pd
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [d[0] for d in cursor.description]
        frame = pd.DataFrame.from_records(cursor.fetchall(), columns=names, coerce_float=True)
    finally:
        cursor.close()
    # Ray+PyArrow cannot infer timezone-aware pandas dtypes, see get_timescale_dataset_local.
    frame["time"] = pd.to_datetime(frame["time"], utc=True).dt.tz_localize(None)
    for col in frame.columns:
        if isinstance(frame[col].dtype, pd.DatetimeTZDtype):
            frame[col] = frame[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return frame


def __read_table(state: dict, conn_str: str, connect, query: str, label: str):
    """
    One table over the connection held in state, reconnecting on
    psycopg2.OperationalError: SQL_READ_ATTEMPTS tries, SQL_RETRY_PAUSE_S
    between them. Any other error, and the last OperationalError, is raised
    with the DSN removed from its message.
    """
    for attempt in range(1, SQL_READ_ATTEMPTS + 1):
        failure = None
        try:
            if state.get("connection") is None:
                state["connection"] = connect(conn_str)
                try:
                    state["connection"].autocommit = True
                except Exception:
                    pass
            return __query_to_frame(state["connection"], query)
        except psycopg2.OperationalError as e:
            failure = f"{type(e).__name__}: {__redact(e, conn_str)}"
        except Exception as e:
            if __redact(e, conn_str) == str(e):
                raise
            failure = f"{type(e).__name__}: {__redact(e, conn_str)}"
            attempt = None
        # Outside the except block: raised from here, the new exception carries
        # no __context__ that still holds the original message.
        if attempt is None:
            raise RuntimeError(f"reading {label} failed: {failure}")
        try:
            if state.get("connection") is not None:
                state["connection"].close()
        except Exception:
            pass
        state["connection"] = None
        if attempt == SQL_READ_ATTEMPTS:
            raise psycopg2.OperationalError(f"reading {label} failed after {attempt} attempts: {failure}")
        logger.warning(f"reading {label}: attempt {attempt} of {SQL_READ_ATTEMPTS} failed "
                       f"({failure}), reconnecting in {SQL_RETRY_PAUSE_S}s")
        time.sleep(SQL_RETRY_PAUSE_S)


def __sql_input_datasets(conn_str: str, jobs: typing.List[SqlInputJob], duration: datetime.timedelta, end: datetime.datetime, changes_only: bool, connect):
    """
    Yield one dataset per job, in order, reading one table after the other over
    one connection. A generator, so that a caller which wants the inputs one by
    one (the remote task, whose returns are separate object refs) gets each as
    soon as it is read and a failure only takes the inputs from the failing one on.
    """
    import pandas as pd
    state = {"connection": None}
    try:
        for job in jobs:
            frames = []
            for entry in job:
                device, topic = entry[0], entry[1]
                overrides = entry[2] if len(entry) > 2 else None
                frame = __read_topic_groups(state, conn_str, connect, topic, overrides, duration, end, changes_only)
                if device is not None:
                    frame["device_id"] = device
                frames.append(frame)
            yield ray.data.from_pandas(frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True))
    finally:
        if state.get("connection") is not None:
            try:
                state["connection"].close()
            except Exception:
                pass


def __override_groups(topic: InputTopic, overrides) -> typing.List[typing.Tuple[typing.Optional[SqlOverride], InputTopic]]:
    """
    The mappings of one topic split by their override, in order of first
    appearance: [(override or None, topic with only those mappings)]. Equal
    overrides share a group, and so do the mappings without one; an override
    with neither field set counts as none.
    """
    groups: typing.Dict[typing.Optional[SqlOverride], typing.List] = {}
    for mapping in topic.mappings:
        override = (overrides or {}).get(mapping.dest)
        if override is not None and override.is_empty:
            override = None
        groups.setdefault(override, []).append(mapping)
    if len(groups) == 1:
        (override,) = groups
        return [(override, topic)]
    result = []
    for override, mappings in groups.items():
        sub = copy.deepcopy(topic)
        sub.mappings = mappings
        result.append((override, sub))
    return result


def __read_topic_groups(state, conn_str, connect, topic, overrides, duration, end, changes_only):
    """
    One device table: one query per group of mappings that share an override,
    the frames put one under the other (a column of another group is NaN) and
    sorted by time. A topic without any override is one plain query.
    """
    import pandas as pd
    frames = []
    for override, group in __override_groups(topic, overrides):
        group_duration = override.duration if override is not None and override.duration is not None else duration
        bucket = override.bucket if override is not None else None
        query = __get_sql_reader_query(group, group_duration, end, changes_only, bucket)
        frames.append(__read_table(state, conn_str, connect, query,
                                   f"{topic.name} ({topic.filterValue})"))
    if len(frames) == 1:
        return frames[0]
    columns = ["time"] + [m.dest for m in topic.mappings]
    filled = [f for f in frames if len(f)] or frames[:1]
    merged = pd.concat(filled, ignore_index=True).reindex(columns=columns)
    return merged.sort_values("time", kind="stable", ignore_index=True)


def get_timescale_sql_datasets_local(conn_str: str, jobs: typing.List[SqlInputJob], duration: datetime.timedelta, end: datetime.datetime, changes_only: bool = False, connect=None) -> typing.List[ray.data.Dataset]:
    return list(__sql_input_datasets(conn_str, jobs, duration, end, changes_only, connect or __create_timescale_connection))


@ray.remote
def get_timescale_sql_datasets_remote(conn_str: str, jobs: typing.List[SqlInputJob], duration: datetime.timedelta, end: datetime.datetime, changes_only: bool = False, connect=None):
    # A generator: called with num_returns = len(jobs), every yield becomes the
    # object ref of one input, also for a single input (then .remote returns one ref).
    yield from __sql_input_datasets(conn_str, jobs, duration, end, changes_only, connect or __create_timescale_connection)
