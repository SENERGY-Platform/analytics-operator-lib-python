"""
   Copyright 2026 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

"""
Read history through timescale-wrapper instead of over a direct database
connection.

This exists so that a run whose code is not trusted can still read history. The
direct path in timescale.py connects with a shared DSN that reaches every series
in the instance, and which series it reads is decided by the input topics rather
than by who started the operator. Where that DSN is absent and a platform token
is present -- an experiment in the Operator Development Environment -- the same
read goes through timescale-wrapper, which checks the caller's Execute
permission on the device itself. The operator then reads exactly what the
developer may read, and carries no database credential at all.

A deployed operator started by the flow engine has no token and keeps the direct
path. That asymmetry is the whole reason this is a second implementation rather
than a replacement.
"""

__all__ = (
    "get_ts_wrapper_dataset_local",
    "get_ts_wrapper_dataset_remote",
    "get_ts_wrapper_export_dataset_local",
    "get_ts_wrapper_export_dataset_remote",
    "TimescaleWrapperError",
    "TokenExpiredError",
)

import datetime
import time
import typing

import ray
import requests

from operator_lib.util.model import InputTopic
from operator_lib.util.helpers.exports import ImportExport, export_column_pairs
from operator_lib.util.logger import logger

# The layout timescale-wrapper renders timestamps in when asked for it. Sent
# explicitly rather than relying on the server default, because the values come
# back as strings and a layout guessed wrong shifts every timestamp silently.
TIME_FORMAT = "2006-01-02T15:04:05.000Z07:00"

# The response carries one sub-series per requested column rather than one wide
# table, which is what keeps a column's own sampling instants intact.
RESPONSE_FORMAT = "per_query"

# How much of the requested duration one request asks for.
#
# Not the whole window. The API gateway in front of timescale-wrapper answers an
# oversized response with a 502 rather than relaying it, and a training read is
# far larger than the profile reads that ceiling was found with. So the window is
# walked in chunks and the frames concatenated. Seven days is a starting point,
# not a measured optimum -- a chunk that comes back refused is halved and retried.
DEFAULT_CHUNK = datetime.timedelta(days=7)

# The floor on halving. Below this a refusal is the platform saying no rather
# than a size to negotiate, and continuing would issue thousands of requests.
MIN_CHUNK = datetime.timedelta(minutes=15)

REQUEST_TIMEOUT_SECONDS = 300


class TimescaleWrapperError(RuntimeError):
    pass


class TokenExpiredError(TimescaleWrapperError):
    """
    The platform rejected the token partway through a read.

    Its own error, because it is the failure a long training run invites and it
    is not a permission problem: the developer had the rights when the run
    started. A run that outlives its token needs a longer-lived one, not a
    different device.
    """


def get_ts_wrapper_dataset_local(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
    end: typing.Optional[datetime.datetime] = None,
) -> ray.data.Dataset:
    frame = read_history(wrapper_url, token, conf, duration, require_full_duration, end)
    return ray.data.from_pandas(frame)


@ray.remote
def get_ts_wrapper_dataset_remote(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
    end: typing.Optional[datetime.datetime] = None,
) -> ray.data.Dataset:
    # A ray task around the same read. Unlike the direct path this cannot shard
    # the read across workers -- ray.data.read_sql does that against Postgres
    # with a shard key, and there is no equivalent over one HTTP response -- so
    # this is a sequential fetch that happens to run on a worker.
    return get_ts_wrapper_dataset_local(
        wrapper_url, token, conf, duration, require_full_duration, end)


def get_ts_wrapper_export_dataset_local(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    entry: ImportExport,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
    end: typing.Optional[datetime.datetime] = None,
) -> ray.data.Dataset:
    frame = read_export_history(
        wrapper_url, token, conf, entry, duration, require_full_duration, end)
    return ray.data.from_pandas(frame)


@ray.remote
def get_ts_wrapper_export_dataset_remote(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    entry: ImportExport,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
    end: typing.Optional[datetime.datetime] = None,
) -> ray.data.Dataset:
    return get_ts_wrapper_export_dataset_local(
        wrapper_url, token, conf, entry, duration, require_full_duration, end)


def read_history(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
    end: typing.Optional[datetime.datetime] = None,
):
    """
    Return the same frame the direct path returns: a `time` column plus one
    column per mapping, named after the mapping's dest, ordered by time ascending.

    With `end` given the window is the fixed `[end - duration, end)` rather than
    `[now - duration, now)`, and `require_full_duration` cannot wait its way to
    more data against a bound that will not move: `_require_reach` probes once
    and raises instead of `_await_full_duration`'s sleep loop.
    """
    return _read(wrapper_url, token, conf, None, duration, require_full_duration, end)


def read_export_history(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    entry: ImportExport,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
    end: typing.Optional[datetime.datetime] = None,
):
    """
    `read_history` for an input topic whose history the deployer resolved to an
    analytics-serving export: the same frame, the same windows, chunking and
    `require_full_duration` behaviour, but the element names the export and the
    rows are kept as the export holds them. See `_decode_export` for why that
    differs from the device decoding.
    """
    return _read(wrapper_url, token, conf, entry, duration, require_full_duration, end)


def _read(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    entry: typing.Optional[ImportExport],
    duration: datetime.timedelta,
    require_full_duration: bool,
    end: typing.Optional[datetime.datetime],
):
    import pandas as pd

    if require_full_duration:
        if end is not None:
            _require_reach(wrapper_url, token, conf, duration, end, entry)
        else:
            _await_full_duration(wrapper_url, token, conf, duration, entry)

    end = end if end is not None else datetime.datetime.now(datetime.timezone.utc)
    start = end - duration

    frames = []
    chunk = DEFAULT_CHUNK
    window_start = start
    while window_start < end:
        window_end = min(window_start + chunk, end)
        rows, chunk = _read_window(
            wrapper_url, token, conf, window_start, window_end, chunk, entry)
        if rows is not None and not rows.empty:
            frames.append(rows)
        # chunk may have been halved by a refusal, in which case the window that
        # was refused is retried at the new size rather than skipped.
        if rows is not None:
            window_start = window_end
        if chunk < MIN_CHUNK:
            raise TimescaleWrapperError(
                f"timescale-wrapper kept refusing the read for {_describe(conf, entry)} down to "
                f"{chunk}, which is no longer a size worth negotiating; asking for less "
                f"will not help, the service or the gateway is the problem")

    columns = ["time"] + [mapping.dest for mapping in conf.mappings]
    if not frames:
        return pd.DataFrame(columns=columns)

    frame = pd.concat(frames, ignore_index=True)
    frame = frame.sort_values("time", kind="stable").reset_index(drop=True)
    if entry is None:
        frame = frame.drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
    else:
        # Whole rows, never the timestamp alone: a forecast export holds one row
        # per forecasted_for under the same time, and all of them are data.
        frame = frame.drop_duplicates(keep="first").reset_index(drop=True)

    # Ray and PyArrow cannot infer timezone-aware pandas dtypes like
    # datetime64[ns, UTC], the same reason the direct path normalises here.
    for col in frame.columns:
        if pd.api.types.is_datetime64tz_dtype(frame[col].dtype):
            frame[col] = frame[col].dt.tz_convert("UTC").dt.tz_localize(None)

    return frame[columns]


def _read_window(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    chunk: datetime.timedelta,
    entry: typing.Optional[ImportExport] = None,
):
    """
    Read one time window. Returns (frame, chunk); frame is None when the window
    was refused for its size and should be retried at the returned smaller chunk.

    The frame covers exactly [window_start, window_end). timescale-wrapper renders
    `"time" > start AND "time" < end`, strict on both sides, so a row stamped
    exactly on a chunk boundary would belong to neither the chunk ending there nor
    the one starting there. That is not a corner case: a split's bounds are whole
    hours, the chunks are whole days, and an import that stamps by issue time
    stamps every row on a whole hour. So the request starts one millisecond -- the
    wrapper's time resolution -- earlier, and the window's own lower edge is
    applied here.
    """
    element = _build_element(conf, window_start - _TIME_RESOLUTION, window_end, entry)
    try:
        payload = _post(wrapper_url, token, [element])
    except _OversizedResponse:
        halved = chunk / 2
        logger.warning(
            f"the gateway refused the read for {_describe(conf, entry)} over "
            f"{window_start.isoformat()}..{window_end.isoformat()}; retrying with a "
            f"{halved} window")
        return None, halved
    frame = _decode_for(payload, conf, entry)
    if not frame.empty:
        frame = frame[frame["time"] >= _floor_to_resolution(window_start)].reset_index(drop=True)
    return frame, chunk


# The resolution timestamps travel at, both ways: _format_time renders
# milliseconds and the wrapper answers in TIME_FORMAT, which has milliseconds.
_TIME_RESOLUTION = datetime.timedelta(milliseconds=1)


def _floor_to_resolution(value: datetime.datetime):
    # The same truncation _format_time applies, so that a window's lower edge as
    # applied here is the edge the previous window's upper bound was sent as.
    import pandas as pd

    stamp = pd.Timestamp(value)
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    return stamp.floor("ms")


def _decode_for(payload, conf: InputTopic, entry: typing.Optional[ImportExport]):
    if entry is None:
        return _decode(payload, conf)
    return _decode_export(payload, conf, entry)


def _build_element(
    conf: InputTopic,
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    entry: typing.Optional[ImportExport] = None,
) -> typing.Dict[str, typing.Any]:
    if entry is not None:
        # Column names are the export's own, looked up per mapping; the time
        # column is implicit as the first column of every row.
        return {
            "exportId": entry.export_id,
            "columns": [{"name": column} for column, _ in export_column_pairs(entry, conf)],
            "time": {
                "start": _format_time(window_start),
                "end": _format_time(window_end),
            },
            "orderColumnIndex": 0,
            "orderDirection": "asc",
        }
    return {
        "deviceId": conf.filterValue,
        # The topic carries the service id with colons replaced by underscores,
        # the same derivation the direct path applies to build a table name.
        "serviceId": conf.name.replace("_", ":"),
        "columns": [{"name": _source_path(mapping.source)} for mapping in conf.mappings],
        "time": {
            "start": _format_time(window_start),
            "end": _format_time(window_end),
        },
        "orderColumnIndex": 0,
        "orderDirection": "asc",
    }


def _source_path(source: str) -> str:
    # Drop the first path element, exactly as the direct path does when it turns
    # a mapping source into a column name.
    return ".".join(source.split(".")[1:])


def _format_time(value: datetime.datetime) -> str:
    return value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") \
        + f"{value.microsecond // 1000:03d}Z"


class _OversizedResponse(Exception):
    pass


def _post(wrapper_url: str, token: str, elements: typing.List[dict]):
    url = wrapper_url.rstrip("/") + "/queries/v2"
    response = requests.post(
        url,
        params={"format": RESPONSE_FORMAT, "time_format": TIME_FORMAT},
        json=elements,
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code == 401:
        raise TokenExpiredError(
            "timescale-wrapper rejected the platform token; a run that outlives its "
            "token loses access partway through, so this is a lifetime problem rather "
            "than a permission one")
    if response.status_code == 403:
        raise TimescaleWrapperError(
            "timescale-wrapper refused the read: no execute permission on the device "
            "this input topic names")
    if response.status_code == 502:
        # Two different failures share this code: a response too large for the
        # gateway to relay, and an upstream that errored. Only the first is worth
        # asking for less, and the caller distinguishes them by whether halving
        # ever helps.
        raise _OversizedResponse()
    if not response.ok:
        raise TimescaleWrapperError(
            f"timescale-wrapper returned {response.status_code}: {response.text[:500]}")
    return response.json()


def _decode(payload, conf: InputTopic):
    """
    Turn one /queries/v2 response element into a frame.

    The response carries a sub-series per requested column, each row `[time,
    value]`. The sub-series are separate queries server-side and the server trims
    trailing empty rows per series, so they can end at different points and must
    be recombined on their timestamps rather than zipped by position.
    """
    import pandas as pd

    dests = [mapping.dest for mapping in conf.mappings]
    columns = ["time"] + dests

    if not payload:
        return pd.DataFrame(columns=columns)

    element = payload[0]
    data = element.get("data") or []

    rows: typing.Dict[str, typing.List[typing.Any]] = {}
    for series_index, series in enumerate(data):
        for row in series or []:
            if not row:
                continue
            at = row[0]
            record = rows.setdefault(at, [None] * len(dests))
            for column_index, value_index in _column_targets(
                    len(dests), len(data), series_index, len(row)):
                if value_index < len(row):
                    record[column_index] = row[value_index]

    if not rows:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(
        [[at] + values for at, values in rows.items()], columns=columns)
    frame["time"] = pd.to_datetime(frame["time"], format="ISO8601", utc=True)
    return frame.sort_values("time", kind="stable").reset_index(drop=True)


def _decode_export(payload, conf: InputTopic, entry: ImportExport):
    """
    Turn one /queries/v2 response element for an export into a frame, one record
    per response row.

    timescale-wrapper answers a raw query for an export with a single SELECT
    over all requested columns, so a row is `[time, v1, ..., vn]` and several
    rows may share a timestamp: a forecast export holds one row per
    forecasted_for under the same time. `_decode` folds rows by timestamp and
    would keep one of them, so exports are decoded here without recombining.
    A row of any other width means the response is not that shape; the values
    could then only be matched to columns by guessing, so this raises instead.
    """
    import pandas as pd

    pairs = export_column_pairs(entry, conf)
    columns = ["time"] + [dest for _, dest in pairs]

    if not payload:
        return pd.DataFrame(columns=columns)

    records = []
    for series in payload[0].get("data") or []:
        for row in series or []:
            # An empty or time-less row is the server's marker for "no data".
            if not row or row[0] is None:
                continue
            if len(row) != len(pairs) + 1:
                raise TimescaleWrapperError(
                    f"timescale-wrapper answered the read for {_describe(conf, entry)} "
                    f"with rows of width {len(row)}, expected {len(pairs) + 1} "
                    f"(time plus {len(pairs)} columns in one wide table); the response "
                    f"is not in the shape this reader decodes")
            records.append(list(row))

    if not records:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(records, columns=columns)
    frame["time"] = pd.to_datetime(frame["time"], format="ISO8601", utc=True)
    return frame.sort_values("time", kind="stable").reset_index(drop=True)


def _column_targets(column_count: int, series_count: int, series_index: int, row_width: int):
    """
    Which requested column a position in a response row belongs to.

    Two shapes occur, and they are told apart by width rather than guessed at:
    reading the wrong column is exactly the failure this prevents.
    """
    # A series per column: as many sub-series as columns, each two wide.
    if series_count == column_count and row_width == 2:
        return [(series_index, 1)]
    # One wide table carrying every column.
    if row_width == column_count + 1:
        return [(column, column + 1) for column in range(column_count)]
    # A short row within the per-column shape still belongs to its own series.
    if series_count == column_count:
        return [(series_index, 1)]
    return []


def _await_full_duration(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    duration: datetime.timedelta,
    entry: typing.Optional[ImportExport] = None,
):
    """
    Wait until the series reaches back at least `duration`, matching what the
    direct path does with a LIMIT 1 probe.
    """
    while True:
        end = datetime.datetime.now(datetime.timezone.utc)
        element = _build_element(conf, end - duration, end, entry)
        element["limit"] = 1
        element["orderDirection"] = "asc"
        try:
            frame = _decode_for(_post(wrapper_url, token, [element]), conf, entry)
        except _OversizedResponse:
            # One row cannot be too large; treat it as the service failing.
            raise TimescaleWrapperError(
                f"timescale-wrapper could not answer a one-row probe for "
                f"{_describe(conf, entry)}, so the service rather than the size is the problem")
        if frame.empty:
            logger.debug(
                f"no data yet for {_describe(conf, entry)}; waiting {duration} for the full window")
            time.sleep(duration.total_seconds())
            continue
        oldest = frame["time"].iloc[0].to_pydatetime()
        reach = datetime.datetime.now(datetime.timezone.utc) - oldest
        if reach >= duration:
            return
        remaining = (duration - reach).total_seconds()
        logger.debug(
            f"{_describe(conf, entry)} reaches back {reach}, waiting {remaining}s for {duration}")
        time.sleep(remaining)


def _require_reach(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    duration: datetime.timedelta,
    end: datetime.datetime,
    entry: typing.Optional[ImportExport] = None,
):
    """
    Single-shot counterpart of `_await_full_duration` for a fixed `end`: a bound
    that will not advance cannot be waited past, so this probes the series once,
    ascending from `end - duration`, and raises ValueError rather than sleeping
    towards a window that will never arrive.
    """
    element = _build_element(conf, end - duration, end, entry)
    element["limit"] = 1
    element["orderDirection"] = "asc"
    try:
        frame = _decode_for(_post(wrapper_url, token, [element]), conf, entry)
    except _OversizedResponse:
        # One row cannot be too large; treat it as the service failing.
        raise TimescaleWrapperError(
            f"timescale-wrapper could not answer a one-row probe for "
            f"{_describe(conf, entry)}, so the service rather than the size is the problem")
    if frame.empty:
        raise ValueError(
            f"no data for {_describe(conf, entry)} in the {duration} before "
            f"{end.isoformat()}; require_full_duration cannot wait for a fixed end")
    oldest = frame["time"].iloc[0].to_pydatetime()
    reach = end - oldest
    if reach < duration:
        raise ValueError(
            f"{_describe(conf, entry)} reaches back only {reach} before {end.isoformat()}, "
            f"short of the {duration} require_full_duration asked for; "
            f"require_full_duration cannot wait for a fixed end")


def _describe(conf: InputTopic, entry: typing.Optional[ImportExport] = None) -> str:
    if entry is not None:
        return f"export {entry.export_id} of import {conf.filterValue}"
    return f"device {conf.filterValue} service {conf.name.replace('_', ':')}"
