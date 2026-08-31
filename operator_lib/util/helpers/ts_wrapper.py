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
    "TimescaleWrapperError",
    "TokenExpiredError",
)

import datetime
import time
import typing

import ray
import requests

from operator_lib.util.model import InputTopic
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
) -> ray.data.Dataset:
    frame = read_history(wrapper_url, token, conf, duration, require_full_duration)
    return ray.data.from_pandas(frame)


@ray.remote
def get_ts_wrapper_dataset_remote(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
) -> ray.data.Dataset:
    # A ray task around the same read. Unlike the direct path this cannot shard
    # the read across workers -- ray.data.read_sql does that against Postgres
    # with a shard key, and there is no equivalent over one HTTP response -- so
    # this is a sequential fetch that happens to run on a worker.
    return get_ts_wrapper_dataset_local(
        wrapper_url, token, conf, duration, require_full_duration)


def read_history(
    wrapper_url: str,
    token: str,
    conf: InputTopic,
    duration: datetime.timedelta,
    require_full_duration: bool = False,
):
    """
    Return the same frame the direct path returns: a `time` column plus one
    column per mapping, named after the mapping's dest, ordered by time ascending.
    """
    import pandas as pd

    if require_full_duration:
        _await_full_duration(wrapper_url, token, conf, duration)

    end = datetime.datetime.now(datetime.timezone.utc)
    start = end - duration

    frames = []
    chunk = DEFAULT_CHUNK
    window_start = start
    while window_start < end:
        window_end = min(window_start + chunk, end)
        rows, chunk = _read_window(
            wrapper_url, token, conf, window_start, window_end, chunk)
        if rows is not None and not rows.empty:
            frames.append(rows)
        # chunk may have been halved by a refusal, in which case the window that
        # was refused is retried at the new size rather than skipped.
        if rows is not None:
            window_start = window_end
        if chunk < MIN_CHUNK:
            raise TimescaleWrapperError(
                f"timescale-wrapper kept refusing the read for {_describe(conf)} down to "
                f"{chunk}, which is no longer a size worth negotiating; asking for less "
                f"will not help, the service or the gateway is the problem")

    columns = ["time"] + [mapping.dest for mapping in conf.mappings]
    if not frames:
        return pd.DataFrame(columns=columns)

    frame = pd.concat(frames, ignore_index=True)
    frame = frame.sort_values("time", kind="stable").reset_index(drop=True)
    frame = frame.drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)

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
):
    """
    Read one time window. Returns (frame, chunk); frame is None when the window
    was refused for its size and should be retried at the returned smaller chunk.
    """
    element = _build_element(conf, window_start, window_end)
    try:
        payload = _post(wrapper_url, token, [element])
    except _OversizedResponse:
        halved = chunk / 2
        logger.warning(
            f"the gateway refused the read for {_describe(conf)} over "
            f"{window_start.isoformat()}..{window_end.isoformat()}; retrying with a "
            f"{halved} window")
        return None, halved
    return _decode(payload, conf), chunk


def _build_element(
    conf: InputTopic,
    window_start: datetime.datetime,
    window_end: datetime.datetime,
) -> typing.Dict[str, typing.Any]:
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
):
    """
    Wait until the series reaches back at least `duration`, matching what the
    direct path does with a LIMIT 1 probe.
    """
    while True:
        end = datetime.datetime.now(datetime.timezone.utc)
        element = _build_element(conf, end - duration, end)
        element["limit"] = 1
        element["orderDirection"] = "asc"
        try:
            frame = _decode(_post(wrapper_url, token, [element]), conf)
        except _OversizedResponse:
            # One row cannot be too large; treat it as the service failing.
            raise TimescaleWrapperError(
                f"timescale-wrapper could not answer a one-row probe for "
                f"{_describe(conf)}, so the service rather than the size is the problem")
        if frame.empty:
            logger.debug(
                f"no data yet for {_describe(conf)}; waiting {duration} for the full window")
            time.sleep(duration.total_seconds())
            continue
        oldest = frame["time"].iloc[0].to_pydatetime()
        reach = datetime.datetime.now(datetime.timezone.utc) - oldest
        if reach >= duration:
            return
        remaining = (duration - reach).total_seconds()
        logger.debug(
            f"{_describe(conf)} reaches back {reach}, waiting {remaining}s for {duration}")
        time.sleep(remaining)


def _describe(conf: InputTopic) -> str:
    return f"device {conf.filterValue} service {conf.name.replace('_', ':')}"
