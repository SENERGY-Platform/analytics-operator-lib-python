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
Narrowing a shared import to one place: resolves the ImportFilters an operator
declares once at start and keeps the result, per import input, for the live loop
(op_base) and the history readers (data).

The result is process state of the driver, like the clock: the readers run on Ray
workers, so the filter reaches them as an argument, never through this module.
"""

__all__ = (
    "ValueFilter",
    "ResolvedImportFilter",
    "resolved",
    "declared_import_filters",
    "resolve_import_filters",
    "filter_for",
    "passes",
    "clear",
)

import dataclasses
import math
import typing

from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.declaration import DeclarationError, ImportFilter
from operator_lib.util.helpers.exports import ImportExport, find_import_export
from operator_lib.util.logger import logger
from operator_lib.util.model import InputTopic

# Only places that reported within this window are candidates for "nearest", on every
# read path, so a decommissioned place cannot win and no path scans the whole export.
PLACES_WINDOW_DAYS = 7


@dataclasses.dataclass(frozen=True)
class ValueFilter:
    """A resolved filter: keep what carries `value` at `path` of the raw message."""
    path: str
    value: str
    # The export column of `path`, for the history readers.
    column: str


@dataclasses.dataclass(frozen=True)
class ResolvedImportFilter:
    """What start found for one declared filter, for the operator to report."""
    path: str
    inputs: typing.Tuple[str, ...]
    # None when the filter was declared but nothing was set, so the import is not filtered.
    value: typing.Optional[str]
    # "config" (the value field), "near" (the nearest place) or "none".
    found: str
    # Only for "near": the distance to the chosen place and its coordinates, when the source gave them.
    distance_km: typing.Optional[float] = None
    latitude: typing.Optional[float] = None
    longitude: typing.Optional[float] = None


# (topic name, import id) of an import input -> its filter.
_active: typing.Dict[typing.Tuple[str, str], ValueFilter] = {}
_resolved: typing.List[ResolvedImportFilter] = []


def clear():
    _active.clear()
    _resolved.clear()


def resolved() -> typing.List[ResolvedImportFilter]:
    return list(_resolved)


def _keys(topic: InputTopic) -> typing.List[typing.Tuple[str, str]]:
    return [(topic.name, v.strip()) for v in (topic.filterValue or "").split(",")]


def filter_for(topic: InputTopic) -> typing.Optional[ValueFilter]:
    if topic.filterType != "ImportId":
        return None
    for key in _keys(topic):
        if key in _active:
            return _active[key]
    return None


def filter_for_message(topic_name: str, import_id) -> typing.Optional[ValueFilter]:
    return _active.get((topic_name, str(import_id).strip())) if _active else None


def value_at(message: typing.Any, path: str) -> typing.Any:
    current = message
    for segment in path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def passes(value_filter: ValueFilter, message: typing.Any) -> bool:
    """Compared as strings: an id is zero-padded text in one import and a number in another."""
    found = value_at(message, value_filter.path)
    return found is not None and str(found).strip() == value_filter.value


def declared_import_filters(operator) -> typing.List[ImportFilter]:
    declared = []
    single = getattr(operator, "import_filter", None)
    if single is not None:
        declared.append(single)
    declared += list(getattr(operator, "import_filters", None) or ())
    for item in declared:
        if not isinstance(item, ImportFilter):
            raise DeclarationError(f"import_filter(s) take ImportFilter instances, got {type(item).__name__}")
    return declared


def _blank(value) -> bool:
    return value is None or not str(value).strip()


def _bound_topics(spec: ImportFilter, topics: typing.List[InputTopic]) -> typing.List[InputTopic]:
    imports = [t for t in topics if t.filterType == "ImportId"]
    if not spec.inputs:
        return imports
    return [t for t in imports if any(m.dest in spec.inputs for m in t.mappings)]


def _distance_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * math.asin(min(1.0, math.sqrt(a)))


def _coordinate(config, field: str, low: float, high: float) -> float:
    raw = getattr(config, field, None)
    try:
        value = float(str(raw).strip())
    except ValueError:
        raise MissingConfigValueError(f"config field '{field}' must be a number, got {raw!r}") from None
    if not low <= value <= high:
        raise MissingConfigValueError(f"config field '{field}' must be between {low} and {high}, got {value}")
    return value


def _reader(config, dep_config):
    """('dsn', conn_str) or ('wrapper', url, token), the same choice the history reads make."""
    ts_conn = getattr(dep_config, "ts_conn", None) or config.ts_conn
    if ts_conn:
        return ("dsn", ts_conn)
    token = getattr(dep_config, "senergy_token", None)
    if config.ts_wrapper_url and token:
        return ("wrapper", config.ts_wrapper_url, token)
    raise MissingConfigValueError(
        "cannot read the import's export for its filter: neither a database connection "
        "nor an authorised reader is configured")


def _check_columns(config, dep_config, entry: ImportExport, columns: typing.List[str]):
    # Lazy: ray and psycopg2 are not needed by an operator that declares no filter.
    reader = _reader(config, dep_config)
    if reader[0] == "dsn":
        from operator_lib.util.helpers import timescale
        timescale.check_export_columns(reader[1], entry, columns)
    else:
        from operator_lib.util.helpers import ts_wrapper
        ts_wrapper.check_export_columns(reader[1], reader[2], entry, columns)


def _read_places(config, dep_config, entry: ImportExport, columns: typing.List[str]):
    reader = _reader(config, dep_config)
    if reader[0] == "dsn":
        from operator_lib.util.helpers import timescale
        return timescale.read_export_places(reader[1], entry, columns, PLACES_WINDOW_DAYS)
    from operator_lib.util.helpers import ts_wrapper
    return ts_wrapper.read_export_places(reader[1], reader[2], entry, columns, PLACES_WINDOW_DAYS)


def _nearest(config, spec: ImportFilter, rows, source: str) -> typing.Tuple[str, float, float, float]:
    """The place of `rows` ((place, lat, lon) tuples) nearest to the operator's location."""
    lat = _coordinate(config, spec.near[0], -90.0, 90.0)
    lon = _coordinate(config, spec.near[1], -180.0, 180.0)
    best = None
    for place, p_lat, p_lon in rows:
        try:
            distance = _distance_km(lat, lon, float(p_lat), float(p_lon))
        except (TypeError, ValueError):
            continue
        if place is None or math.isnan(distance):
            continue
        if best is None or distance < best[1]:
            best = (str(place).strip(), distance, float(p_lat), float(p_lon))
    if best is None:
        raise MissingConfigValueError(
            f"{source} holds no place with a '{spec.path}' and a location "
            f"({spec.near_paths[0]}, {spec.near_paths[1]}), nothing to choose the nearest from")
    return best


def _places_from_kafka(dep_config, spec: ImportFilter, topic: InputTopic):
    """(place, lat, lon) of the last `places_scan_messages` messages of this import in the topic."""
    from operator_lib.util.helpers import kafka
    import_id = (topic.filterValue or "").split(",")[0].strip()
    paths = (spec.path, *spec.near_paths)

    def extract(message):
        # Kept per message: the identifier and the location, nothing else.
        if not isinstance(message, dict) or str(message.get("import_id", "")).strip() != import_id:
            return None
        row = tuple(value_at(message, path) for path in paths)
        return row if all(isinstance(v, (str, int, float)) for v in row) else None

    return kafka.read_recent_messages(
        dep_config.config_bootstrap_servers, topic.name, spec.places_scan_messages, extract)


def _check_config_fields(spec: ImportFilter, config):
    # A misspelt field would otherwise read as "not set" and switch the filter off.
    for name in (spec.value, *spec.near):
        if name and not hasattr(config, name):
            raise DeclarationError(
                f"import filter on '{spec.path}' names the config field '{name}', "
                f"which the operator's config type does not declare")


def resolve_import_filters(
    declared: typing.List[ImportFilter],
    config,
    dep_config,
    input_topics: typing.List[InputTopic],
    training_topics: typing.Optional[typing.List[InputTopic]] = None,
):
    """
    Resolve each declared filter once and activate it for its import inputs. A filter
    that was asked for, by a value or a location, but cannot be resolved raises:
    falling back to the unfiltered import would serve every place without a word.
    """
    clear()
    topics = list(input_topics or []) + list(training_topics or [])
    claimed: typing.Dict[typing.Tuple[str, str], ImportFilter] = {}
    for spec in declared:
        _check_config_fields(spec, config)
        bound = _bound_topics(spec, topics)
        if not bound:
            if spec.inputs:
                raise MissingConfigValueError(
                    f"import filter on '{spec.path}' names the inputs {', '.join(spec.inputs)}, "
                    f"none of which is an import input of this deployment")
            logger.info(f"import filter on '{spec.path}': the deployment has no import input")
            continue
        imports = {key for topic in bound for key in _keys(topic)}
        if len(imports) > 1:
            which = "has no inputs, but the operator reads" if not spec.inputs else "names inputs of"
            raise DeclarationError(
                f"import filter on '{spec.path}' {which} {len(imports)} imports "
                f"({', '.join(sorted(name for name, _ in imports))}); a filter applies to one import, "
                f"name only the inputs of one")
        for key in imports:
            if key in claimed and claimed[key] is not spec:
                raise DeclarationError(
                    f"import input {key[0]} ({key[1]}) is bound by two import filters, "
                    f"'{claimed[key].path}' and '{spec.path}'; give each filter its own inputs")
            claimed[key] = spec

        explicit = getattr(config, spec.value, None) if spec.value else None
        near_set = [not _blank(getattr(config, f, None)) for f in spec.near]
        if spec.near and any(near_set) and not all(near_set):
            raise MissingConfigValueError(
                f"config fields '{spec.near[0]}' and '{spec.near[1]}' must be set together")
        by_value = not _blank(explicit)
        by_place = bool(spec.near) and all(near_set)
        if not by_value and not by_place:
            logger.warning(
                f"import filter on '{spec.path}': neither "
                f"{('config field ' + repr(spec.value)) if spec.value else 'a value'}"
                f"{(' nor ' + repr(spec.near[0]) + '/' + repr(spec.near[1])) if spec.near else ''} is set, "
                f"{', '.join(t.name for t in bound)} is not filtered")
            for key in imports:
                claimed.pop(key, None)
            _resolved.append(ResolvedImportFilter(spec.path, spec.inputs, None, "none"))
            continue

        # An import whose mapped paths no export carries as columns (a forecast list, say) has
        # no entry: it is filtered live and in the Kafka history, and its place is found in Kafka.
        entry = next((e for e in (find_import_export(config, t) for t in bound) if e is not None), None)
        if entry is not None:
            _check_columns(config, dep_config, entry,
                           [spec.column, *(spec.near_columns if by_place else ())])
        found = ResolvedImportFilter(spec.path, spec.inputs, None, "near")
        if by_value:
            wanted = str(explicit).strip()
            how = f"config field '{spec.value}'"
            found = ResolvedImportFilter(spec.path, spec.inputs, wanted, "config")
        elif entry is not None:
            wanted, distance, place_lat, place_lon = _nearest(
                config, spec, _read_places(config, dep_config, entry, [spec.column, *spec.near_columns]),
                f"export {entry.export_id} in the last {PLACES_WINDOW_DAYS} days")
            how = (f"nearest to '{spec.near[0]}', '{spec.near[1]}' in export {entry.export_id}, "
                   f"{distance:.1f} km away")
            found = ResolvedImportFilter(spec.path, spec.inputs, wanted, "near", distance, place_lat, place_lon)
        else:
            wanted, distance, place_lat, place_lon = _nearest(
                config, spec, _places_from_kafka(dep_config, spec, bound[0]),
                f"the last {spec.places_scan_messages} messages of {bound[0].name}")
            how = (f"nearest to '{spec.near[0]}', '{spec.near[1]}' among the last "
                   f"{spec.places_scan_messages} messages of {bound[0].name}, {distance:.1f} km away")
            found = ResolvedImportFilter(spec.path, spec.inputs, wanted, "near", distance, place_lat, place_lon)
        _resolved.append(dataclasses.replace(found, value=wanted))
        resolved = ValueFilter(path=spec.path, value=wanted, column=spec.column)
        for key in imports:
            _active[key] = resolved
        logger.info(f"import filter on '{spec.path}': {resolved.value!r} from {how}; applies to "
                    f"{', '.join(t.name for t in bound)}")
