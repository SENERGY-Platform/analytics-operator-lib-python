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
The exports a deployment names as the history of its import inputs.

An import topic is a Kafka topic with a retention of days, so its history
cannot be trained on. analytics-serving exports write the same messages to
timescale without a limit, and the deployer -- which holds the user's token and
checks the user's Execute permission on the export, none of which a running
operator can do -- resolves which export belongs to which import and writes the
result into the operator config as `import_exports`. This module only parses
that value and answers lookups against it. It decides nothing about access: an
entry is trusted because the deployer wrote it, and a topic without an entry is
read from Kafka exactly as before.
"""

__all__ = (
    "ImportExport",
    "parse_import_exports",
    "find_import_export",
    "export_column_pairs",
)

import json
import typing
from dataclasses import dataclass, field

from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.model import InputTopic, Mapping

CONFIG_KEY = "import_exports"

_REQUIRED_TEXT_FIELDS = ("topic", "import_id", "export_id", "table")


@dataclass(frozen=True)
class ImportExport:
    topic: str
    import_id: str
    export_id: str
    # The timescale table of the export, taken as given. Not derived from the
    # export id here: the name embeds the owner's database id as well, which an
    # operator has no way to know.
    table: str
    # Message-relative mapping source (e.g. "value.temp") to the export's
    # timescale column. A dict inside a frozen dataclass is not hashable, which
    # nothing here needs; it is a ray task argument, which is pickled.
    columns: typing.Dict[str, str] = field(default_factory=dict)


def parse_import_exports(raw: typing.Optional[str]) -> typing.List[ImportExport]:
    """
    Parse the JSON-encoded `import_exports` config value.

    Absent (None or blank) means no entries. Anything else that does not parse
    is an error rather than "no entries": a deployer that wrote a value meant
    it, and falling back to Kafka on a typo would train on a week of data while
    the author believes it is a year, with nothing to say so.
    """
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return []
    if not isinstance(raw, (str, bytes)):
        raise MissingConfigValueError(
            f"'{CONFIG_KEY}' in the operator config must be a JSON-encoded string, "
            f"got {type(raw).__name__}")
    try:
        decoded = json.loads(raw)
    except ValueError as err:
        raise MissingConfigValueError(
            f"'{CONFIG_KEY}' in the operator config is not valid JSON: {err}")
    if not isinstance(decoded, list):
        raise MissingConfigValueError(
            f"'{CONFIG_KEY}' in the operator config must decode to a list of entries, "
            f"got {type(decoded).__name__}")
    return [_parse_entry(index, item) for index, item in enumerate(decoded)]


def _parse_entry(index: int, item: typing.Any) -> ImportExport:
    where = f"'{CONFIG_KEY}'[{index}]"
    if not isinstance(item, dict):
        raise MissingConfigValueError(
            f"{where} must be an object, got {type(item).__name__}")
    for name in _REQUIRED_TEXT_FIELDS:
        value = item.get(name)
        if not isinstance(value, str) or not value:
            raise MissingConfigValueError(
                f"{where} is missing '{name}' or it is not a non-empty string")
    columns = item.get("columns")
    if not isinstance(columns, dict):
        raise MissingConfigValueError(
            f"{where} (topic {item['topic']}) is missing 'columns' or it is not an "
            f"object mapping a mapping source to an export column")
    for source, column in columns.items():
        if not isinstance(column, str) or not column:
            raise MissingConfigValueError(
                f"{where} (topic {item['topic']}) maps source '{source}' to "
                f"something other than a non-empty column name")
    return ImportExport(
        topic=item["topic"],
        import_id=item["import_id"],
        export_id=item["export_id"],
        table=item["table"],
        columns=dict(columns),
    )


def find_import_export(config, topic: InputTopic) -> typing.Optional[ImportExport]:
    """
    The entry for this input topic, or None. Matched on the topic name and the
    import id together, because one import can feed several topics and one topic
    name can in principle carry several imports; the deployer keys it the same way.
    The import id is compared trimmed, because the deployer trims the filter value
    before it writes the entry.
    """
    import_id = (topic.filterValue or "").strip()
    for entry in parse_import_exports(getattr(config, CONFIG_KEY, None)):
        if entry.topic == topic.name and entry.import_id == import_id:
            return entry
    return None


def export_column(entry: ImportExport, mapping: Mapping) -> str:
    column = entry.columns.get(mapping.source)
    if column is None:
        # The deployer only writes an entry whose columns cover every mapping of
        # the topic, so a gap means the config and the input topic disagree.
        # Guessing a column from the path would read the wrong series quietly.
        raise MissingConfigValueError(
            f"export {entry.export_id} for topic {entry.topic} has no column for "
            f"mapping source '{mapping.source}'; 'columns' in '{CONFIG_KEY}' must cover "
            f"every mapping of the input topic")
    return column


def export_column_pairs(entry: ImportExport, topic: InputTopic) -> typing.List[typing.Tuple[str, str]]:
    """The (export column, mapping dest) of every mapping of the topic, in mapping order."""
    return [(export_column(entry, mapping), mapping.dest) for mapping in topic.mappings]
