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

__all__ = ("Inputs", "Outputs", "ImportFilter", "DeclarationError")

import dataclasses
import inspect
import typing

from .model import Config

TYPE_NAMES = {str: "string", float: "float", int: "int", bool: "bool"}
_BY_NAME = {t.__name__: t for t in TYPE_NAMES}


class DeclarationError(Exception):
    pass


def _type_name(owner: str, field: str, annotation) -> str:
    # A module with `from __future__ import annotations` hands over the type's name.
    if isinstance(annotation, str):
        annotation = _BY_NAME.get(annotation.strip(), annotation)
    try:
        return TYPE_NAMES[annotation]
    except (KeyError, TypeError):
        supported = ", ".join(t.__name__ for t in TYPE_NAMES)
        raise DeclarationError(
            f"{owner}.{field}: unsupported type {annotation!r}, use one of {supported}"
        ) from None


def _annotations(classes) -> typing.Dict[str, typing.Any]:
    # Parents first, so a redeclared field keeps its position and takes the nearest type.
    merged = {}
    for cls in classes:
        for name, annotation in inspect.get_annotations(cls).items():
            if not name.startswith("_"):
                merged[name] = annotation
    return merged


class _Declaration:
    """Fields are class annotations, in declaration order."""

    @classmethod
    def _classes(cls):
        return [c for c in reversed(cls.__mro__) if c not in (object, _Declaration, Inputs, Outputs)]

    @classmethod
    def names(cls) -> typing.List[str]:
        return list(_annotations(cls._classes()))

    @classmethod
    def fields(cls) -> typing.List[typing.Tuple[str, str]]:
        return [(name, _type_name(cls.__name__, name, a)) for name, a in _annotations(cls._classes()).items()]


@dataclasses.dataclass(frozen=True)
class ImportFilter:
    """
    Narrows import inputs to the messages of one place: a shared import carries many
    locations in one topic. Declared on the operator as `import_filter` (one filter) or
    `import_filters` (a list).

    path: dotted path of the identifying value in the raw import message, e.g. "value.meta.id".
    column: the export column that holds it, for the history, e.g. "station_id". The deployer's
        `import_exports` lists only the columns of mapped inputs, so the name is given here.
    value: config field holding the wanted value; may be empty.
    near, near_paths, near_columns: config fields with a latitude and a longitude, and the
        message paths and export columns of the two; the nearest place is chosen when `value`
        is empty.
    inputs: the declared inputs (mapping dests) whose import topics the filter applies to;
        required when the operator reads more than one import, empty means the only import.
    places_scan_messages: for an import without an export, how many of the last messages
        per partition of the topic are read to find the nearest place.
    """
    path: str
    column: str
    value: typing.Optional[str] = None
    near: typing.Tuple[str, ...] = ()
    near_paths: typing.Tuple[str, ...] = ()
    near_columns: typing.Tuple[str, ...] = ()
    inputs: typing.Tuple[str, ...] = ()
    places_scan_messages: int = 20000

    def __post_init__(self):
        # Accept lists, keep the instance hashable.
        for name in ("near", "near_paths", "near_columns", "inputs"):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        for name in ("path", "column"):
            if not getattr(self, name) or not isinstance(getattr(self, name), str):
                raise DeclarationError(f"ImportFilter: {name} must be a non-empty string")
        if self.near or self.near_paths or self.near_columns:
            if not (len(self.near) == len(self.near_paths) == len(self.near_columns) == 2):
                raise DeclarationError(
                    "ImportFilter: near, near_paths and near_columns each take two entries, latitude then longitude")
        if not isinstance(self.places_scan_messages, int) or self.places_scan_messages < 1:
            raise DeclarationError("ImportFilter: places_scan_messages must be a positive integer")
        if not self.value and not self.near:
            raise DeclarationError("ImportFilter: set value, near, or both, otherwise there is nothing to resolve")


class Inputs(_Declaration):
    """Base class of an operator's declared inputs, e.g. `window_open: bool`."""


class Outputs(_Declaration):
    """Base class of an operator's declared outputs, e.g. `window_open: bool`."""


def is_declaration(value, base) -> bool:
    return isinstance(value, type) and issubclass(value, base)


def config_fields(config_type) -> typing.List[typing.Tuple[str, str]]:
    """
    The typed fields the operator's own Config subclasses declare; the library's
    base fields are not part of it. The annotation is the declaration only:
    simple_struct ignores it, so a typed config parses exactly as an untyped one.
    """
    classes = [c for c in reversed(config_type.__mro__) if issubclass(c, Config) and c is not Config]
    own = []
    for c in classes:
        own += [n for n in vars(c).get("_own_fields", ()) if n not in own]
    annotations = _annotations(classes)
    fields = []
    for name in own:
        if name not in annotations:
            raise DeclarationError(f"{config_type.__name__}.{name}: config field without a type annotation")
        fields.append((name, _type_name(config_type.__name__, name, annotations[name])))
    for name in annotations:
        if name not in own:
            # simple_struct only parses attributes that have a value in the class body.
            raise DeclarationError(f"{config_type.__name__}.{name}: typed but without a default, the config never sets it")
    return fields
