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

__all__ = ("Inputs", "Outputs", "DeclarationError")

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
