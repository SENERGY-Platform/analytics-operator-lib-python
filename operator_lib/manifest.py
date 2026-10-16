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
Writes operator-manifest.json from the declarations of an operator:

    python -m operator_lib manifest path/to/main.py [--operator ClassName] [-o operator-manifest.json]
"""

__all__ = ("build_manifest", "find_operators", "load_module", "main")

import argparse
import importlib.util
import json
import os
import sys
import typing

from operator_lib.util.declaration import DeclarationError, Inputs, Outputs, config_fields, is_declaration
from operator_lib.util.op_base import OperatorBase


class ManifestError(Exception):
    pass


def load_module(path: str):
    # Not as __main__, so an operator's entry point under `if __name__ == "__main__"` stays unrun.
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise ManifestError(f"{path}: no such file")
    # The operator imports its sibling modules.
    sys.path.insert(0, os.path.dirname(path))
    name = "_operator_manifest_target"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def find_operators(module) -> typing.List[type]:
    return [
        v for v in vars(module).values()
        if isinstance(v, type) and issubclass(v, OperatorBase) and v.__module__ == module.__name__
    ]


def _select(module, name: typing.Optional[str]) -> type:
    candidates = find_operators(module)
    names = [c.__name__ for c in candidates]
    if name is not None:
        if name not in names:
            raise ManifestError(f"no operator class {name!r} in the file, found: {', '.join(names) or 'none'}")
        return candidates[names.index(name)]
    if len(candidates) != 1:
        raise ManifestError(
            f"expected exactly one operator class in the file, found: {', '.join(names) or 'none'}; pick one with --operator"
        )
    return candidates[0]


def _entries(fields):
    return [{"name": n, "type": t} for n, t in fields]


def build_manifest(operator: type) -> typing.Optional[dict]:
    """None unless the operator declares both inputs and outputs."""
    if not (is_declaration(operator.inputs, Inputs) and is_declaration(operator.outputs, Outputs)):
        return None
    return {
        "inputs": _entries(operator.inputs.fields()),
        "outputs": _entries(operator.outputs.fields()),
        "config_values": _entries(config_fields(operator.configType)),
    }


def main(argv: typing.Optional[typing.List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m operator_lib")
    sub = parser.add_subparsers(dest="command", required=True)
    cmd = sub.add_parser("manifest", help="write operator-manifest.json from the operator's declarations")
    cmd.add_argument("file", help="the operator's main.py")
    cmd.add_argument("--operator", help="class name, if the file defines several operators")
    cmd.add_argument("-o", "--output", default="operator-manifest.json")
    args = parser.parse_args(argv)
    try:
        operator = _select(load_module(args.file), args.operator)
        manifest = build_manifest(operator)
    except (ManifestError, DeclarationError) as ex:
        print(f"error: {ex}", file=sys.stderr)
        return 1
    if manifest is None:
        print(f"{operator.__name__} declares no inputs and outputs, nothing written")
        return 0
    with open(args.output, "w") as f:
        f.write(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {args.output}")
    return 0
