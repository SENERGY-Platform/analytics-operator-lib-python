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

import datetime
import unittest
from unittest import mock

from operator_lib.util import Config, Inputs, Outputs, DeclarationError
from operator_lib.util.declaration import config_fields
from operator_lib.util.op_base import OperatorBase

VALUES = {"retries": "3", "ratio": "0.5", "enabled": "False", "label": "x", "logger_level": "debug"}


class Untyped(Config):
    retries = 1
    ratio = 1.0
    enabled = True
    label = ""


class Typed(Config):
    retries: int = 1
    ratio: float = 1.0
    enabled: bool = True
    label: str = ""


class Narrower(Typed):
    retries: int = 5
    extra: str = "e"


class TestConfigParsingUnchanged(unittest.TestCase):
    def test_annotations_do_not_change_what_a_string_valued_config_parses_to(self):
        untyped, typed = dict(Untyped(dict(VALUES))), dict(Typed(dict(VALUES)))
        self.assertEqual(untyped, typed)
        self.assertEqual("3", typed["retries"])
        self.assertEqual("False", typed["enabled"])

    def test_the_defaults_are_the_same_with_and_without_annotations(self):
        self.assertEqual(dict(Untyped({})), dict(Typed({})))

    def test_the_own_field_marker_stays_out_of_the_parsed_values(self):
        self.assertNotIn("_own_fields", dict(Typed({})))


class TestConfigFields(unittest.TestCase):
    def test_only_the_fields_of_the_operators_own_class_count(self):
        self.assertEqual(
            [("retries", "int"), ("ratio", "float"), ("enabled", "bool"), ("label", "string")],
            config_fields(Typed),
        )

    def test_a_subclass_of_a_config_adds_to_its_parents_and_may_redeclare(self):
        self.assertEqual(
            [("retries", "int"), ("ratio", "float"), ("enabled", "bool"), ("label", "string"), ("extra", "string")],
            config_fields(Narrower),
        )

    def test_a_redeclared_library_field_is_part_of_the_operators_config(self):
        class Redeclared(Config):
            logger_level: str = "info"
        self.assertEqual([("logger_level", "string")], config_fields(Redeclared))

    def test_the_base_config_has_no_fields_of_its_own(self):
        self.assertEqual([], config_fields(Config))

    def test_a_field_without_a_type_is_an_error_naming_it(self):
        with self.assertRaises(DeclarationError) as ctx:
            config_fields(Untyped)
        self.assertIn("Untyped.retries", str(ctx.exception))

    def test_an_unsupported_type_is_an_error_naming_it(self):
        class Bad(Config):
            names: list = None
        with self.assertRaises(DeclarationError) as ctx:
            config_fields(Bad)
        self.assertIn("Bad.names", str(ctx.exception))

    def test_a_type_without_a_default_is_an_error_because_the_config_never_sets_it(self):
        class NoDefault(Config):
            retries: int
        with self.assertRaises(DeclarationError) as ctx:
            config_fields(NoDefault)
        self.assertIn("NoDefault.retries", str(ctx.exception))


class Out(Outputs):
    window_open: bool
    confidence: float


class Derived(Out):
    reason: str


class TestDeclarations(unittest.TestCase):
    def test_fields_keep_the_declaration_order_and_map_the_types(self):
        self.assertEqual([("window_open", "bool"), ("confidence", "float")], Out.fields())

    def test_a_subclass_extends_its_parent(self):
        self.assertEqual(["window_open", "confidence", "reason"], Derived.names())

    def test_all_four_types_map(self):
        class All(Inputs):
            a: str
            b: float
            c: int
            d: bool
        self.assertEqual([("a", "string"), ("b", "float"), ("c", "int"), ("d", "bool")], All.fields())

    def test_an_unsupported_type_is_an_error_naming_it(self):
        class Bad(Inputs):
            when: datetime.datetime
        with self.assertRaises(DeclarationError) as ctx:
            Bad.fields()
        self.assertIn("Bad.when", str(ctx.exception))


class _Result:
    def __init__(self, data):
        self.ex = None
        self.data = data
        self.filter_ids = ["f"]


class _Operator(OperatorBase):
    def __init__(self, ret):
        self.ret = ret

    def run(self, data, selector, device_id, timestamp):
        return self.ret


class _Declaring(_Operator):
    outputs = Out


def call_run(operator, *rets):
    handler = mock.Mock()
    handler.get_results.return_value = [_Result({"v": 1})]
    handler.get_filter_args.return_value = {"selector": None}
    setattr(operator, "_OperatorBase__filter_handler", handler)
    results = []
    for ret in rets:
        operator.ret = ret
        results.append(operator._OperatorBase__call_run({}, "device", datetime.datetime.now(datetime.timezone.utc)))
    return results


class TestUndeclaredOutputWarning(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch("operator_lib.util.op_base.logger")
        self.logger = patcher.start()
        self.addCleanup(patcher.stop)

    def test_an_undeclared_key_warns_once_not_per_message(self):
        call_run(_Declaring(None), {"window_open": True, "oops": 1}, {"oops": 2}, {"oops": 3})
        self.assertEqual(1, self.logger.warning.call_count)
        self.assertIn("oops", self.logger.warning.call_args[0][0])

    def test_each_undeclared_key_warns_once(self):
        call_run(_Declaring(None), {"a": 1, "b": 2}, {"a": 1, "b": 2})
        self.assertEqual(2, self.logger.warning.call_count)

    def test_declared_keys_do_not_warn(self):
        call_run(_Declaring(None), {"window_open": True, "confidence": 0.5})
        self.logger.warning.assert_not_called()

    def test_every_dict_of_a_list_result_is_checked(self):
        call_run(_Declaring(None), [{"window_open": True}, {"oops": 1}])
        self.assertEqual(1, self.logger.warning.call_count)

    def test_nothing_is_warned_or_changed_without_declared_outputs(self):
        operator = _Operator(None)
        ((dts, results),) = call_run(operator, {"anything": 1})
        self.logger.warning.assert_not_called()
        self.assertEqual([{"anything": 1}], results)

    def test_an_outputs_attribute_that_is_not_a_declaration_is_left_alone(self):
        operator = _Operator(None)
        operator.outputs = []
        ((dts, results),) = call_run(operator, {"anything": 1})
        self.logger.warning.assert_not_called()
        self.assertEqual([{"anything": 1}], results)

    def test_the_result_is_passed_on_unchanged_when_a_key_is_undeclared(self):
        ((dts, results),) = call_run(_Declaring(None), {"oops": 1})
        self.assertEqual([{"oops": 1}], results)


if __name__ == "__main__":
    unittest.main()
