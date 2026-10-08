import datetime
import unittest
from unittest import mock

from operator_lib.util.op_base import OperatorBase


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


def call_run(ret):
    operator = _Operator(ret)
    handler = mock.Mock()
    handler.get_results.return_value = [_Result({"v": 1})]
    handler.get_filter_args.return_value = {"selector": None}
    setattr(operator, "_OperatorBase__filter_handler", handler)
    return operator._OperatorBase__call_run({}, "device", datetime.datetime.now(datetime.timezone.utc))


class TestCallRunPairsTimestamps(unittest.TestCase):
    def test_several_results_without_a_timestamp_get_one_none_each(self):
        dts, results = call_run([{"a": 1}, {"b": 2}, {"c": 3}])
        self.assertEqual([{"a": 1}, {"b": 2}, {"c": 3}], results)
        self.assertEqual([None, None, None], dts)

    def test_several_results_with_one_timestamp_share_it(self):
        at = datetime.datetime(2026, 10, 8, tzinfo=datetime.timezone.utc)
        dts, results = call_run((at, [{"a": 1}, {"b": 2}]))
        self.assertEqual([at, at], dts)

    def test_a_single_result_keeps_its_timestamp(self):
        at = datetime.datetime(2026, 10, 8, tzinfo=datetime.timezone.utc)
        self.assertEqual(([at], [{"a": 1}]), call_run((at, {"a": 1})))
        self.assertEqual(([None], [{"a": 1}]), call_run({"a": 1}))


if __name__ == "__main__":
    unittest.main()
