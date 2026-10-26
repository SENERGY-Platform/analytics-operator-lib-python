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
import sys
import unittest
from datetime import timedelta
from unittest import mock

import pandas as pd
import ray

import operator_lib.util as util
from operator_lib.util.helpers import data
from operator_lib.util.model import Config, InputTopic

E = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
SERVICE_UUID = "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
SERVICE_TOPIC = f"urn_infai_ses_service_{SERVICE_UUID}"


def _topic_dict(name, filter_type, filter_value):
    return {
        "name": name,
        "filterType": filter_type,
        "filterValue": filter_value,
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    }


def _frame_for(device):
    return pd.DataFrame({"time": [1, 2], "temp": [float(len(device)), 2.0]})


def _fake_local(ts_conn, topic, duration, require_full_duration, end):
    import ray.data
    return ray.data.from_pandas(_frame_for(topic.filterValue))


@ray.remote
def _fake_remote(ts_conn, topic, duration, require_full_duration, end):
    import ray.data
    return ray.data.from_pandas(_frame_for(topic.filterValue))


class _Env(unittest.TestCase):
    """
    The deployment config and the readers are replaced; the dispatch in
    data.__read_topic and the union run for real. Calls of the local fake are
    recorded in self.reader.
    """
    topics = []

    def setUp(self):
        opr = {
            "inputTopics": self.topics,
            "config": {"ts_conn": "postgres://x"},
        }
        dep = mock.Mock(pipeline_id="p", config_bootstrap_servers="k",
                        ts_conn=None, senergy_token=None)
        self.reader = mock.Mock(side_effect=_fake_local)
        for patch in (
            mock.patch.object(util, "DeploymentConfig", return_value=dep),
            mock.patch.object(util, "load_operator_config_json", return_value=opr),
            mock.patch.object(data, "get_timescale_dataset_local", self.reader),
            mock.patch.object(data, "get_timescale_dataset_remote", _fake_remote),
            mock.patch.object(data.clock, "fixed", return_value=E),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def _ends(self):
        return [c.args[4] for c in self.reader.call_args_list]


class TestMultiDevice(_Env):
    topics = [
        _topic_dict(SERVICE_TOPIC, "DeviceId", "a,b"),
        _topic_dict(SERVICE_TOPIC, "DeviceId", "c"),
    ]

    @classmethod
    def setUpClass(cls):
        # The fake reader lives in this module, which a worker may not be able
        # to import under every runner (discover vs. -m); ship it by value.
        ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def _check(self, ds_multi, ds_single):
        multi = ds_multi.to_pandas()
        self.assertEqual({"a", "b"}, set(multi["device_id"]))
        self.assertEqual(4, len(multi))
        for device in ("a", "b"):
            rows = multi[multi["device_id"] == device]
            self.assertEqual([1, 2], sorted(rows["time"]))
        single = ds_single.to_pandas()
        self.assertNotIn("device_id", single.columns)
        self.assertEqual(2, len(single))

    def test_local_returns_one_dataset_per_topic_with_a_device_column(self):
        result = data.provide_historic_data_local(
            timedelta(days=1), require_full_duration=True)
        self.assertEqual(2, len(result))
        self._check(*result)
        # a, b, then c: the order of the config, devices in the order written.
        self.assertEqual(["a", "b", "c"],
                         [c.args[1].filterValue for c in self.reader.call_args_list])

    def test_remote_returns_one_object_ref_per_topic_with_a_device_column(self):
        result = data.provide_historic_data(timedelta(days=1))
        self.assertEqual(2, len(result))
        self.assertTrue(all(isinstance(r, ray.ObjectRef) for r in result))
        self._check(*[ray.get(r) for r in result])

    def test_the_bound_and_full_duration_reach_every_device(self):
        data.provide_historic_data_local(timedelta(days=1), require_full_duration=True)
        self.assertEqual(3, self.reader.call_count)
        for call in self.reader.call_args_list:
            self.assertEqual(timedelta(days=1), call.args[2])
            self.assertTrue(call.args[3])
            self.assertEqual(E, call.args[4])

    def test_read_input_window_passes_its_window_to_every_device(self):
        start = E - timedelta(hours=6)
        result = data.read_input_window(start, E)
        self.assertEqual(2, len(result))
        (topic0, frame0), (topic1, frame1) = result
        self.assertEqual("a,b", topic0.filterValue)
        self.assertEqual("c", topic1.filterValue)
        self.assertEqual({"a", "b"}, set(frame0["device_id"]))
        self.assertNotIn("device_id", frame1.columns)
        for call in self.reader.call_args_list:
            self.assertEqual(timedelta(hours=6), call.args[2])
            self.assertFalse(call.args[3])
            self.assertEqual(E, call.args[4])

    def test_the_input_topic_itself_is_not_changed(self):
        result = data.read_input_window(E - timedelta(hours=1), E)
        self.assertEqual("a,b", result[0][0].filterValue)


class TestSingleDevice(_Env):
    topics = [_topic_dict(SERVICE_TOPIC, "DeviceId", "a")]

    def test_one_device_reads_once_without_a_device_column(self):
        (ds,) = data.provide_historic_data_local(timedelta(days=1))
        self.assertNotIn("device_id", ds.to_pandas().columns)
        self.reader.assert_called_once()
        self.assertEqual("a", self.reader.call_args.args[1].filterValue)


class TestTrailingComma(TestSingleDevice):
    topics = [_topic_dict(SERVICE_TOPIC, "DeviceId", "a, ")]


class TestSplitDevices(unittest.TestCase):
    def test_empty_entries_are_dropped_and_repeats_read_once(self):
        self.assertEqual(["a", "b"], data._split_devices(" a, ,b ,a,"))
        self.assertEqual([], data._split_devices(","))
        self.assertEqual([], data._split_devices(None))


class TestOperatorIdTopic(_Env):
    topics = [_topic_dict("analytics-upstream", "OperatorId", "op:pipe,x:y")]

    def test_an_operator_id_topic_is_not_split(self):
        kafka = mock.Mock(side_effect=lambda *a: _fake_local(None, a[1], None, None, None))
        with mock.patch.object(data, "get_kafka_dataset_local", kafka):
            (ds,) = data.provide_historic_data_local(timedelta(days=1))
        kafka.assert_called_once()
        self.assertEqual("op:pipe,x:y", kafka.call_args.args[1].filterValue)
        self.assertNotIn("device_id", ds.to_pandas().columns)
        self.reader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
