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
from datetime import timedelta
from unittest import mock

from operator_lib.util import clock
from operator_lib.util.model import Config, InputTopic
from operator_lib.util.op_base import OperatorBase
from operator_lib.util.helpers import ts_wrapper
from operator_lib.util.helpers import timescale
from operator_lib.util.helpers import kafka as kafka_module

E = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

# UUID-shaped: timescale.py's table-name derivation base64-encodes the raw
# bytes of the device and service ids, which only a hex string survives.
DEVICE_ID = "0362b1a1-a1a1-4b2b-8c3c-9d4d5e6f7a8b"
SERVICE_UUID = "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"


def _topic():
    return InputTopic({
        "name": f"urn_infai_ses_service_{SERVICE_UUID}",
        "filterType": "DeviceId",
        "filterValue": DEVICE_ID,
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    })


class TestTsWrapperBound(unittest.TestCase):
    def setUp(self):
        self.conf = _topic()

    def test_a_fixed_end_posts_the_exact_window(self):
        with mock.patch.object(ts_wrapper, "_post", return_value=[]) as post:
            ts_wrapper.read_history(
                "http://wrapper", "token", self.conf, timedelta(days=7), end=E)
        post.assert_called_once()
        _, _, elements = post.call_args.args
        self.assertEqual(1, len(elements))
        self.assertEqual(
            ts_wrapper._format_time(E - timedelta(days=7)), elements[0]["time"]["start"])
        self.assertEqual(ts_wrapper._format_time(E), elements[0]["time"]["end"])

    def test_require_full_duration_with_a_fixed_end_raises_on_short_reach(self):
        probe_time = E - timedelta(days=1)
        payload = [{"data": [[[ts_wrapper._format_time(probe_time), "1.0"]]]}]
        with mock.patch.object(ts_wrapper, "_post", return_value=payload):
            with self.assertRaises(ValueError) as ctx:
                ts_wrapper.read_history(
                    "http://wrapper", "token", self.conf, timedelta(days=7),
                    require_full_duration=True, end=E)
        message = str(ctx.exception)
        self.assertIn(str(timedelta(days=7)), message)
        self.assertIn(E.isoformat(), message)

    def test_require_full_duration_with_a_fixed_end_and_full_reach_does_not_raise(self):
        probe_time = E - timedelta(days=8)
        payload = [{"data": [[[ts_wrapper._format_time(probe_time), "1.0"]]]}]
        with mock.patch.object(ts_wrapper, "_post", return_value=payload):
            ts_wrapper.read_history(
                "http://wrapper", "token", self.conf, timedelta(days=7),
                require_full_duration=True, end=E)

    def test_no_end_posts_an_end_within_two_seconds_of_now(self):
        with mock.patch.object(ts_wrapper, "_post", return_value=[]) as post:
            ts_wrapper.read_history(
                "http://wrapper", "token", self.conf, timedelta(days=1), end=None)
        _, _, elements = post.call_args.args
        posted_end = datetime.datetime.strptime(
            elements[0]["time"]["end"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=datetime.timezone.utc)
        now = datetime.datetime.now(datetime.timezone.utc)
        self.assertLessEqual(abs((now - posted_end).total_seconds()), 2)


class TestTimescaleBound(unittest.TestCase):
    def setUp(self):
        self.conf = _topic()
        self.query_fn = getattr(timescale, "__get_timescale_dataset_query")
        self.literal_fn = getattr(timescale, "__timestamptz_literal")

    def test_a_fixed_end_renders_literal_bounds(self):
        query = self.query_fn("dsn", self.conf, timedelta(hours=1), False, end=E)
        self.assertIn("time >= TIMESTAMPTZ '", query)
        self.assertIn(f"time < TIMESTAMPTZ '{self.literal_fn(E)}'", query)
        self.assertNotIn("NOW()", query)

    def test_no_end_keeps_todays_sql(self):
        query = self.query_fn("dsn", self.conf, timedelta(hours=1), False, end=None)
        self.assertIn("NOW() - INTERVAL", query)

    def test_require_full_duration_with_a_fixed_end_raises_on_short_reach(self):
        fake_cursor = mock.MagicMock()
        fake_cursor.fetchone.return_value = (E - timedelta(minutes=30),)
        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=fake_conn):
            with self.assertRaises(ValueError):
                self.query_fn("dsn", self.conf, timedelta(hours=1), True, end=E)

    def test_require_full_duration_with_a_fixed_end_and_full_reach_does_not_raise(self):
        fake_cursor = mock.MagicMock()
        fake_cursor.fetchone.return_value = (E - timedelta(hours=2),)
        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=fake_conn):
            self.query_fn("dsn", self.conf, timedelta(hours=1), True, end=E)


class TestKafkaBound(unittest.TestCase):
    def setUp(self):
        self.conf = _topic()

    def test_a_fixed_end_seeks_start_offset_and_applies_a_filter(self):
        fake_ds = mock.MagicMock()
        fake_ds.filter.return_value = fake_ds
        with mock.patch.object(kafka_module.ray.data, "read_kafka", return_value=fake_ds) as read_kafka:
            kafka_module.get_kafka_dataset_local(
                "bootstrap:9092", self.conf, "pipeline-1", timedelta(hours=1), False, end=E)
        read_kafka.assert_called_once()
        self.assertEqual(E - timedelta(hours=1), read_kafka.call_args.kwargs["start_offset"])
        fake_ds.filter.assert_called_once()

    def test_require_full_duration_with_a_fixed_end_raises_on_short_reach(self):
        fake_ds = mock.MagicMock()
        fake_ds.filter.return_value = fake_ds
        fake_ds.take.return_value = [
            {"timestamp": int((E - timedelta(minutes=5)).timestamp() * 1000)}
        ]
        with mock.patch.object(kafka_module.ray.data, "read_kafka", return_value=fake_ds):
            with self.assertRaises(ValueError):
                kafka_module.get_kafka_dataset_local(
                    "bootstrap:9092", self.conf, "pipeline-1", timedelta(hours=1),
                    True, end=E)

    def test_require_full_duration_with_a_fixed_end_and_full_reach_does_not_raise(self):
        fake_ds = mock.MagicMock()
        fake_ds.filter.return_value = fake_ds
        fake_ds.take.return_value = [
            {"timestamp": int((E - timedelta(minutes=55)).timestamp() * 1000)}
        ]
        with mock.patch.object(kafka_module.ray.data, "read_kafka", return_value=fake_ds):
            kafka_module.get_kafka_dataset_local(
                "bootstrap:9092", self.conf, "pipeline-1", timedelta(hours=1),
                True, end=E)


class TestOperatorBaseClock(unittest.TestCase):
    def setUp(self):
        self.addCleanup(clock.set_fixed, None)

    def _init(self, config):
        operator = OperatorBase()
        operator.init(
            kafka_consumer=None,
            kafka_producer=None,
            filter_handler=None,
            output_topic=None,
            pipeline_id="p",
            operator_id="o",
            config=config,
        )
        return operator

    def test_a_training_end_on_the_config_sets_the_clock(self):
        self._init(Config({"training_end": "2026-06-01T00:00:00Z"}))
        self.assertEqual(
            datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc), clock.fixed())

    def test_no_training_end_releases_the_clock(self):
        clock.set_fixed(datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc))
        self._init(Config({}))
        self.assertIsNone(clock.fixed())


if __name__ == "__main__":
    unittest.main()
