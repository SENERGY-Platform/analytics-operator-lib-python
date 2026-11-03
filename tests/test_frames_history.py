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
import json
import re
import unittest
from datetime import timedelta
from unittest import mock

import pandas as pd
import ray

import operator_lib.util as util
from operator_lib.util.helpers import HistoryNeedsRayError, data, provide_historic_frames_local, timescale

E = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
SERVICE_UUID = "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
SERVICE_TOPIC = f"urn_infai_ses_service_{SERVICE_UUID}"
DSN = "postgres://u:p@h:5432/db"
DEVICES = {d: f"{i:08x}-0000-4000-8000-000000000000" for i, d in enumerate("abc", start=1)}
_get_table_name = getattr(timescale, "__get_table_name")
TABLES = {_get_table_name(DEVICES[d], SERVICE_TOPIC.replace("_", ":")): d for d in DEVICES}

IMPORT_ID = "urn:infai:ses:import:a50aa583-282e-56c2-b101-7aaf68ebd2b9"
IMPORT_TOPIC = "urn_infai_ses_import_a50aa583-282e-56c2-b101-7aaf68ebd2b9"
EXPORT_TABLE = "userid:yk0RSePtTgueSTvakI3kNg_export:4xd1ukv1RqW6j4Gts5dxBA"
TABLES[EXPORT_TABLE] = "x"
IMPORT_EXPORTS = json.dumps([{
    "topic": IMPORT_TOPIC,
    "import_id": IMPORT_ID,
    "export_id": "e31775ba-4bf5-46a5-ba8f-81adb3977104",
    "table": EXPORT_TABLE,
    "columns": {"value.temp": "temp"},
}])


def _device_topic(*letters):
    return {
        "name": SERVICE_TOPIC,
        "filterType": "DeviceId",
        "filterValue": ",".join(DEVICES[x] for x in letters),
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    }


def _import_topic():
    return {
        "name": IMPORT_TOPIC,
        "filterType": "ImportId",
        "filterValue": IMPORT_ID,
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    }


def _operator_topic():
    return {
        "name": "analytics-upstream",
        "filterType": "OperatorId",
        "filterValue": "op:pipe",
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    }


class FakeCursor:
    """Two rows per table, with tz-aware times as psycopg2 returns them for timestamptz."""

    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self.rows = []

    def execute(self, query, *args):
        self.conn.queries.append(query)
        device = TABLES[re.search(r'FROM\s+"([^"]+)"', query).group(1)]
        dests = list(dict.fromkeys(re.findall(r'AS "([^"]+)"', query)))
        self.description = [("time",)] + [(d,) for d in dests]
        base = datetime.datetime(2026, 5, 31, tzinfo=datetime.timezone.utc)
        self.rows = [(base + timedelta(hours=h),) + (float(ord(device) + h),) * len(dests) for h in (1, 2)]

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.queries = []
        self.closed = False
        self.autocommit = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakeDb:
    def __init__(self):
        self.conns = []

    def __call__(self, conn_str):
        conn = FakeConn()
        self.conns.append(conn)
        return conn

    @property
    def queries(self):
        return [q for c in self.conns for q in c.queries]


class _Base(unittest.TestCase):
    """
    The deployment config and the database connection are replaced; the dispatch
    in data.__read_topic and the readers in timescale run for real. Ray is shut
    down before the class and must still be down after every read.
    """
    topics = []
    config = {"ts_conn": DSN}
    token = None

    @classmethod
    def setUpClass(cls):
        ray.shutdown()

    def setUp(self):
        data._logged_import_sources.clear()
        opr = {"inputTopics": self.topics, "config": self.config}
        dep = mock.Mock(pipeline_id="p", config_bootstrap_servers="k", ts_conn=None, senergy_token=self.token)
        self.db = FakeDb()
        self.kafka = mock.Mock()
        for patch in (
            mock.patch.object(util, "DeploymentConfig", return_value=dep),
            mock.patch.object(util, "load_operator_config_json", return_value=opr),
            mock.patch.object(data.clock, "fixed", return_value=E),
            mock.patch.object(timescale, "__create_timescale_connection", self.db),
            mock.patch.object(data, "get_kafka_dataset_local", self.kafka),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(lambda: self.assertFalse(ray.is_initialized(), "the read started Ray"))


class TestFramesFromTimescale(_Base):
    topics = [_device_topic("a"), _device_topic("b", "c")]

    def test_each_input_comes_back_as_a_pandas_frame_in_config_order(self):
        single, multi = provide_historic_frames_local(timedelta(days=1))
        self.assertIsInstance(single, pd.DataFrame)
        self.assertEqual(["time", "temp"], list(single.columns))
        self.assertEqual([float(ord("a") + 1), float(ord("a") + 2)], list(single["temp"]))
        self.assertIsInstance(multi, pd.DataFrame)

    def test_reads_without_starting_ray(self):
        provide_historic_frames_local(timedelta(days=1))
        self.assertFalse(ray.is_initialized())

    def test_time_is_utc_naive(self):
        single, _ = provide_historic_frames_local(timedelta(days=1))
        self.assertIsNone(single["time"].dt.tz)
        self.assertEqual(pd.Timestamp("2026-05-31T01:00:00"), single["time"].iloc[0])

    def test_an_input_with_several_devices_has_a_device_id_column(self):
        single, multi = provide_historic_frames_local(timedelta(days=1))
        self.assertNotIn("device_id", single.columns)
        self.assertEqual([DEVICES["b"]] * 2 + [DEVICES["c"]] * 2, list(multi["device_id"]))
        self.assertEqual([float(ord("b") + 1), float(ord("b") + 2), float(ord("c") + 1), float(ord("c") + 2)],
                         list(multi["temp"]))

    def test_the_window_ends_at_the_clock(self):
        provide_historic_frames_local(timedelta(days=1))
        self.assertEqual(3, len(self.db.queries))
        for query in self.db.queries:
            self.assertIn("2026-06-01T12:00:00.000000+00:00", query)

    def test_the_sql_reader_returns_frames_over_one_connection(self):
        single, multi = provide_historic_frames_local(timedelta(days=1), reader="sql")
        self.assertEqual(1, len(self.db.conns))
        self.assertEqual([float(ord("a") + 1), float(ord("a") + 2)], list(single["temp"]))
        self.assertEqual({DEVICES["b"], DEVICES["c"]}, set(multi["device_id"]))


class TestFramesFromAnExport(_Base):
    topics = [_import_topic()]
    config = {"ts_conn": DSN, "import_exports": IMPORT_EXPORTS}

    def test_an_import_with_an_export_is_read_from_the_export_table(self):
        (frame,) = provide_historic_frames_local(timedelta(days=1))
        self.assertIn(EXPORT_TABLE, self.db.queries[0])
        self.assertEqual([float(ord("x") + 1), float(ord("x") + 2)], list(frame["temp"]))
        self.kafka.assert_not_called()


class TestFramesFromTimescaleWrapper(_Base):
    topics = [_device_topic("a"), _import_topic()]
    config = {"ts_wrapper_url": "http://wrapper", "import_exports": IMPORT_EXPORTS}
    token = "tok"

    def test_without_a_dsn_both_inputs_come_from_the_wrapper(self):
        device_frame = pd.DataFrame({"time": [1], "temp": [1.0]})
        export_frame = pd.DataFrame({"time": [2], "temp": [2.0]})
        with mock.patch.object(data, "read_history", return_value=device_frame) as history, \
             mock.patch.object(data, "read_export_history", return_value=export_frame) as export_history:
            device_result, export_result = provide_historic_frames_local(timedelta(days=1))
        self.assertIs(device_frame, device_result)
        self.assertIs(export_frame, export_result)
        self.assertEqual(("http://wrapper", "tok"), history.call_args.args[:2])
        self.assertEqual((timedelta(days=1), False, E), history.call_args.args[3:])
        self.assertEqual(("http://wrapper", "tok"), export_history.call_args.args[:2])
        self.assertEqual([], self.db.conns)


class TestFramesOfAnInputOnlyKafkaHas(_Base):
    topics = [_device_topic("a"), _operator_topic()]

    def test_the_output_of_another_operator_raises_history_needs_ray_error(self):
        with self.assertRaises(HistoryNeedsRayError) as ctx:
            provide_historic_frames_local(timedelta(days=1))
        self.assertIn("analytics-upstream", str(ctx.exception))
        self.assertIn("provide_historic_data_local", str(ctx.exception))
        self.kafka.assert_not_called()

    def test_an_import_without_an_export_raises_too(self):
        opr = {"inputTopics": [_import_topic()], "config": {"ts_conn": DSN}}
        with mock.patch.object(util, "load_operator_config_json", return_value=opr):
            with self.assertRaises(HistoryNeedsRayError):
                provide_historic_frames_local(timedelta(days=1))
        self.kafka.assert_not_called()

    def test_always_prefer_kafka_raises_for_a_device_input(self):
        opr = {"inputTopics": [_device_topic("a")], "config": {"ts_conn": DSN}}
        with mock.patch.object(util, "load_operator_config_json", return_value=opr), \
             mock.patch.object(data, "ALWAYS_PREFER_KAFKA", True):
            with self.assertRaises(HistoryNeedsRayError):
                provide_historic_frames_local(timedelta(days=1))
        self.assertEqual([], self.db.conns)

    def test_the_datasets_still_read_kafka_for_it(self):
        kafka = mock.Mock(return_value="kafka dataset")
        with mock.patch.object(data, "get_kafka_dataset_local", kafka), \
             mock.patch.object(data, "get_timescale_dataset_local", return_value="timescale dataset"):
            result = data.provide_historic_data_local(timedelta(days=1))
        self.assertEqual(["timescale dataset", "kafka dataset"], result)


if __name__ == "__main__":
    unittest.main()
