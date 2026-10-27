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
import re
import sys
import unittest
from datetime import timedelta
from unittest import mock

import psycopg2
import ray

import operator_lib.util as util
from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.helpers import data, timescale
from operator_lib.util.helpers import SqlOverride
from operator_lib.util.logger import logger
from operator_lib.util.model import InputTopic

E = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
SERVICE_UUID = "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
SERVICE_TOPIC = f"urn_infai_ses_service_{SERVICE_UUID}"
PASSWORD = "sec" + "ret"
DSN = "postgres://" + "u:" + PASSWORD + "@h:5432/db"

# device uuid -> uuid-like device ids; the fake database is keyed by the quoted table name
DEVICES = {d: f"{i:08x}-0000-4000-8000-000000000000" for i, d in enumerate("abcd", start=1)}
_get_table_name = getattr(timescale, "__get_table_name")


def _table(device):
    return _get_table_name(DEVICES[device], SERVICE_TOPIC.replace("_", ":"))


TABLES = {_table(d): d for d in DEVICES}
BAD_DEVICE = "d"  # the fake raises ProgrammingError for this table


def _topic_dict(name, filter_type, filter_value):
    return {
        "name": name,
        "filterType": filter_type,
        "filterValue": filter_value,
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    }


def _dev(*letters):
    return ",".join(DEVICES[x] for x in letters)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self.rows = []

    def execute(self, query):
        self.conn.queries.append(query)
        table = re.search(r'FROM\s+"([^"]+)"', query).group(1)
        device = TABLES[table]
        if device == BAD_DEVICE:
            raise psycopg2.ProgrammingError(f'relation "{table}" does not exist')
        if self.conn.fail_queries:
            self.conn.fail_queries -= 1
            raise psycopg2.OperationalError(f"server closed the connection ({DSN})")
        dests = list(dict.fromkeys(re.findall(r'AS "([^"]+)"', query)))
        self.description = [("time",)] + [(d,) for d in dests]
        base = datetime.datetime(2026, 5, 31, tzinfo=datetime.timezone.utc)
        # a bucket query returns its rows half an hour after the plain ones
        offset = 30 if "time_bucket" in query else 0
        self.rows = [(base + timedelta(hours=h, minutes=offset), float(ord(device) + h)) for h in (1, 2)]
        self.rows = [(t,) + (v,) * len(dests) for t, v in self.rows]

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, fail_queries=0):
        self.queries = []
        self.fail_queries = fail_queries
        self.closed = False
        self.autocommit = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakeDb:
    """connect() replacement: counts connections, can fail the first N connects or queries."""

    def __init__(self, connect_failures=0, query_failures=0):
        self.connect_failures = connect_failures
        self.query_failures = query_failures
        self.conns = []

    def __call__(self, conn_str):
        if self.connect_failures:
            self.connect_failures -= 1
            raise psycopg2.OperationalError(f"could not connect to {conn_str}")
        conn = FakeConn(self.query_failures)
        self.query_failures = 0
        self.conns.append(conn)
        return conn

    @property
    def queries(self):
        return [q for c in self.conns for q in c.queries]


def _fake_kafka(bootstrap, topic, *rest):
    import pandas as pd
    return ray.data.from_pandas(pd.DataFrame({"time": [9], "temp": [9.0]}))


class _Base(unittest.TestCase):
    topics = []

    def setUp(self):
        opr = {"inputTopics": self.topics, "config": {"ts_conn": DSN}}
        dep = mock.Mock(pipeline_id="p", config_bootstrap_servers="k", ts_conn=None, senergy_token=None)
        self.db = FakeDb()
        self.dataset_reader = mock.Mock()
        for patch in (
            mock.patch.object(util, "DeploymentConfig", return_value=dep),
            mock.patch.object(util, "load_operator_config_json", return_value=opr),
            mock.patch.object(data.clock, "fixed", return_value=E),
            mock.patch.object(timescale, "__create_timescale_connection", self.db),
            mock.patch.object(data, "get_timescale_dataset_local", self.dataset_reader),
            mock.patch.object(data, "get_kafka_dataset_local", mock.Mock(side_effect=_fake_kafka)),
            mock.patch.object(timescale, "time"),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.sleep = timescale.time.sleep  # timescale.time is a Mock here, the module-wide sleep is untouched


class TestSqlLocal(_Base):
    topics = [
        _topic_dict(SERVICE_TOPIC, "DeviceId", _dev("a", "b")),
        _topic_dict("analytics-upstream", "OperatorId", "op:pipe"),
        _topic_dict(SERVICE_TOPIC, "DeviceId", _dev("c")),
    ]

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_inputs_come_back_in_config_order_over_one_connection(self):
        multi, other, single = data.provide_historic_data_local(timedelta(days=1), reader="sql")
        self.assertEqual(1, len(self.db.conns))
        self.assertTrue(self.db.conns[0].closed)
        self.assertEqual(3, len(self.db.queries))
        self.dataset_reader.assert_not_called()
        multi = multi.to_pandas()
        self.assertEqual({"a", "b"}, set(multi["device_id"].map({v: k for k, v in DEVICES.items()})))
        self.assertEqual(4, len(multi))
        self.assertEqual([9.0], list(other.to_pandas()["temp"]))
        single = single.to_pandas()
        self.assertNotIn("device_id", single.columns)
        self.assertEqual([float(ord("c") + 1), float(ord("c") + 2)], list(single["temp"]))

    def test_the_device_id_is_the_device_of_the_rows(self):
        multi = data.provide_historic_data_local(timedelta(days=1), reader="sql")[0].to_pandas()
        for letter in "ab":
            rows = multi[multi["device_id"] == DEVICES[letter]]
            self.assertEqual([float(ord(letter) + 1), float(ord(letter) + 2)], list(rows["temp"]))

    def test_time_is_utc_naive(self):
        single = data.provide_historic_data_local(timedelta(days=1), reader="sql")[2].to_pandas()
        self.assertIsNone(single["time"].dt.tz)

    def test_the_window_reaches_the_query_and_changes_only_switches_the_query(self):
        data.provide_historic_data_local(timedelta(days=1), reader="sql")
        self.assertIn("2026-06-01T12:00:00.000000+00:00", self.db.queries[0])
        self.assertNotIn("IS DISTINCT FROM", self.db.queries[0])
        self.db.conns.clear()
        data.provide_historic_data_local(timedelta(days=1), reader="sql", changes_only=True)
        self.assertEqual(3, len(self.db.queries))
        for q in self.db.queries:
            self.assertIn("IS DISTINCT FROM", q)
            self.assertIn("2026-06-01T12:00:00.000000+00:00", q)

    def test_the_default_reader_is_untouched(self):
        self.dataset_reader.side_effect = lambda *a: _fake_kafka(None, None)
        data.provide_historic_data_local(timedelta(days=1))
        self.assertEqual(3, self.dataset_reader.call_count)
        self.assertEqual([], self.db.conns)

    def test_without_a_dsn_the_input_goes_the_ordinary_way(self):
        dep = mock.Mock(pipeline_id="p", config_bootstrap_servers="k", ts_conn=None, senergy_token=None)
        opr = {"inputTopics": self.topics, "config": {}}
        with mock.patch.object(util, "load_operator_config_json", return_value=opr):
            with self.assertRaises(MissingConfigValueError):
                data.provide_historic_data_local(timedelta(days=1), reader="sql")
        self.assertEqual([], self.db.conns)


def _two_input_topic(filter_value):
    return {
        "name": SERVICE_TOPIC,
        "filterType": "DeviceId",
        "filterValue": filter_value,
        "mappings": [{"dest": "temp", "source": "value.temp"}, {"dest": "power", "source": "value.power"}],
    }


class TestOverrides(_Base):
    topics = [_two_input_topic(_dev("a")), _two_input_topic(_dev("b", "c"))]
    bucket = SqlOverride(duration=timedelta(days=90), bucket=timedelta(seconds=60))

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def _read(self, overrides, changes_only=False):
        return data.provide_historic_data_local(timedelta(days=1), reader="sql", changes_only=changes_only, overrides=overrides)

    def test_a_topic_without_a_matching_override_is_one_plain_query(self):
        for overrides in (None, {}, {"temp": SqlOverride()}):
            with self.subTest(overrides=overrides):
                self.db.conns.clear()
                self._read(overrides)
                self.assertEqual(3, len(self.db.queries))
                self.assertTrue(all("time_bucket" not in q for q in self.db.queries))

    def test_a_mixed_topic_is_read_per_group_and_put_together_sorted_by_time(self):
        single, multi = self._read({"power": self.bucket})
        # a: 2 groups, b and c: 2 groups each
        self.assertEqual(6, len(self.db.queries))
        frame = single.to_pandas()
        self.assertEqual(["time", "temp", "power"], list(frame.columns))
        self.assertTrue(frame["time"].is_monotonic_increasing)
        self.assertEqual(4, len(frame))
        plain, bucketed = frame[frame["temp"].notna()], frame[frame["power"].notna()]
        self.assertEqual(2, len(plain))
        self.assertTrue(plain["power"].isna().all())
        self.assertEqual(2, len(bucketed))
        self.assertTrue(bucketed["temp"].isna().all())
        self.assertEqual({30}, set(bucketed["time"].dt.minute))  # the bucket rows are the second group's
        self.assertEqual({0}, set(plain["time"].dt.minute))

    def test_a_mixed_multi_device_input_is_sorted_per_device(self):
        multi = self._read({"power": self.bucket})[1].to_pandas()
        self.assertEqual({DEVICES["b"], DEVICES["c"]}, set(multi["device_id"]))
        for device, rows in multi.groupby("device_id", sort=False):
            self.assertEqual(4, len(rows))
            self.assertTrue(rows["time"].is_monotonic_increasing, device)
        # the devices stay together, in config order, as without overrides
        self.assertEqual([DEVICES["b"]] * 4 + [DEVICES["c"]] * 4, list(multi["device_id"]))

    def test_mappings_with_the_same_override_share_one_query(self):
        self._read({"temp": self.bucket, "power": SqlOverride(duration=timedelta(days=90), bucket=timedelta(seconds=60))})
        self.assertEqual(3, len(self.db.queries))
        for q in self.db.queries:
            self.assertIn("time_bucket(INTERVAL '60 seconds', time)", q)
            self.assertEqual(2, q.count("max("))

    def test_the_window_is_per_group(self):
        self._read({"power": SqlOverride(duration=timedelta(days=90))})
        a_queries = [q for q in self.db.queries if f'"{_table("a")}"' in q]
        self.assertEqual(2, len(a_queries))
        plain = next(q for q in a_queries if '"temp"' in q)
        long = next(q for q in a_queries if '"power"' in q)
        self.assertIn("TIMESTAMPTZ '2026-05-31T12:00:00.000000+00:00'", plain)
        self.assertIn("TIMESTAMPTZ '2026-03-03T12:00:00.000000+00:00'", long)
        self.assertNotIn("time_bucket", long)  # a duration alone does not bucket
        self.assertIn("TIMESTAMPTZ '2026-06-01T12:00:00.000000+00:00'", long)  # same end

    def test_bucket_query_takes_the_maximum_per_bucket(self):
        self._read({"power": SqlOverride(bucket=timedelta(minutes=1))})
        q = next(q for q in self.db.queries if "time_bucket" in q)
        self.assertIn('time_bucket(INTERVAL \'60 seconds\', time) AS time', q)
        self.assertIn('max("power") AS "power"', q)
        self.assertNotIn('"temp"', q)
        self.assertIn("GROUP BY 1", q)
        self.assertNotIn("IS DISTINCT FROM", q)

    def test_changes_only_works_on_the_bucket_rows(self):
        self._read({"power": self.bucket}, changes_only=True)
        q = next(q for q in self.db.queries if "time_bucket" in q)
        self.assertRegex(q, r"WITH base AS \(\s+SELECT\s+time_bucket\(INTERVAL '60 seconds', time\) AS time,")
        self.assertIn("GROUP BY 1\n        ), neighbours AS (", q)
        self.assertIn('"power" IS DISTINCT FROM __prev_0', q)
        plain = next(q for q in self.db.queries if "time_bucket" not in q)
        self.assertIn("IS DISTINCT FROM", plain)

    def test_an_override_for_an_input_not_read_over_the_dsn_is_logged(self):
        topics = [_two_input_topic(_dev("a")), _topic_dict("analytics-upstream", "OperatorId", "op:pipe")]
        topics[1]["mappings"] = [{"dest": "other", "source": "value.other"}]
        opr = {"inputTopics": topics, "config": {"ts_conn": DSN}}
        with mock.patch.object(util, "load_operator_config_json", return_value=opr):
            with self.assertLogs(logger, level="WARNING") as logs:
                data.provide_historic_data_local(timedelta(days=1), reader="sql", overrides={"other": self.bucket})
        self.assertTrue(any("'other'" in line and "ignored" in line for line in logs.output))

    def test_parameter_errors(self):
        for f in (data.provide_historic_data, data.provide_historic_data_local):
            with self.subTest(f=f.__name__):
                with self.assertRaisesRegex(ValueError, "overrides need"):
                    f(timedelta(days=1), overrides={"power": self.bucket})
                with self.assertRaisesRegex(ValueError, "overrides need"):
                    f(timedelta(days=1), reader="dataset", overrides={})
                with self.assertRaisesRegex(ValueError, "SqlOverride"):
                    f(timedelta(days=1), reader="sql", overrides={"power": {"bucket": 60}})
                with self.assertRaisesRegex(ValueError, "SqlOverride"):
                    f(timedelta(days=1), reader="sql", overrides=[("power", self.bucket)])
        with self.assertRaisesRegex(ValueError, "no input: 'powr'"):
            self._read({"powr": self.bucket})
        self.assertEqual([], self.db.conns)

    def test_override_values_are_checked(self):
        for kwargs in ({"bucket": timedelta(0)}, {"bucket": timedelta(seconds=-5)}, {"bucket": timedelta(seconds=1.5)},
                       {"bucket": timedelta(milliseconds=500)}, {"bucket": 60}, {"bucket": "60 seconds"},
                       {"duration": timedelta(0)}, {"duration": -timedelta(days=1)}, {"duration": 5}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    SqlOverride(**kwargs)
        self.assertEqual(timedelta(minutes=2), SqlOverride(bucket=timedelta(minutes=2)).bucket)

    def test_the_bucket_literal_is_built_from_the_number_of_seconds_only(self):
        build = getattr(timescale, "__get_sql_reader_query")
        topic = InputTopic(_two_input_topic(_dev("a")))
        query = build(topic, timedelta(days=1), E, False, timedelta(hours=1, seconds=30))
        self.assertIn("INTERVAL '3630 seconds'", query)
        with self.assertRaises(ValueError):
            build(topic, timedelta(days=1), E, False, timedelta(milliseconds=1500))


class TestParameterChecks(_Base):
    topics = [_topic_dict(SERVICE_TOPIC, "DeviceId", _dev("a"))]

    def test_value_errors(self):
        for f in (data.provide_historic_data, data.provide_historic_data_local):
            with self.subTest(f=f.__name__):
                with self.assertRaisesRegex(ValueError, "unknown reader"):
                    f(timedelta(days=1), reader="pandas")
                with self.assertRaisesRegex(ValueError, "changes_only"):
                    f(timedelta(days=1), changes_only=True)
                with self.assertRaisesRegex(ValueError, "changes_only"):
                    f(timedelta(days=1), reader="dataset", changes_only=True)
                with self.assertRaisesRegex(ValueError, "require_full_duration"):
                    f(timedelta(days=1), require_full_duration=True, reader="sql")
        self.assertEqual([], self.db.conns)


class TestRetry(_Base):
    topics = [_topic_dict(SERVICE_TOPIC, "DeviceId", _dev("a", "b"))]

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_a_failed_connect_is_retried_after_a_pause(self):
        self.db.connect_failures = 2
        (ds,) = data.provide_historic_data_local(timedelta(days=1), reader="sql")
        self.assertEqual(4, len(ds.to_pandas()))
        self.assertEqual(2, self.sleep.call_count)
        self.sleep.assert_called_with(timescale.SQL_RETRY_PAUSE_S)

    def test_a_dropped_query_reconnects_and_reads_the_table_again(self):
        self.db.query_failures = 1
        (ds,) = data.provide_historic_data_local(timedelta(days=1), reader="sql")
        self.assertEqual(4, len(ds.to_pandas()))
        self.assertEqual(2, len(self.db.conns))
        self.assertTrue(self.db.conns[0].closed)
        self.assertEqual(1, self.sleep.call_count)

    def test_three_failures_end_the_input_without_the_dsn(self):
        self.db.connect_failures = 3
        with self.assertLogs(logger, level="DEBUG") as logs:
            with self.assertRaises(psycopg2.OperationalError) as ctx:
                data.provide_historic_data_local(timedelta(days=1), reader="sql")
        self.assertIn("3 attempts", str(ctx.exception))
        self.assertEqual(2, self.sleep.call_count)
        for text in logs.output + [str(ctx.exception), repr(ctx.exception.__cause__), repr(ctx.exception.__context__)]:
            self.assertNotIn(DSN, text)
            self.assertNotIn(PASSWORD, text)

    def test_another_error_is_not_retried(self):
        topics = [_topic_dict(SERVICE_TOPIC, "DeviceId", _dev("d"))]
        opr = {"inputTopics": topics, "config": {"ts_conn": DSN}}
        with mock.patch.object(util, "load_operator_config_json", return_value=opr):
            with self.assertRaises(psycopg2.ProgrammingError):
                data.provide_historic_data_local(timedelta(days=1), reader="sql")
        self.assertEqual(0, self.sleep.call_count)


# The remote task runs in a worker: the database there cannot be patched, so the
# fake connect is handed to the task, and the module is shipped by value.
class _WithFakeConnect:
    def __init__(self, connect, **options):
        self.connect, self.options_ = connect, options

    def options(self, **options):
        return _WithFakeConnect(self.connect, **options)

    def remote(self, *args):
        real = timescale.get_timescale_sql_datasets_remote
        return real.options(**self.options_).remote(*args, connect=self.connect)


def _worker_connect(conn_str):
    return FakeDb()(conn_str)


class TestSqlRemote(_Base):
    topics = [
        _topic_dict(SERVICE_TOPIC, "DeviceId", _dev("a", "b")),
        _topic_dict(SERVICE_TOPIC, "DeviceId", _dev("c")),
    ]

    @classmethod
    def setUpClass(cls):
        ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def setUp(self):
        super().setUp()
        patch = mock.patch.object(data, "get_timescale_sql_datasets_remote", _WithFakeConnect(_worker_connect))
        patch.start()
        self.addCleanup(patch.stop)

    def _use(self, topics):
        opr = {"inputTopics": topics, "config": {"ts_conn": DSN}}
        patch = mock.patch.object(util, "load_operator_config_json", return_value=opr)
        patch.start()
        self.addCleanup(patch.stop)

    def test_one_object_ref_per_input_in_config_order(self):
        result = data.provide_historic_data(timedelta(days=1), reader="sql")
        self.assertEqual(2, len(result))
        self.assertTrue(all(isinstance(r, ray.ObjectRef) for r in result))
        multi, single = [ray.get(r).to_pandas() for r in result]
        self.assertEqual(4, len(multi))
        self.assertEqual({DEVICES["a"], DEVICES["b"]}, set(multi["device_id"]))
        self.assertNotIn("device_id", single.columns)

    def test_a_single_input_is_still_a_list_of_one_ref(self):
        self._use(self.topics[1:])
        result = data.provide_historic_data(timedelta(days=1), reader="sql", changes_only=True)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result))
        self.assertEqual(2, len(ray.get(result[0]).to_pandas()))

    def test_mixed_inputs_keep_their_position(self):
        self._use([self.topics[0], _topic_dict("analytics-upstream", "OperatorId", "op:pipe"), self.topics[1]])
        with mock.patch.object(data, "get_kafka_dataset_remote", ray.remote(_fake_kafka)):
            result = data.provide_historic_data(timedelta(days=1), reader="sql")
        frames = [ray.get(r).to_pandas() for r in result]
        self.assertEqual([4, 1, 2], [len(f) for f in frames])

    def test_overrides_travel_to_the_task(self):
        self._use([_two_input_topic(_dev("a")), _two_input_topic(_dev("b"))])
        result = data.provide_historic_data(timedelta(days=1), reader="sql", changes_only=True,
                                            overrides={"power": SqlOverride(duration=timedelta(days=90), bucket=timedelta(minutes=1))})
        frames = [ray.get(r).to_pandas() for r in result]
        for frame in frames:
            self.assertEqual(["time", "temp", "power"], list(frame.columns))
            self.assertEqual(4, len(frame))
            self.assertTrue(frame["time"].is_monotonic_increasing)

    def test_a_failing_input_takes_itself_and_the_later_ones_not_the_earlier_ones(self):
        self._use([self.topics[0], _topic_dict(SERVICE_TOPIC, "DeviceId", _dev("d")), self.topics[1]])
        first, failing, later = data.provide_historic_data(timedelta(days=1), reader="sql")
        self.assertEqual(4, len(ray.get(first).to_pandas()))
        for ref in (failing, later):
            with self.assertRaises(Exception):
                ray.get(ref)


if __name__ == "__main__":
    unittest.main()
