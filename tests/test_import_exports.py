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
import types
import unittest
from datetime import timedelta
from unittest import mock

import operator_lib.util as util
from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.helpers import data
from operator_lib.util.helpers import exports
from operator_lib.util.helpers import timescale
from operator_lib.util.helpers import ts_wrapper
from operator_lib.util.model import Config, InputTopic

E = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

IMPORT_ID = "urn:infai:ses:import:a50aa583-282e-56c2-b101-7aaf68ebd2b9"
IMPORT_TOPIC = "urn_infai_ses_import_a50aa583-282e-56c2-b101-7aaf68ebd2b9"
TABLE = "userid:yk0RSePtTgueSTvakI3kNg_export:4xd1ukv1RqW6j4Gts5dxBA"
EXPORT_ID = "e31775ba-4bf5-46a5-ba8f-81adb3977104"
DEVICE_ID = "0362b1a1-a1a1-4b2b-8c3c-9d4d5e6f7a8b"
SERVICE_UUID = "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"


def _entry_dict(**overrides):
    entry = {
        "topic": IMPORT_TOPIC,
        "import_id": IMPORT_ID,
        "export_id": EXPORT_ID,
        "table": TABLE,
        "columns": {
            "value.forecasted_for": "forecasted_for",
            "value.instant_air_temperature": "instant_air_temperature",
        },
    }
    entry.update(overrides)
    return entry


def _import_topic():
    return InputTopic({
        "name": IMPORT_TOPIC,
        "filterType": "ImportId",
        "filterValue": IMPORT_ID,
        "mappings": [
            {"dest": "for", "source": "value.forecasted_for"},
            {"dest": "temp", "source": "value.instant_air_temperature"},
        ],
    })


def _device_topic():
    return InputTopic({
        "name": f"urn_infai_ses_service_{SERVICE_UUID}",
        "filterType": "DeviceId",
        "filterValue": DEVICE_ID,
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    })


def _config(entries=None, **fields):
    values = dict(fields)
    if entries is not None:
        values["import_exports"] = json.dumps(entries)
    return Config(values)


def _entry():
    return exports.parse_import_exports(json.dumps([_entry_dict()]))[0]


class TestParse(unittest.TestCase):
    def test_a_valid_value_parses(self):
        entries = exports.parse_import_exports(json.dumps([_entry_dict()]))
        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual(IMPORT_TOPIC, entry.topic)
        self.assertEqual(IMPORT_ID, entry.import_id)
        self.assertEqual(EXPORT_ID, entry.export_id)
        self.assertEqual(TABLE, entry.table)
        self.assertEqual("forecasted_for", entry.columns["value.forecasted_for"])

    def test_absent_or_blank_is_no_entries(self):
        self.assertEqual([], exports.parse_import_exports(None))
        self.assertEqual([], exports.parse_import_exports("  "))
        self.assertEqual([], exports.parse_import_exports("[]"))

    def test_the_config_field_is_read_from_the_operator_config(self):
        config = _config([_entry_dict()])
        self.assertIsNotNone(exports.find_import_export(config, _import_topic()))

    def test_invalid_json_is_an_error(self):
        with self.assertRaises(MissingConfigValueError) as ctx:
            exports.parse_import_exports("[{not json")
        self.assertIn("not valid JSON", str(ctx.exception))

    def test_a_non_list_is_an_error(self):
        with self.assertRaises(MissingConfigValueError):
            exports.parse_import_exports(json.dumps(_entry_dict()))

    def test_a_missing_field_is_an_error_naming_it(self):
        for name in ("topic", "import_id", "export_id", "table", "columns"):
            entry = _entry_dict()
            del entry[name]
            with self.subTest(missing=name):
                with self.assertRaises(MissingConfigValueError) as ctx:
                    exports.parse_import_exports(json.dumps([entry]))
                self.assertIn(name, str(ctx.exception))

    def test_columns_that_is_not_an_object_is_an_error(self):
        with self.assertRaises(MissingConfigValueError):
            exports.parse_import_exports(json.dumps([_entry_dict(columns=["a"])]))

    def test_a_mapping_source_without_a_column_is_an_error(self):
        entry = exports.parse_import_exports(json.dumps([_entry_dict(columns={"value.x": "x"})]))[0]
        with self.assertRaises(MissingConfigValueError) as ctx:
            exports.export_column_pairs(entry, _import_topic())
        self.assertIn("value.forecasted_for", str(ctx.exception))

    def test_lookup_is_by_topic_name_and_import_id(self):
        config = _config([_entry_dict()])
        other = InputTopic({
            "name": IMPORT_TOPIC, "filterType": "ImportId",
            "filterValue": "urn:infai:ses:import:other", "mappings": []})
        self.assertIsNone(exports.find_import_export(config, other))


class TestDispatch(unittest.TestCase):
    def setUp(self):
        data._logged_import_sources.clear()
        self.dep = types.SimpleNamespace(
            senergy_token=None, config_bootstrap_servers="kafka:9092", pipeline_id="p")
        self.read_topic = getattr(data, "__read_topic")
        patches = {
            "get_export_dataset_local": mock.patch.object(data, "get_export_dataset_local"),
            "get_kafka_dataset_local": mock.patch.object(data, "get_kafka_dataset_local"),
            "get_timescale_dataset_local": mock.patch.object(data, "get_timescale_dataset_local"),
            "get_ts_wrapper_export_dataset_local": mock.patch.object(
                data, "get_ts_wrapper_export_dataset_local"),
        }
        self.mocks = {}
        for name, patch in patches.items():
            self.mocks[name] = patch.start()
            self.addCleanup(patch.stop)

    def _read(self, config, topic):
        return self.read_topic(config, self.dep, topic, timedelta(days=1), False, False, E)

    def test_an_import_topic_with_an_entry_reads_the_export(self):
        config = _config([_entry_dict()], ts_conn="dsn")
        self._read(config, _import_topic())
        self.mocks["get_export_dataset_local"].assert_called_once()
        args = self.mocks["get_export_dataset_local"].call_args.args
        self.assertEqual("dsn", args[0])
        self.assertEqual(EXPORT_ID, args[2].export_id)
        self.mocks["get_kafka_dataset_local"].assert_not_called()

    def test_without_ts_conn_the_wrapper_is_used_with_the_token(self):
        self.dep.senergy_token = "tok"
        config = _config([_entry_dict()], ts_wrapper_url="http://wrapper")
        self._read(config, _import_topic())
        args = self.mocks["get_ts_wrapper_export_dataset_local"].call_args.args
        self.assertEqual(("http://wrapper", "tok"), args[:2])

    def test_an_entry_without_any_reader_raises(self):
        config = _config([_entry_dict()])
        with self.assertRaises(MissingConfigValueError) as ctx:
            self._read(config, _import_topic())
        self.assertIn(EXPORT_ID, str(ctx.exception))

    def test_an_import_topic_without_an_entry_reads_kafka(self):
        self._read(_config(), _import_topic())
        self.mocks["get_kafka_dataset_local"].assert_called_once()
        self.mocks["get_export_dataset_local"].assert_not_called()

    def test_an_entry_for_another_import_reads_kafka(self):
        config = _config([_entry_dict(import_id="urn:infai:ses:import:other")], ts_conn="dsn")
        self._read(config, _import_topic())
        self.mocks["get_kafka_dataset_local"].assert_called_once()

    def test_always_prefer_kafka_wins_over_an_entry(self):
        config = _config([_entry_dict()], ts_conn="dsn")
        with mock.patch.object(data, "ALWAYS_PREFER_KAFKA", True):
            self._read(config, _import_topic())
        self.mocks["get_kafka_dataset_local"].assert_called_once()
        self.mocks["get_export_dataset_local"].assert_not_called()

    def test_a_device_topic_is_unaffected_even_by_a_broken_value(self):
        config = Config({"ts_conn": "dsn", "import_exports": "{broken"})
        self._read(config, _device_topic())
        self.mocks["get_timescale_dataset_local"].assert_called_once()
        self.mocks["get_export_dataset_local"].assert_not_called()

    def test_a_broken_value_fails_when_an_import_topic_is_read(self):
        config = Config({"ts_conn": "dsn", "import_exports": "{broken"})
        with self.assertRaises(MissingConfigValueError):
            self._read(config, _import_topic())
        self.mocks["get_kafka_dataset_local"].assert_not_called()

    def test_the_source_is_logged_once_per_topic(self):
        config = _config([_entry_dict()], ts_conn="dsn")
        with mock.patch.object(data.logger, "info") as info:
            self._read(config, _import_topic())
            self._read(config, _import_topic())
        info.assert_called_once()
        self.assertIn(EXPORT_ID, info.call_args.args[0])
        self.assertIn(TABLE, info.call_args.args[0])

    def test_the_kafka_fallback_is_logged_with_its_reason(self):
        with mock.patch.object(data.logger, "info") as info:
            self._read(_config(), _import_topic())
        self.assertIn("no entry in import_exports", info.call_args.args[0])


class TestExportSql(unittest.TestCase):
    def setUp(self):
        self.query_fn = getattr(timescale, "__get_export_dataset_query")
        self.literal_fn = getattr(timescale, "__timestamptz_literal")

    def test_the_query_quotes_table_and_columns_and_is_distinct(self):
        query = self.query_fn("dsn", _import_topic(), _entry(), timedelta(hours=1), False, end=E)
        self.assertIn(f'FROM\n            "{TABLE}"', query)
        self.assertIn('"forecasted_for" AS "for"', query)
        self.assertIn('"instant_air_temperature" AS "temp"', query)
        self.assertIn("SELECT DISTINCT\n            time,", query)
        self.assertIn("ORDER BY time ASC", query)

    def test_a_fixed_end_renders_literal_bounds(self):
        query = self.query_fn("dsn", _import_topic(), _entry(), timedelta(hours=1), False, end=E)
        self.assertIn(
            f"time >= TIMESTAMPTZ '{self.literal_fn(E - timedelta(hours=1))}'", query)
        self.assertIn(f"time < TIMESTAMPTZ '{self.literal_fn(E)}'", query)
        self.assertNotIn("NOW()", query)

    def test_no_end_uses_a_relative_window(self):
        query = self.query_fn("dsn", _import_topic(), _entry(), timedelta(hours=1), False, end=None)
        self.assertIn("NOW() - INTERVAL '3600s'", query)

    def test_a_quote_in_a_name_is_escaped(self):
        entry = exports.parse_import_exports(json.dumps([_entry_dict(
            table='a"b', columns={
                "value.forecasted_for": 'x"y', "value.instant_air_temperature": "t"})]))[0]
        query = self.query_fn("dsn", _import_topic(), entry, timedelta(hours=1), False, end=E)
        self.assertIn('"a""b"', query)
        self.assertIn('"x""y" AS "for"', query)

    def test_a_mapping_without_a_column_raises(self):
        entry = exports.parse_import_exports(json.dumps([_entry_dict(columns={"value.x": "x"})]))[0]
        with self.assertRaises(MissingConfigValueError):
            self.query_fn("dsn", _import_topic(), entry, timedelta(hours=1), False, end=E)

    def _conn(self, first_time):
        cursor = mock.MagicMock()
        cursor.fetchone.return_value = None if first_time is None else (first_time,)
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        return conn

    def test_require_full_duration_with_a_fixed_end_raises_on_short_reach(self):
        conn = self._conn(E - timedelta(minutes=30))
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=conn):
            with self.assertRaises(ValueError):
                self.query_fn("dsn", _import_topic(), _entry(), timedelta(hours=1), True, end=E)

    def test_require_full_duration_with_a_fixed_end_raises_on_no_data(self):
        conn = self._conn(None)
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=conn):
            with self.assertRaises(ValueError):
                self.query_fn("dsn", _import_topic(), _entry(), timedelta(hours=1), True, end=E)

    def test_require_full_duration_with_a_fixed_end_and_full_reach_does_not_raise(self):
        conn = self._conn(E - timedelta(hours=2))
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=conn):
            self.query_fn("dsn", _import_topic(), _entry(), timedelta(hours=1), True, end=E)


def _forecast_payload():
    # One response element, one series, rows of [time, forecasted_for, temp]:
    # three forecast steps under one timestamp, plus an exact duplicate row. The
    # timestamp lies inside the [E - 1 day, E) window the tests read, because the
    # reader applies the window's lower edge itself.
    t = "2026-06-01T10:00:00.000Z"
    return [{"data": [[
        [t, "2026-06-01T11:00:00Z", 1.0],
        [t, "2026-06-01T12:00:00Z", 2.0],
        [t, "2026-06-01T13:00:00Z", 3.0],
        [t, "2026-06-01T12:00:00Z", 2.0],
    ]]}]


class TestExportWrapper(unittest.TestCase):
    def test_the_element_names_the_export_and_its_columns(self):
        with mock.patch.object(ts_wrapper, "_post", return_value=[]) as post:
            ts_wrapper.read_export_history(
                "http://wrapper", "tok", _import_topic(), _entry(), timedelta(days=1), end=E)
        _, _, elements = post.call_args.args
        element = elements[0]
        self.assertEqual(EXPORT_ID, element["exportId"])
        self.assertEqual(
            [{"name": "forecasted_for"}, {"name": "instant_air_temperature"}], element["columns"])
        self.assertEqual(0, element["orderColumnIndex"])
        self.assertEqual("asc", element["orderDirection"])
        self.assertEqual(ts_wrapper._format_time(E), element["time"]["end"])
        self.assertNotIn("deviceId", element)
        self.assertNotIn("serviceId", element)

    def test_the_decoder_keeps_every_row_of_a_shared_timestamp(self):
        payload = [{"data": [[
            ["2026-05-31T10:00:00.000Z", f"f{i}", float(i)] for i in range(48)
        ]]}]
        frame = ts_wrapper._decode_export(payload, _import_topic(), _entry())
        self.assertEqual(48, len(frame))
        self.assertEqual(1, frame["time"].nunique())
        self.assertEqual(48, frame["for"].nunique())
        self.assertEqual(["time", "for", "temp"], list(frame.columns))

    def test_read_history_drops_exact_duplicates_only(self):
        with mock.patch.object(ts_wrapper, "_post", return_value=_forecast_payload()):
            frame = ts_wrapper.read_export_history(
                "http://wrapper", "tok", _import_topic(), _entry(), timedelta(days=1), end=E)
        self.assertEqual(3, len(frame))
        self.assertEqual([1.0, 2.0, 3.0], list(frame["temp"]))
        self.assertEqual(["time", "for", "temp"], list(frame.columns))
        # UTC-naive, as the other readers return it.
        self.assertIsNone(frame["time"].dt.tz)

    def test_a_response_that_is_not_wide_raises(self):
        # One two-wide series per column: the device shape, not an export's.
        payload = [{"data": [
            [["2026-05-31T10:00:00.000Z", "a"]],
            [["2026-05-31T10:00:00.000Z", 1.0]],
        ]}]
        with self.assertRaises(ts_wrapper.TimescaleWrapperError) as ctx:
            ts_wrapper._decode_export(payload, _import_topic(), _entry())
        self.assertIn("width", str(ctx.exception))

    def test_an_empty_response_is_an_empty_frame_with_the_columns(self):
        frame = ts_wrapper._decode_export([], _import_topic(), _entry())
        self.assertTrue(frame.empty)
        self.assertEqual(["time", "for", "temp"], list(frame.columns))

    def test_require_full_duration_with_a_fixed_end_probes_the_export(self):
        payload = [{"data": [[["2026-05-31T11:30:00.000Z", "f", 1.0]]]}]
        with mock.patch.object(ts_wrapper, "_post", return_value=payload) as post:
            with self.assertRaises(ValueError):
                ts_wrapper.read_export_history(
                    "http://wrapper", "tok", _import_topic(), _entry(), timedelta(days=7),
                    require_full_duration=True, end=E)
        element = post.call_args.args[2][0]
        self.assertEqual(EXPORT_ID, element["exportId"])
        self.assertEqual(1, element["limit"])

    def test_the_device_decoder_is_unchanged(self):
        # The device path still folds rows by timestamp.
        payload = [{"data": [[["2026-05-31T10:00:00.000Z", 1.0], ["2026-05-31T10:00:00.000Z", 2.0]]]}]
        frame = ts_wrapper._decode(payload, _device_topic())
        self.assertEqual(1, len(frame))


def _strict_wrapper(rows, row_of):
    """
    A stand-in for timescale-wrapper's own filter, `"time" > start AND "time" <
    end`, over a fixed set of rows: what each chunk gets back is decided the way
    the service decides it, not by the test.
    """
    def post(_url, _token, elements):
        window = elements[0]["time"]
        start = datetime.datetime.fromisoformat(window["start"].replace("Z", "+00:00"))
        end = datetime.datetime.fromisoformat(window["end"].replace("Z", "+00:00"))
        hit = [row_of(at, i) for i, at in enumerate(rows) if start < at < end]
        return [{"data": [hit]}]
    return post


class TestChunkBoundaries(unittest.TestCase):
    # Rows on every day boundary of a 14-day window, read in 7-day chunks: the
    # start, the one chunk boundary, and the end (which the window excludes).
    ROWS = [E - timedelta(days=d) for d in range(14, -1, -1)]

    def _iso(self, at):
        return at.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def test_an_export_row_on_a_chunk_boundary_is_read_exactly_once(self):
        post = _strict_wrapper(self.ROWS, lambda at, i: [self._iso(at), f"f{i}", float(i)])
        with mock.patch.object(ts_wrapper, "_post", side_effect=post):
            frame = ts_wrapper.read_export_history(
                "http://wrapper", "tok", _import_topic(), _entry(), timedelta(days=14), end=E)
        # Every row in [E - 14 days, E): 14 of them, the one at E excluded.
        self.assertEqual(14, len(frame))
        self.assertEqual(list(range(14)), [int(v) for v in frame["temp"]])

    def test_a_device_row_on_a_chunk_boundary_is_read_exactly_once(self):
        post = _strict_wrapper(self.ROWS, lambda at, i: [self._iso(at), float(i)])
        with mock.patch.object(ts_wrapper, "_post", side_effect=post):
            frame = ts_wrapper.read_history(
                "http://wrapper", "tok", _device_topic(), timedelta(days=14), end=E)
        self.assertEqual(14, len(frame))
        self.assertEqual(list(range(14)), [int(v) for v in frame["temp"]])


class TestReadInputWindow(unittest.TestCase):
    def setUp(self):
        data._logged_import_sources.clear()

    def test_an_import_topic_with_an_entry_returns_the_export_rows(self):
        operator_config = {
            "config": {
                "ts_wrapper_url": "http://wrapper",
                "import_exports": json.dumps([_entry_dict()]),
            },
            "inputTopics": [{
                "name": IMPORT_TOPIC, "filterType": "ImportId", "filterValue": IMPORT_ID,
                "mappings": [
                    {"dest": "for", "source": "value.forecasted_for"},
                    {"dest": "temp", "source": "value.instant_air_temperature"},
                ],
            }],
        }
        dep = types.SimpleNamespace(
            config=json.dumps(operator_config), senergy_token="tok",
            config_bootstrap_servers="kafka:9092", pipeline_id="p")
        fake_ds = lambda frame: types.SimpleNamespace(to_pandas=lambda: frame)
        with mock.patch.object(util, "DeploymentConfig", return_value=dep), \
             mock.patch.object(ts_wrapper, "_post", return_value=_forecast_payload()), \
             mock.patch.object(ts_wrapper.ray.data, "from_pandas", side_effect=fake_ds), \
             mock.patch.object(data, "get_kafka_dataset_local") as kafka:
            result = data.read_input_window(E - timedelta(days=1), E)
        kafka.assert_not_called()
        self.assertEqual(1, len(result))
        topic, frame = result[0]
        self.assertEqual(IMPORT_TOPIC, topic.name)
        self.assertEqual(3, len(frame))
        self.assertEqual(["time", "for", "temp"], list(frame.columns))


if __name__ == "__main__":
    unittest.main()
