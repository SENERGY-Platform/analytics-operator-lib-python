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
import inspect
import json
import types
import unittest
from datetime import timedelta
from unittest import mock

from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.declaration import DeclarationError, ImportFilter
from operator_lib.util.helpers import data, import_filter, kafka, timescale, ts_wrapper
from operator_lib.util.helpers.import_filter import ValueFilter
from operator_lib.util.model import Config, InputTopic
from operator_lib.util.op_base import OperatorBase

E = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

SOLAR_IMPORT = "urn:infai:ses:import:solar"
SOLAR_TOPIC = "urn_infai_ses_import_solar"
MOSMIX_IMPORT = "urn:infai:ses:import:mosmix"
MOSMIX_TOPIC = "urn_infai_ses_import_mosmix"
DEVICE_TOPIC = "urn_infai_ses_service_1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"

PATHS = ("value.meta.id", "value.meta.lat", "value.meta.long")
COLUMNS = ("lat", "lon")


class Cfg(Config):
    measure_station = ""
    forecast_station = ""
    lat = ""
    long = ""


def _topic(name, filter_type, filter_value, **mappings):
    return InputTopic({
        "name": name,
        "filterType": filter_type,
        "filterValue": filter_value,
        "mappings": [{"dest": dest, "source": source} for dest, source in mappings.items()],
    })


def _solar():
    return _topic(SOLAR_TOPIC, "ImportId", SOLAR_IMPORT, outdoor_temperature="value.temp")


def _mosmix():
    return _topic(MOSMIX_TOPIC, "ImportId", MOSMIX_IMPORT,
                  forecast_temperature="value.temp", forecasted_for="value.for")


def _device():
    return _topic(DEVICE_TOPIC, "DeviceId", "d1", t="value.t")


def _export(topic, export_id, **columns):
    # Only the columns of mapped inputs, as the deployer writes them: the id and the
    # location are not among them.
    base = {"value.temp": "temp"} if topic == SOLAR_TOPIC else {"value.temp": "temp", "value.for": "forecasted_for"}
    base.update(columns)
    return {"topic": topic, "import_id": SOLAR_IMPORT if topic == SOLAR_TOPIC else MOSMIX_IMPORT,
            "export_id": export_id, "table": f"table_{export_id}", "columns": base}


def _spec(**kwargs):
    values = dict(path="value.meta.id", column="station", value="measure_station",
                  near=("lat", "long"), near_paths=PATHS[1:], near_columns=COLUMNS)
    values.update(kwargs)
    return ImportFilter(**values)


def _config(exports=None, **fields):
    if exports is not None:
        fields["import_exports"] = json.dumps(exports)
    return Cfg(fields)


DEP = types.SimpleNamespace(ts_conn=None, senergy_token=None, config_bootstrap_servers="kafka:9092")


def _recent(messages):
    """A read_recent_messages that applies the caller's extraction to raw messages, as the real one does."""
    def read(bootstrap, topic, count, extract):
        return list({row: None for row in map(extract, messages) if row is not None})
    return read


def _resolve(declared, config, topics, training=()):
    import_filter.resolve_import_filters(declared, config, DEP, list(topics), list(training))


class Base(unittest.TestCase):
    def setUp(self):
        import_filter.clear()
        self.addCleanup(import_filter.clear)
        # The columns of the export's table are checked against the database; no database here.
        patch = mock.patch.object(import_filter, "_check_columns")
        self.check_columns = patch.start()
        self.addCleanup(patch.stop)


class _Message:
    def __init__(self, topic, body):
        self._topic = topic
        self._body = body

    def error(self):
        return None

    def timestamp(self):
        return (1, 1700000000000)

    def value(self):
        return json.dumps(self._body).encode()

    def topic(self):
        return self._topic


class _Result:
    ex = None
    filter_ids = ["f"]

    def __init__(self, data):
        self.data = data


class _Operator(OperatorBase):
    def __init__(self):
        self.seen = []

    def run(self, data, selector, device_id, timestamp):
        self.seen.append(data)
        return None


def _route(operator, message):
    handler = mock.Mock()
    handler.get_results.return_value = [_Result({"v": 1})]
    handler.get_filter_args.return_value = {"selector": None}
    consumer = mock.Mock()
    consumer.poll.return_value = message
    producer = mock.MagicMock()
    for name, value in (("filter_handler", handler), ("kafka_consumer", consumer),
                        ("kafka_producer", producer), ("poll_timeout", 0.1),
                        ("output_topic", "out"), ("pipeline_id", "p"), ("operator_id", "o")):
        setattr(operator, f"_OperatorBase__{name}", value)
    operator._OperatorBase__route()


def _import_message(topic, import_id, station, **extra):
    meta = {} if station is None else {"id": station}
    return _Message(topic, {"import_id": import_id, "value": {"meta": meta, "temp": 1}, **extra})


class TestLive(Base):
    def setUp(self):
        super().setUp()
        self.operator = _Operator()
        _resolve([_spec(near=(), near_paths=(), near_columns=())],
                 _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932"), [_solar(), _device()])

    def test_only_the_resolved_station_reaches_run(self):
        _route(self.operator, _import_message(SOLAR_TOPIC, SOLAR_IMPORT, "02932"))
        _route(self.operator, _import_message(SOLAR_TOPIC, SOLAR_IMPORT, "10469"))
        self.assertEqual(1, len(self.operator.seen))

    def test_a_device_topic_is_not_touched(self):
        _route(self.operator, _Message(DEVICE_TOPIC, {"device_id": "d1", "value": {"t": 1}}))
        self.assertEqual(1, len(self.operator.seen))

    def test_a_message_without_the_path_is_dropped(self):
        _route(self.operator, _import_message(SOLAR_TOPIC, SOLAR_IMPORT, None))
        self.assertEqual([], self.operator.seen)

    def test_a_number_matches_the_same_text(self):
        import_filter.clear()
        _resolve([_spec(near=(), near_paths=(), near_columns=())],
                 _config([_export(SOLAR_TOPIC, "e1")], measure_station="2932"), [_solar()])
        _route(self.operator, _import_message(SOLAR_TOPIC, SOLAR_IMPORT, 2932))
        _route(self.operator, _import_message(SOLAR_TOPIC, SOLAR_IMPORT, "2932"))
        self.assertEqual(2, len(self.operator.seen))

    def test_an_import_without_a_filter_is_not_touched(self):
        _route(self.operator, _import_message(MOSMIX_TOPIC, MOSMIX_IMPORT, "anything"))
        self.assertEqual(1, len(self.operator.seen))


class TestResolution(Base):
    SPEC = _spec()

    def _places(self, rows):
        return mock.patch.object(import_filter, "_read_places", return_value=rows)

    def test_an_explicit_value_wins_without_listing_places(self):
        with self._places([]) as places:
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932", lat="51.3", long="12.4"),
                     [_solar()])
        places.assert_not_called()
        self.assertEqual(ValueFilter("value.meta.id", "02932", "station"), import_filter.filter_for(_solar()))

    def test_the_nearest_place_of_the_export_is_chosen(self):
        rows = [("00001", 48.1, 11.6), ("02932", 51.32, 12.43), ("00003", 53.5, 10.0)]
        with self._places(rows):
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], lat="51.34", long="12.37"), [_solar()])
        self.assertEqual("02932", import_filter.filter_for(_solar()).value)

    def test_the_chosen_value_and_distance_are_logged(self):
        with self._places([("02932", 51.32, 12.43)]), mock.patch.object(import_filter.logger, "info") as info:
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], lat="51.34", long="12.37"), [_solar()])
        text = " ".join(str(c.args[0]) for c in info.call_args_list)
        self.assertIn("'02932'", text)
        self.assertIn("km", text)

    def test_nothing_configured_means_no_filter_and_a_warning(self):
        with mock.patch.object(import_filter.logger, "warning") as warning:
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")]), [_solar()])
        self.assertIsNone(import_filter.filter_for(_solar()))
        warning.assert_called_once()

    def _kafka(self, messages):
        return mock.patch.object(kafka, "read_recent_messages", side_effect=_recent(messages))

    @staticmethod
    def _message(station, lat, lon, import_id=SOLAR_IMPORT):
        return {"import_id": import_id, "value": {"meta": {"id": station, "lat": lat, "long": lon}}}

    def test_the_nearest_without_an_export_is_taken_from_the_recent_messages(self):
        messages = [self._message("00001", 48.1, 11.6), self._message("02932", 51.32, 12.43),
                    self._message("99999", 51.34, 12.37, import_id="urn:infai:ses:import:other")]
        with self._kafka(messages) as read:
            _resolve([_spec(places_scan_messages=500)], _config([], lat="51.34", long="12.37"), [_solar()])
        self.assertEqual("02932", import_filter.filter_for(_solar()).value)
        self.assertEqual(("kafka:9092", SOLAR_TOPIC, 500), read.call_args.args[:3])
        self.check_columns.assert_not_called()

    def test_no_message_with_the_paths_stops_the_start(self):
        with self._kafka([{"import_id": SOLAR_IMPORT, "value": {"temp": 1}}]):
            with self.assertRaisesRegex(MissingConfigValueError, "value.meta.id.*nothing to choose"):
                _resolve([_spec()], _config([], lat="51.34", long="12.37"), [_solar()])

    def test_an_explicit_value_without_an_export_entry_filters_without_reading_anything(self):
        with self._kafka([]) as read:
            _resolve([self.SPEC], _config([], measure_station="02932"), [_solar()])
        read.assert_not_called()
        self.check_columns.assert_not_called()
        self.assertEqual(ValueFilter("value.meta.id", "02932", "station"), import_filter.filter_for(_solar()))

    def test_an_export_entry_with_a_missing_column_still_stops_the_start(self):
        self.check_columns.side_effect = MissingConfigValueError("export e1 has no column 'station'")
        with self.assertRaisesRegex(MissingConfigValueError, "e1.*station"):
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932"), [_solar()])

    def test_an_export_entry_keeps_the_places_out_of_kafka(self):
        with self._places([("02932", 51.32, 12.43)]), self._kafka([]) as read:
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], lat="51.34", long="12.37"), [_solar()])
        read.assert_not_called()

    def test_the_declared_columns_are_checked_against_the_export(self):
        with self._places([("02932", 51.32, 12.43)]):
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], lat="51.34", long="12.37"), [_solar()])
        self.assertEqual(["station", "lat", "lon"], self.check_columns.call_args.args[3])

    def test_an_explicit_value_checks_only_its_own_column(self):
        _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932"), [_solar()])
        self.assertEqual(["station"], self.check_columns.call_args.args[3])

    def test_a_missing_export_column_stops_the_start(self):
        self.check_columns.side_effect = MissingConfigValueError("export e1 has no column 'lat'")
        with self.assertRaisesRegex(MissingConfigValueError, "e1.*lat"):
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], lat="51.34", long="12.37"), [_solar()])

    def test_nothing_set_needs_neither_export_nor_columns(self):
        with mock.patch.object(import_filter.logger, "warning"):
            _resolve([self.SPEC], _config([]), [_solar()])
        self.check_columns.assert_not_called()
        self.assertIsNone(import_filter.filter_for(_solar()))

    def test_a_misspelt_config_field_is_a_declaration_error(self):
        with self.assertRaisesRegex(DeclarationError, "measure_statoin"):
            _resolve([_spec(value="measure_statoin")], _config([_export(SOLAR_TOPIC, "e1")]), [_solar()])
        with self.assertRaisesRegex(DeclarationError, "latt"):
            _resolve([_spec(near=("latt", "long"))], _config([_export(SOLAR_TOPIC, "e1")]), [_solar()])

    def test_half_a_location_fails(self):
        with self.assertRaisesRegex(MissingConfigValueError, "together"):
            _resolve([self.SPEC], _config([_export(SOLAR_TOPIC, "e1")], lat="51.34"), [_solar()])

    def test_a_filter_naming_no_import_input_fails(self):
        spec = _spec(inputs=["t"])
        with self.assertRaisesRegex(MissingConfigValueError, "none of which is an import input"):
            _resolve([spec], _config(measure_station="1"), [_solar(), _device()])

    def test_a_declaration_needs_something_to_resolve(self):
        with self.assertRaises(DeclarationError):
            ImportFilter(path="value.meta.id", column="station")
        with self.assertRaises(DeclarationError):
            ImportFilter(path="value.meta.id", column="station", near=("lat", "long"), near_paths=PATHS[1:])
        with self.assertRaises(DeclarationError):
            ImportFilter(path="value.meta.id", column="", value="v")


class TestTwoFilters(Base):
    """A measurement from one import and a forecast from another, each with its own station."""

    def setUp(self):
        super().setUp()
        self.exports = [_export(SOLAR_TOPIC, "solar"), _export(MOSMIX_TOPIC, "mosmix")]
        self.declared = [
            _spec(inputs=["outdoor_temperature"]),
            _spec(inputs=["forecast_temperature", "forecasted_for"], value="forecast_station"),
        ]
        places = {
            "solar": [("02932", 51.32, 12.43), ("00001", 48.1, 11.6)],
            "mosmix": [("10469", 51.3, 12.4), ("P0001", 48.0, 11.5)],
        }
        patch = mock.patch.object(
            import_filter, "_read_places", side_effect=lambda cfg, dep, entry, columns: places[entry.export_id])
        patch.start()
        self.addCleanup(patch.stop)

    def _resolve(self, **fields):
        _resolve(self.declared, _config(self.exports, **fields), [_solar(), _mosmix(), _device()])

    def test_each_filter_picks_the_nearest_of_its_own_export(self):
        self._resolve(lat="51.34", long="12.37")
        self.assertEqual("02932", import_filter.filter_for(_solar()).value)
        self.assertEqual("10469", import_filter.filter_for(_mosmix()).value)
        self.assertIsNone(import_filter.filter_for(_device()))

    def test_an_explicit_value_of_one_filter_leaves_the_other_to_its_location(self):
        self._resolve(forecast_station="10469X", lat="51.34", long="12.37")
        self.assertEqual("02932", import_filter.filter_for(_solar()).value)
        self.assertEqual("10469X", import_filter.filter_for(_mosmix()).value)

    def test_live_each_import_keeps_its_own_station(self):
        self._resolve(lat="51.34", long="12.37")
        operator = _Operator()
        _route(operator, _import_message(SOLAR_TOPIC, SOLAR_IMPORT, "02932"))
        _route(operator, _import_message(SOLAR_TOPIC, SOLAR_IMPORT, "10469"))
        _route(operator, _import_message(MOSMIX_TOPIC, MOSMIX_IMPORT, "10469"))
        _route(operator, _import_message(MOSMIX_TOPIC, MOSMIX_IMPORT, "02932"))
        self.assertEqual(2, len(operator.seen))

    def test_history_each_import_is_read_with_its_own_filter(self):
        self._resolve(lat="51.34", long="12.37")
        dep = types.SimpleNamespace(ts_conn=None, senergy_token=None, config_bootstrap_servers="k", pipeline_id="p")
        config = _config(self.exports, ts_conn="dsn")
        read_topic = getattr(data, "__read_topic")
        with mock.patch.object(data, "get_export_dataset_local") as read:
            read_topic(config, dep, _solar(), timedelta(days=1), False, False, E)
            read_topic(config, dep, _mosmix(), timedelta(days=1), False, False, E)
        self.assertEqual(
            [ValueFilter("value.meta.id", "02932", "station"), ValueFilter("value.meta.id", "10469", "station")],
            [call.args[-1] for call in read.call_args_list])

    def test_one_import_with_two_filters_is_ambiguous(self):
        declared = [_spec(near=(), near_paths=(), near_columns=()),
                    _spec(inputs=["forecast_temperature"], value="forecast_station", near=(), near_paths=(), near_columns=())]
        with self.assertRaisesRegex(DeclarationError, "two import filters"):
            _resolve(declared, _config(self.exports, measure_station="1", forecast_station="2"), [_mosmix()])

    def test_a_forecast_import_without_an_export_finds_its_place_in_kafka(self):
        exports = [_export(SOLAR_TOPIC, "solar")]
        messages = [{"import_id": MOSMIX_IMPORT, "value": {"meta": {"id": "10469", "lat": 51.3, "long": 12.4}}},
                    {"import_id": MOSMIX_IMPORT, "value": {"meta": {"id": "P0001", "lat": 48.0, "long": 11.5}}}]
        with mock.patch.object(kafka, "read_recent_messages", side_effect=_recent(messages)) as read:
            _resolve(self.declared, _config(exports, lat="51.34", long="12.37"), [_solar(), _mosmix()])
        self.assertEqual("02932", import_filter.filter_for(_solar()).value)
        self.assertEqual("10469", import_filter.filter_for(_mosmix()).value)
        self.assertEqual(MOSMIX_TOPIC, read.call_args.args[1])

    def test_a_filter_without_inputs_over_two_imports_is_refused(self):
        spec = _spec(near=(), near_paths=(), near_columns=())
        with self.assertRaisesRegex(DeclarationError, "has no inputs"):
            _resolve([spec], _config(self.exports, measure_station="02932"), [_solar(), _mosmix()])

    def test_a_filter_whose_inputs_reach_two_imports_is_refused(self):
        spec = _spec(inputs=["outdoor_temperature", "forecast_temperature"], near=(), near_paths=(), near_columns=())
        with self.assertRaisesRegex(DeclarationError, "names inputs of 2 imports"):
            _resolve([spec], _config(self.exports, measure_station="02932"), [_solar(), _mosmix()])

    def test_a_filter_without_inputs_is_fine_for_one_import_read_twice(self):
        spec = _spec(near=(), near_paths=(), near_columns=())
        _resolve([spec], _config(self.exports, measure_station="02932"), [_solar()], [_solar()])
        self.assertEqual("02932", import_filter.filter_for(_solar()).value)

    def test_the_operator_class_offers_both_forms(self):
        class Single(OperatorBase):
            import_filter = ImportFilter(path="p", column="c", value="measure_station")

        class Several(OperatorBase):
            import_filters = [ImportFilter(path="p", column="c", value="a"), ImportFilter(path="q", column="d", value="b")]

        self.assertEqual(["p"], [f.path for f in import_filter.declared_import_filters(Single())])
        self.assertEqual(["p", "q"], [f.path for f in import_filter.declared_import_filters(Several())])
        self.assertEqual([], import_filter.declared_import_filters(OperatorBase()))


class TestHistoryTimescale(Base):
    VF = ValueFilter("value.meta.id", "02932'; DROP TABLE x; --", "station")

    def setUp(self):
        super().setUp()
        self.entry = import_filter.find_import_export(
            _config([_export(SOLAR_TOPIC, "e1")]), _solar())
        self.query = getattr(timescale, "__get_export_dataset_query")
        self.params = getattr(timescale, "__export_query_params")

    def test_the_query_carries_a_bound_parameter_not_the_value(self):
        sql = self.query("dsn", _solar(), self.entry, timedelta(hours=1), False, E, self.VF)
        self.assertIn('AND "station" = %s', sql)
        self.assertNotIn("DROP", sql)
        self.assertEqual((self.VF.value,), self.params(self.VF))

    def test_without_a_filter_the_query_is_unchanged(self):
        sql = self.query("dsn", _solar(), self.entry, timedelta(hours=1), False, E)
        self.assertNotIn("%s", sql)
        self.assertIsNone(self.params(None))

    def test_a_percent_in_a_name_is_escaped_when_parameters_are_bound(self):
        entry = import_filter.find_import_export(
            _config([_export(SOLAR_TOPIC, "e1", **{"value.temp": "humidity_%"})]), _solar())
        vf = ValueFilter("value.meta.id", "x", "station_%")
        sql = self.query("dsn", _solar(), entry, timedelta(hours=1), False, E, vf)
        # What the driver sends once it has filled the parameter in.
        sent = sql % ("'x'",)
        self.assertIn('"humidity_%" AS "outdoor_temperature"', sent)
        self.assertIn('"station_%" = \'x\'', sent)

    def test_the_frame_reader_passes_the_parameters(self):
        with mock.patch.object(timescale, "__create_timescale_connection"), \
                mock.patch("pandas.io.sql.read_sql_query") as read:
            timescale.read_export_frame("dsn", _solar(), self.entry, timedelta(hours=1), False, E, self.VF)
        self.assertEqual((self.VF.value,), read.call_args.kwargs["params"])
        self.assertIn("%s", read.call_args.args[0])

    def test_the_filtered_ray_reader_passes_the_parameters(self):
        with mock.patch.object(timescale.ray.data, "read_sql") as read_sql:
            timescale.get_export_dataset_filtered_remote._function(
                "dsn", _solar(), self.entry, timedelta(hours=1), False, E, self.VF)
        self.assertEqual((self.VF.value,), read_sql.call_args.kwargs["sql_params"])

    def test_the_full_duration_probe_carries_the_parameter(self):
        cursor = mock.Mock()
        cursor.fetchone.return_value = (E - timedelta(hours=2),)
        conn = mock.Mock()
        conn.cursor.return_value = cursor
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=conn):
            self.query("dsn", _solar(), self.entry, timedelta(hours=1), True, E, self.VF)
        sql, params = cursor.execute.call_args.args
        self.assertTrue(sql.rstrip().endswith("LIMIT 1"))
        self.assertEqual((self.VF.value,), params)

    def test_the_unfiltered_ray_reader_keeps_its_old_signature(self):
        # An older worker image calls it with exactly these arguments.
        names = list(inspect.signature(timescale.get_export_dataset_remote._function).parameters)
        self.assertEqual(
            ["conn_str", "conf", "entry", "duration", "require_full_duration", "end"], names)

    def test_the_places_query_lists_recent_distinct_places(self):
        cursor = mock.Mock()
        cursor.fetchall.return_value = [("a", 1.0, 2.0)]
        conn = mock.Mock()
        conn.cursor.return_value = cursor
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=conn):
            rows = timescale.read_export_places("dsn", self.entry, ["station", "lat", "lon"], 7)
        self.assertEqual([("a", 1.0, 2.0)], rows)
        self.assertEqual(
            'SELECT DISTINCT "station", "lat", "lon" FROM "table_e1" '
            "WHERE time > NOW() - INTERVAL '7 days'", cursor.execute.call_args.args[0])

    def test_the_column_check_names_what_the_table_lacks(self):
        cursor = mock.Mock()
        cursor.description = [("time",), ("station",), ("lat",)]
        conn = mock.Mock()
        conn.cursor.return_value = cursor
        with mock.patch.object(timescale, "__create_timescale_connection", return_value=conn):
            timescale.check_export_columns("dsn", self.entry, ["station", "lat"])
            with self.assertRaisesRegex(MissingConfigValueError, "e1.*'lon'"):
                timescale.check_export_columns("dsn", self.entry, ["station", "lat", "lon"])


class TestResolvedReport(Base):
    """What the operator can ask for after start, e.g. to show which station it follows."""

    def test_the_operator_reports_each_filter_in_declaration_order(self):
        exports = [_export(SOLAR_TOPIC, "solar")]
        messages = [{"import_id": MOSMIX_IMPORT, "value": {"meta": {"id": "10469", "lat": 51.3, "long": 12.4}}}]
        class Op(_Operator):
            import_filters = [_spec(inputs=["outdoor_temperature"]),
                              _spec(inputs=["forecast_temperature", "forecasted_for"], value="forecast_station")]

        op = Op()
        op.set_topics([_solar(), _mosmix()], [])
        config = _config(exports, lat="51.34", long="12.37")
        with mock.patch("operator_lib.util.config.DeploymentConfig", return_value=DEP), \
                mock.patch.object(import_filter, "_read_places", return_value=[("02932", 51.32, 12.43)]), \
                mock.patch.object(kafka, "read_recent_messages", side_effect=_recent(messages)):
            op.init(None, None, None, "out", "p", "o", config=config)
        first, second = op.resolved_import_filters()
        self.assertEqual(("02932", "near", ("outdoor_temperature",)), (first.value, first.found, first.inputs))
        self.assertAlmostEqual(import_filter._distance_km(51.34, 12.37, 51.32, 12.43), first.distance_km)
        self.assertEqual((51.32, 12.43), (first.latitude, first.longitude))
        self.assertEqual(("10469", "near", (51.3, 12.4)), (second.value, second.found, (second.latitude, second.longitude)))

    def test_a_value_from_the_config_has_no_distance(self):
        _resolve([_spec()], _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932"), [_solar()])
        (entry,) = import_filter.resolved()
        self.assertEqual(("02932", "config", None, None), (entry.value, entry.found, entry.distance_km, entry.latitude))

    def test_a_filter_with_nothing_set_is_reported_as_none(self):
        with mock.patch.object(import_filter.logger, "warning"):
            _resolve([_spec()], _config([]), [_solar()])
        (entry,) = import_filter.resolved()
        self.assertEqual((None, "none"), (entry.value, entry.found))

    def test_an_operator_without_filters_reports_nothing(self):
        self.assertEqual([], _Operator().resolved_import_filters())


class TestRecentMessages(Base):
    def _consumer(self, partitions, messages):
        """partitions: {partition: (low, high)}; messages: [(partition, offset, body)]."""
        consumer = mock.Mock()
        topic_meta = mock.Mock(error=None, partitions=list(partitions))
        consumer.list_topics.return_value.topics = {"t": topic_meta}
        consumer.get_watermark_offsets.side_effect = lambda tp, timeout=None: partitions[tp.partition]
        polled = []
        for partition, offset, body in messages:
            m = mock.Mock(partition=mock.Mock(return_value=partition), offset=mock.Mock(return_value=offset),
                          value=mock.Mock(return_value=body.encode()))
            m.error.return_value = None
            polled.append(m)
        consumer.poll.side_effect = polled + [None] * 5
        return consumer

    def test_it_starts_n_before_the_end_of_each_partition_and_stops_at_the_end(self):
        consumer = self._consumer({0: (0, 50), 1: (90, 100)},
                                  [(0, 40, '{"a": 1}'), (0, 49, '{"a": 2}'), (1, 90, 'not json'), (1, 99, '{"a": 3}')])
        with mock.patch.object(kafka.confluent_kafka, "Consumer", return_value=consumer):
            messages = kafka.read_recent_messages("k", "t", 10, lambda m: m["a"])
        starts = {tp.partition: tp.offset for tp in consumer.assign.call_args.args[0]}
        self.assertEqual({0: 40, 1: 90}, starts)
        self.assertEqual([1, 2, 3], messages)
        consumer.close.assert_called_once()

    def test_only_the_extracted_part_is_kept_and_equal_results_once(self):
        consumer = self._consumer({0: (0, 4)}, [(0, i, '{"id": "a", "big": "' + "x" * 1000 + '"}') for i in range(3)]
                                  + [(0, 3, '{"id": "b", "big": "y"}')])
        with mock.patch.object(kafka.confluent_kafka, "Consumer", return_value=consumer):
            kept = kafka.read_recent_messages("k", "t", 10, lambda m: m["id"])
        self.assertEqual(["a", "b"], kept)

    def test_a_message_without_an_extraction_is_skipped(self):
        consumer = self._consumer({0: (0, 2)}, [(0, 0, '{"id": "a"}'), (0, 1, '{"other": 1}')])
        with mock.patch.object(kafka.confluent_kafka, "Consumer", return_value=consumer):
            kept = kafka.read_recent_messages("k", "t", 10, lambda m: m.get("id"))
        self.assertEqual(["a"], kept)

    def test_an_empty_partition_is_not_assigned(self):
        consumer = self._consumer({0: (5, 5)}, [])
        with mock.patch.object(kafka.confluent_kafka, "Consumer", return_value=consumer):
            self.assertEqual([], kafka.read_recent_messages("k", "t", 10, lambda m: m))
        self.assertEqual([], consumer.assign.call_args.args[0])

    def test_a_read_that_does_not_reach_the_end_raises(self):
        consumer = self._consumer({0: (0, 50)}, [(0, 40, '{"a": 1}')])
        with mock.patch.object(kafka.confluent_kafka, "Consumer", return_value=consumer):
            with self.assertRaisesRegex(TimeoutError, "partitions"):
                kafka.read_recent_messages("k", "t", 10, lambda m: m, timeout_s=0.0)

    def test_the_scan_size_must_be_positive(self):
        with self.assertRaises(DeclarationError):
            _spec(places_scan_messages=0)


class TestHistoryWrapper(Base):
    def setUp(self):
        super().setUp()
        self.entry = import_filter.find_import_export(_config([_export(SOLAR_TOPIC, "e1")]), _solar())

    def test_the_request_carries_the_filter(self):
        with mock.patch.object(ts_wrapper, "_post", return_value=[]) as post:
            ts_wrapper.read_export_history(
                "http://w", "tok", _solar(), self.entry, timedelta(days=1), end=E,
                value_filter=ValueFilter("value.meta.id", "02932", "station"))
        element = post.call_args.args[2][0]
        self.assertEqual([{"column": "station", "type": "=", "value": "02932"}], element["filters"])

    def test_without_a_filter_the_element_has_none(self):
        with mock.patch.object(ts_wrapper, "_post", return_value=[]) as post:
            ts_wrapper.read_export_history("http://w", "tok", _solar(), self.entry, timedelta(days=1), end=E)
        self.assertNotIn("filters", post.call_args.args[2][0])

    def test_the_unfiltered_remote_tasks_keep_their_old_signature(self):
        names = list(inspect.signature(ts_wrapper.get_ts_wrapper_export_dataset_remote._function).parameters)
        self.assertEqual(
            ["wrapper_url", "token", "conf", "entry", "duration", "require_full_duration", "end"], names)

    def test_the_places_come_from_the_last_days_and_are_deduplicated(self):
        rows = [["2026-06-01T00:00:00.000Z", "02932", 51.3, 12.4], ["2026-06-01T01:00:00.000Z", "02932", 51.3, 12.4],
                ["2026-06-01T01:00:00.000Z", "00001", 48.1, 11.6]]
        with mock.patch.object(ts_wrapper, "_post", return_value=[rows]) as post:
            places = ts_wrapper.read_export_places("http://w", "tok", self.entry, ["station", "lat", "lon"], 7)
        self.assertEqual({("02932", 51.3, 12.4), ("00001", 48.1, 11.6)}, set(places))
        element = post.call_args.args[2][0]
        self.assertEqual([{"name": "station"}, {"name": "lat"}, {"name": "lon"}], element["columns"])
        start = datetime.datetime.fromisoformat(element["time"]["start"].replace("Z", "+00:00"))
        end = datetime.datetime.fromisoformat(element["time"]["end"].replace("Z", "+00:00"))
        self.assertEqual(timedelta(days=7), end - start)

    def test_an_oversized_response_is_halved(self):
        calls = []

        def post(url, token, elements, path=None, params=None):
            window = elements[0]["time"]
            calls.append(window)
            if len(calls) == 1:
                raise ts_wrapper._OversizedResponse()
            return [[]]
        with mock.patch.object(ts_wrapper, "_post", side_effect=post):
            ts_wrapper.read_export_places("http://w", "tok", self.entry, ["station"], 7)
        self.assertEqual(3, len(calls))

    def test_a_refused_column_is_reported_as_missing(self):
        with mock.patch.object(ts_wrapper, "_post", side_effect=ts_wrapper.TimescaleWrapperError("returned 400")):
            with self.assertRaisesRegex(MissingConfigValueError, "'lat'"):
                ts_wrapper.check_export_columns("http://w", "tok", self.entry, ["station", "lat"])


class TestHistoryKafka(Base):
    def _rows(self):
        def row(station):
            return {"value": json.dumps({"import_id": "i", "value": {"meta": {"id": station}}}).encode()}
        return [row("02932"), row("10469"), row(2932),
                {"value": json.dumps({"import_id": "i", "value": "scalar"}).encode()},
                {"value": json.dumps({"import_id": "i"}).encode()},
                {"value": b"not json"}]

    def test_the_unfiltered_expression_references_nothing_an_older_worker_lacks(self):
        # Expressions travel to the workers pickled by value; the unfiltered one must be what v1.14.0 builds.
        from ray import cloudpickle
        from ray.data.expressions import col
        plain = cloudpickle.dumps(kafka.json_get(col("value"), "import_id") == "imp")
        self.assertNotIn(b"json_get_path", plain)
        self.assertNotIn(b"_json_path_value", plain)
        filtered = cloudpickle.dumps(kafka.value_filter_expression(ValueFilter("value.meta.id", "x", "c")))
        self.assertIn(b"json_get_path", filtered)

    def test_the_expression_keeps_only_the_station(self):
        import ray
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
        self.addCleanup(ray.shutdown)
        ds = ray.data.from_items(self._rows()).filter(
            expr=kafka.value_filter_expression(ValueFilter("value.meta.id", "02932", "station")))
        kept = [json.loads(r["value"])["value"]["meta"]["id"] for r in ds.take_all()]
        self.assertEqual(["02932"], kept)

    def test_the_unfiltered_remote_task_keeps_its_old_signature(self):
        names = list(inspect.signature(kafka.get_kafka_dataset_remote._function).parameters)
        self.assertEqual(
            ["bootstrap", "input_topic", "pipeline_id", "duration", "require_full_duration", "end"], names)

    def test_the_kafka_reader_gets_the_filter_from_the_dispatch(self):
        _resolve([_spec(near=(), near_paths=(), near_columns=())],
                 _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932"), [_solar()])
        dep = types.SimpleNamespace(ts_conn=None, senergy_token=None, config_bootstrap_servers="k", pipeline_id="p")
        with mock.patch.object(data, "get_kafka_dataset_local") as read:
            getattr(data, "__read_topic")(_config([]), dep, _solar(), timedelta(days=1), False, False, E)
        self.assertEqual(ValueFilter("value.meta.id", "02932", "station"), read.call_args.args[-1])


class TestRemoteChoice(Base):
    """A filtered read runs in its own remote tasks; the unfiltered ones stay as an older worker knows them."""

    def setUp(self):
        super().setUp()
        self.dep = types.SimpleNamespace(ts_conn=None, senergy_token=None, config_bootstrap_servers="k", pipeline_id="p")
        self.read_topic = getattr(data, "__read_topic")
        self.names = ["get_export_dataset_remote", "get_export_dataset_filtered_remote",
                      "get_kafka_dataset_remote", "get_kafka_dataset_filtered_remote"]
        self.mocks = {}
        for name in self.names:
            patch = mock.patch.object(data, name)
            self.mocks[name] = patch.start()
            self.addCleanup(patch.stop)

    def _read(self, with_filter, ts_conn):
        if with_filter:
            _resolve([_spec(near=(), near_paths=(), near_columns=())],
                     _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932"), [_solar()])
        data._logged_import_sources.clear()
        config = _config([_export(SOLAR_TOPIC, "e1")], **({"ts_conn": "dsn"} if ts_conn else {}))
        self.read_topic(config, self.dep, _solar(), timedelta(days=1), False, True, E)
        return {n: m.remote.called for n, m in self.mocks.items()}

    def test_a_filtered_export_read_uses_the_new_task(self):
        called = self._read(True, True)
        self.assertTrue(called["get_export_dataset_filtered_remote"])
        self.assertFalse(called["get_export_dataset_remote"])

    def test_an_unfiltered_export_read_uses_the_old_task(self):
        called = self._read(False, True)
        self.assertTrue(called["get_export_dataset_remote"])
        self.assertFalse(called["get_export_dataset_filtered_remote"])

    def test_a_filtered_kafka_read_uses_the_new_task(self):
        data._logged_import_sources.clear()
        _resolve([_spec(near=(), near_paths=(), near_columns=())],
                 _config([_export(SOLAR_TOPIC, "e1")], measure_station="02932"), [_solar()])
        self.read_topic(_config([]), self.dep, _solar(), timedelta(days=1), False, True, E)
        self.assertTrue(self.mocks["get_kafka_dataset_filtered_remote"].remote.called)
        self.assertFalse(self.mocks["get_kafka_dataset_remote"].remote.called)

    def test_an_unfiltered_kafka_read_uses_the_old_task(self):
        self.read_topic(_config([]), self.dep, _solar(), timedelta(days=1), False, True, E)
        self.assertTrue(self.mocks["get_kafka_dataset_remote"].remote.called)
        self.assertFalse(self.mocks["get_kafka_dataset_filtered_remote"].remote.called)


class _StartingOperator(OperatorBase):
    configType = Cfg
    import_filter = _spec(near=(), near_paths=(), near_columns=())

    def start(self):
        pass


class TestStart(Base):
    CONFIG = {
        "config": {"measure_station": "02932",
                   "import_exports": json.dumps([_export(SOLAR_TOPIC, "e1")])},
        "inputTopics": [{"name": SOLAR_TOPIC, "filterType": "ImportId", "filterValue": SOLAR_IMPORT,
                         "mappings": [{"dest": "outdoor_temperature", "source": "value.temp"}]}],
        "trainingTopics": [],
    }

    def test_operator_lib_resolves_the_filter_before_start(self):
        import operator_lib.operator_lib as lib
        dep = types.SimpleNamespace(
            pipeline_id="p", operator_id="o", output="out", baggage=None, metrics=False,
            config_bootstrap_servers="k", config_application_id="app",
            consumer_auto_offset_reset_config="earliest", ts_conn=None, senergy_token=None)
        patches = [
            mock.patch.object(lib.util, "print_init"),
            mock.patch.object(lib.util, "DeploymentConfig", return_value=dep),
            mock.patch("operator_lib.util.config.DeploymentConfig", return_value=dep),
            mock.patch.object(lib.util, "load_operator_config_json", return_value=self.CONFIG),
            mock.patch.object(lib.util, "init_logger"),
            mock.patch.object(lib.util, "parse_baggage"),
            mock.patch.object(lib.util, "redact_config"),
            mock.patch.object(lib.util, "create_filter_handler"),
            mock.patch.object(lib.confluent_kafka, "Consumer"),
            mock.patch.object(lib.confluent_kafka, "Producer"),
            mock.patch.object(lib.cncr_wdg, "Watchdog"),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        lib.OperatorLib(_StartingOperator(), git_info_file=None)
        self.assertEqual(
            ValueFilter("value.meta.id", "02932", "station"), import_filter.filter_for(_solar()))

    def test_an_operator_with_its_own_init_signature_still_starts(self):
        class Own(_StartingOperator):
            def init(self, kafka_consumer, kafka_producer, filter_handler, output_topic, pipeline_id,
                     operator_id, poll_timeout=1.0, config=Cfg({}), result_error_handler=None):
                super().init(kafka_consumer, kafka_producer, filter_handler, output_topic, pipeline_id,
                             operator_id, poll_timeout, config, result_error_handler)

        operator = Own()
        operator.set_topics([_solar()], [])
        with mock.patch("operator_lib.util.config.DeploymentConfig", return_value=DEP):
            operator.init(None, None, None, "out", "p", "o", config=_config(
                [_export(SOLAR_TOPIC, "e1")], measure_station="02932"))
        self.assertEqual("02932", import_filter.filter_for(_solar()).value)

    def test_an_ml_operator_resolves_before_its_first_training(self):
        import mlflow
        from operator_lib.util import MLOperator
        seen = []

        class Ml(MLOperator):
            import_filter = _spec(near=(), near_paths=(), near_columns=())

        operator = Ml()
        operator.set_topics([_solar()], [])
        with mock.patch("operator_lib.util.config.DeploymentConfig", return_value=DEP), \
                mock.patch("ray.shutdown"), mock.patch.object(mlflow, "set_tracking_uri"), \
                mock.patch.object(mlflow, "set_experiment"), \
                mock.patch.object(Ml, "_MLOperator__load_model", return_value=None), \
                mock.patch.object(Ml, "_MLOperator__wrap_training",
                                  side_effect=lambda: seen.append(import_filter.filter_for(_solar()))):
            operator.init(None, None, None, "out", "p", "o", config=_config(
                [_export(SOLAR_TOPIC, "e1")], measure_station="02932"))
        self.assertEqual([ValueFilter("value.meta.id", "02932", "station")], seen)


if __name__ == "__main__":
    unittest.main()
