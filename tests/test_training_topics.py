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
import os
import types
import unittest
from datetime import timedelta
from unittest import mock

import pandas as pd

import operator_lib.operator_lib as operator_lib_module
import operator_lib.util as util
from operator_lib.util import clock
from operator_lib.util.helpers import data
from operator_lib.util.model import Config, InputTopic, OperatorConfig, Selector
from operator_lib.util.op_ml import MLOperator, _compute_evaluation_metric

TRAINING_END = datetime.datetime(2026, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
TEST_END = datetime.datetime(2026, 6, 1, 0, 5, 0, tzinfo=datetime.timezone.utc)

# A main meter and a sub-meter of the same device model: one service, so one
# topic name, told apart only by the device. The sub-meter is the label.
SERVICE_TOPIC = "urn_infai_ses_service_1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
MAIN_METER = "device-main-meter"
SUB_METER = "device-heat-pump"
WALLBOX = "device-wallbox"
WEATHER = "device-weather"

# `_source_path` drops the first path element, so the sub-meter's mapping
# resolves against this target series and the main meter's does not.
TARGET_SERIES = "root.egy_watt"


def _topic_dict(name, filter_value, dest, source):
    return {
        "name": name,
        "filterType": "DeviceId",
        "filterValue": filter_value,
        "mappings": [{"dest": dest, "source": source}],
    }


MAIN = _topic_dict(SERVICE_TOPIC, MAIN_METER, "meter", "value.sensor.data.obis_1_8_0_f")
WEATHER_INPUT = _topic_dict("urn_infai_ses_service_weather", WEATHER, "temp", "value.temp")
HEAT_PUMP = _topic_dict(SERVICE_TOPIC, SUB_METER, "hp", "value.root.egy_watt")
WALLBOX_LABEL = _topic_dict("urn_infai_ses_service_wallbox", WALLBOX, "wb", "value.power")


def _naive(minute, second=0):
    # Readers return tz-naive UTC times; the replay treats them as UTC.
    return datetime.datetime(2026, 6, 1, 0, minute, second)


def _main_frame():
    # One reading a minute, :00 to :04.
    return pd.DataFrame({
        "time": [_naive(m) for m in range(5)],
        "meter": [100.0, 101.0, 102.0, 103.0, 104.0],
    })


def _heat_pump_frame():
    # Half a minute after each main meter reading, so that a row of this frame
    # reaching the replay would show up as an extra infer() call at its own
    # time.
    return pd.DataFrame({
        "time": [_naive(m, 30) for m in range(5)],
        "hp": [1.0, 3.0, 5.0, 7.0, 9.0],
    })


def _inputs():
    return [(InputTopic(MAIN), _main_frame())]


def _training():
    return [(InputTopic(HEAT_PUMP), _heat_pump_frame())]


def _window_reader(inputs, training):
    def read(start, end, role="input"):
        return training if role == "training" else inputs
    return read


class _RecordingOperator(MLOperator):
    def __init__(self):
        self.infer_calls = []
        self.trained_on = []

    def infer(self, model, data_, selector, device_id, timestamp):
        self.infer_calls.append({
            "data": data_,
            "selector": selector,
            "device_id": device_id,
            "timestamp": timestamp,
        })
        return None, {"prediction": 4.0}, None

    def train(self, model, logger):
        return None

    def need_retraining(self, model):
        return False


def _mlflow_mock():
    m = mock.MagicMock()
    m.start_run.return_value.info.run_id = "r1"
    m.active_run.return_value = None
    return m


def _mlflow_client_mock():
    client = mock.MagicMock()
    client.artifacts = []

    def _capture_artifact(run_id, path, artifact_path=None):
        with open(path, "r") as fh:
            client.artifacts.append(fh.read())

    client.log_artifact.side_effect = _capture_artifact
    return client


def _logger_mock():
    logger_cls = mock.MagicMock()
    logger_cls.return_value.trace.return_value.__enter__.return_value = None
    logger_cls.return_value.trace.return_value.__exit__.return_value = False
    return logger_cls


def _split_config(**extra):
    return Config({
        "training_end": "2026-06-01T00:00:00Z",
        "test_end": "2026-06-01T00:05:00Z",
        **extra,
    })


def _evaluate(operator, config, inputs, training, client=None):
    client = client or _mlflow_client_mock()
    with mock.patch("operator_lib.util.op_ml.ray"), \
         mock.patch("operator_lib.util.op_ml.mlflow", _mlflow_mock()), \
         mock.patch("operator_lib.util.op_ml.MlflowClient", return_value=client), \
         mock.patch("operator_lib.util.op_ml.TrainMlflowLogger", _logger_mock()), \
         mock.patch(
             "operator_lib.util.helpers.data.read_input_window",
             side_effect=_window_reader(inputs, training),
         ):
        operator.init(
            kafka_consumer=None,
            kafka_producer=None,
            filter_handler=None,
            output_topic=None,
            pipeline_id="p",
            operator_id="o",
            config=config,
        )
    return client


def _params(client):
    return {call.args[1]: call.args[2] for call in client.log_param.call_args_list}


class TestOperatorConfig(unittest.TestCase):
    def test_training_topics_parse_next_to_the_input_topics(self):
        config = OperatorConfig({"inputTopics": [MAIN], "trainingTopics": [HEAT_PUMP]})
        self.assertEqual([MAIN_METER], [t.filterValue for t in config.inputTopics])
        self.assertEqual([SUB_METER], [t.filterValue for t in config.trainingTopics])
        self.assertIsInstance(config.trainingTopics[0], InputTopic)
        self.assertEqual("hp", config.trainingTopics[0].mappings[0].dest)
        self.assertEqual("value.root.egy_watt", config.trainingTopics[0].mappings[0].source)

    def test_a_config_without_the_key_has_no_training_topics(self):
        # What the flow engine writes, and what ODE wrote before v1.9.0.
        config = OperatorConfig({"inputTopics": [MAIN]})
        self.assertEqual([], config.trainingTopics)
        self.assertEqual([MAIN_METER], [t.filterValue for t in config.inputTopics])

    def test_null_is_no_training_topics(self):
        config = OperatorConfig({"inputTopics": [MAIN], "trainingTopics": None})
        self.assertEqual([], config.trainingTopics)
        self.assertEqual([MAIN_METER], [t.filterValue for t in config.inputTopics])

    def test_the_live_filters_are_built_from_the_input_topics_alone(self):
        # OperatorLib subscribes through create_filter_handler; a training
        # topic reaching it would be delivered to run() and so to infer().
        operator_config = {"config": {}, "inputTopics": [MAIN], "trainingTopics": [HEAT_PUMP]}
        operator = mock.MagicMock()
        operator.selectors = []
        with mock.patch.dict(os.environ, {"CONFIG": json.dumps(operator_config)}), \
             mock.patch.object(operator_lib_module, "confluent_kafka"), \
             mock.patch.object(operator_lib_module, "cncr_wdg"), \
             mock.patch.object(operator_lib_module, "prometheus_client"), \
             mock.patch.object(util, "create_filter_handler") as create_filter_handler:
            operator_lib_module.OperatorLib(operator, git_info_file=None)
        topics = create_filter_handler.call_args.args[0]
        self.assertEqual([MAIN_METER], [t.filterValue for t in topics])


class TestRoleSelection(unittest.TestCase):
    """
    provide_historic_data, provide_historic_data_local and read_input_window
    choose the topic list in the driver. __read_topic -- the dispatch to the
    readers, which run on Ray workers with the cluster image's library -- is
    replaced here by a function with exactly its own seven parameters, so a
    role reaching it would fail the call.
    """

    def setUp(self):
        self.addCleanup(clock.set_fixed, None)
        self.addCleanup(data.withhold_training_topics, False)
        self._use_config({
            "config": {},
            "inputTopics": [MAIN, WEATHER_INPUT],
            "trainingTopics": [HEAT_PUMP, WALLBOX_LABEL],
        })
        read_patch = mock.patch.object(data, "__read_topic", side_effect=self._read_topic)
        self.read_topic = read_patch.start()
        self.addCleanup(read_patch.stop)

    def _use_config(self, operator_config):
        dep = types.SimpleNamespace(
            config=json.dumps(operator_config), ts_conn=None, senergy_token=None,
            config_bootstrap_servers="kafka:9092", pipeline_id="p")
        patch = mock.patch.object(util, "DeploymentConfig", return_value=dep)
        self.deployment_config = patch.start()
        self.addCleanup(patch.stop)

    @staticmethod
    def _read_topic(config, dep_config, topic, duration, require_full_duration, remote, end):
        frame = pd.DataFrame({"time": [], topic.mappings[0].dest: []})
        return types.SimpleNamespace(topic=topic, to_pandas=lambda: frame)

    def _read_devices(self):
        return [call.args[2].filterValue for call in self.read_topic.call_args_list]

    def test_input_is_the_default_and_reads_the_input_topics_only(self):
        datasets = data.provide_historic_data_local(timedelta(days=1))
        self.assertEqual([MAIN_METER, WEATHER], [ds.topic.filterValue for ds in datasets])
        self.assertEqual([MAIN_METER, WEATHER], self._read_devices())

    def test_an_explicit_input_role_reads_the_same(self):
        data.provide_historic_data_local(timedelta(days=1), role="input")
        self.assertEqual([MAIN_METER, WEATHER], self._read_devices())

    def test_training_reads_the_training_topics_only_in_config_order(self):
        datasets = data.provide_historic_data_local(timedelta(days=1), role="training")
        self.assertEqual([SUB_METER, WALLBOX], [ds.topic.filterValue for ds in datasets])
        self.assertEqual([SUB_METER, WALLBOX], self._read_devices())

    def test_the_remote_variant_selects_the_same_way(self):
        data.provide_historic_data(timedelta(days=1), role="training")
        self.assertEqual([SUB_METER, WALLBOX], self._read_devices())
        self.assertTrue(all(call.args[5] is True for call in self.read_topic.call_args_list))

    def test_read_input_window_selects_by_role(self):
        start, end = TRAINING_END, TEST_END
        inputs = data.read_input_window(start, end)
        training = data.read_input_window(start, end, role="training")
        self.assertEqual([MAIN_METER, WEATHER], [t.filterValue for t, _ in inputs])
        self.assertEqual([SUB_METER, WALLBOX], [t.filterValue for t, _ in training])
        self.assertEqual(["time", "hp"], list(training[0][1].columns))

    def test_the_readers_receive_the_arguments_they_always_did(self):
        clock.set_fixed(TRAINING_END)
        data.provide_historic_data_local(timedelta(days=1), True, role="training")
        call = self.read_topic.call_args_list[0]
        self.assertEqual({}, call.kwargs)
        self.assertEqual(7, len(call.args))
        self.assertEqual(SUB_METER, call.args[2].filterValue)
        # duration, require_full_duration, remote, and the clock's bound.
        self.assertEqual((timedelta(days=1), True, False, TRAINING_END), call.args[3:])

    def test_an_unknown_role_raises_before_the_config_is_read(self):
        calls = {
            "provide_historic_data": lambda: data.provide_historic_data(
                timedelta(days=1), role="label"),
            "provide_historic_data_local": lambda: data.provide_historic_data_local(
                timedelta(days=1), role="Training"),
            "read_input_window": lambda: data.read_input_window(
                TRAINING_END, TEST_END, role=None),
        }
        for name, call in calls.items():
            with self.subTest(function=name):
                with self.assertRaises(ValueError) as ctx:
                    call()
                self.assertIn("'input', 'training'", str(ctx.exception))
        self.deployment_config.assert_not_called()
        self.read_topic.assert_not_called()

    def test_a_config_without_training_topics_reads_nothing_for_training(self):
        self._use_config({"config": {}, "inputTopics": [MAIN]})
        self.assertEqual([], data.provide_historic_data_local(timedelta(days=1), role="training"))
        self.assertEqual([], data.read_input_window(TRAINING_END, TEST_END, role="training"))
        self.read_topic.assert_not_called()
        data.provide_historic_data_local(timedelta(days=1))
        self.assertEqual([MAIN_METER], self._read_devices())

    def test_withheld_training_topics_raise_while_input_topics_are_still_read(self):
        data.withhold_training_topics(True)
        for call in (
            lambda: data.provide_historic_data(timedelta(days=1), role="training"),
            lambda: data.provide_historic_data_local(timedelta(days=1), role="training"),
            lambda: data.read_input_window(TRAINING_END, TEST_END, role="training"),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                call()
            self.assertIn("withheld", str(ctx.exception))
        self.read_topic.assert_not_called()
        data.provide_historic_data_local(timedelta(days=1))
        self.assertEqual([MAIN_METER, WEATHER], self._read_devices())

        data.withhold_training_topics(False)
        self.read_topic.reset_mock()
        data.provide_historic_data_local(timedelta(days=1), role="training")
        self.assertEqual([SUB_METER, WALLBOX], self._read_devices())


class TestEvaluationWithTrainingTopics(unittest.TestCase):
    """
    The evaluation phase with a training topic on the launch: the sub-meter is
    read for the metric and nothing else. MLflow, Ray and read_input_window
    are mocked as in tests/test_evaluation.py; read_input_window answers by
    role.
    """

    def setUp(self):
        self.addCleanup(clock.set_fixed, None)
        self.addCleanup(data.withhold_training_topics, False)

    def test_the_replay_never_hands_infer_a_training_topic_row(self):
        operator = _RecordingOperator()
        client = _evaluate(operator, _split_config(), _inputs(), _training())

        # One call per main meter row, at its own time: the heat pump's rows at
        # :xx:30 would have added five more.
        self.assertEqual([_naive(m).replace(tzinfo=datetime.timezone.utc) for m in range(5)],
                         [c["timestamp"] for c in operator.infer_calls])
        self.assertEqual({MAIN_METER}, {c["device_id"] for c in operator.infer_calls})
        self.assertTrue(all(set(c["data"]) == {"meter"} for c in operator.infer_calls))
        self.assertEqual(5, _params(client)["evaluation.messages"])

        predictions = client.artifacts[0]
        self.assertIn(MAIN_METER, predictions)
        self.assertNotIn(SUB_METER, predictions)

    def test_get_selector_is_never_asked_about_a_training_topic(self):
        # A selector for the main meter's fields only: get_selector raises for
        # the heat pump's, so a call for it would also fail the replay.
        operator = _RecordingOperator()
        operator.selectors = [Selector({"name": "main", "args": ["meter"]})]
        with mock.patch.object(util, "get_selector", wraps=util.get_selector) as get_selector:
            client = _evaluate(operator, _split_config(), _inputs(), _training())

        asked = [{m.dest for m in call.args[0]} for call in get_selector.call_args_list]
        self.assertEqual([{"meter"}], asked)
        self.assertEqual({"main"}, {c["selector"] for c in operator.infer_calls})
        client.set_terminated.assert_called_once_with("r1", status="FINISHED")

    def test_the_metric_is_scored_against_the_training_topic(self):
        # Same topic name for both meters, so this also covers a lookup by
        # name, which would have found the main meter's rows and no "hp".
        operator = _RecordingOperator()
        config = _split_config(
            evaluation_metric="mae",
            evaluation_target_series=TARGET_SERIES,
            evaluation_prediction_field="prediction",
            evaluation_resolution="2m",
        )
        client = _evaluate(operator, config, _inputs(), _training())

        params = _params(client)
        self.assertEqual("computed", params["evaluation.metric_status"])
        # 2m buckets of the heat pump: means 2, 6 and 9. Every message
        # predicts 4 at its own time: errors 2, 2 and 5 per bucket.
        self.assertEqual(3, params["evaluation.metric_n"])
        self.assertAlmostEqual(3.0, params["evaluation.metric_value"])

    def test_infer_reading_the_training_topics_fails_the_replay(self):
        class _PeekingOperator(_RecordingOperator):
            def infer(self, model, data_, selector, device_id, timestamp):
                super().infer(model, data_, selector, device_id, timestamp)
                data.provide_historic_data_local(timedelta(hours=1), role="training")
                return None, {"prediction": 4.0}, None

        operator = _PeekingOperator()
        client = _mlflow_client_mock()
        with self.assertRaises(RuntimeError) as ctx:
            _evaluate(operator, _split_config(), _inputs(), _training(), client)

        self.assertIn("withheld", str(ctx.exception))
        self.assertEqual(1, len(operator.infer_calls))
        client.set_terminated.assert_called_once_with("r1", status="FAILED")
        # Lifted again once the replay is over, so a later train() is not
        # refused.
        self.assertFalse(data._training_topics_withheld)

    def test_train_reads_the_training_topics_up_to_training_end(self):
        class _TrainingOperator(_RecordingOperator):
            def train(self, model, logger):
                self.trained_on = data.provide_historic_data_local(
                    timedelta(days=30), role="training")
                return None

        operator_config = {"config": {}, "inputTopics": [MAIN], "trainingTopics": [HEAT_PUMP]}
        dep = types.SimpleNamespace(
            config=json.dumps(operator_config), ts_conn=None, senergy_token=None,
            config_bootstrap_servers="kafka:9092", pipeline_id="p")
        operator = _TrainingOperator()
        with mock.patch.object(util, "DeploymentConfig", return_value=dep), \
             mock.patch.object(data, "__read_topic") as read_topic:
            _evaluate(operator, _split_config(), _inputs(), _training())

        self.assertEqual(1, read_topic.call_count)
        args = read_topic.call_args.args
        self.assertEqual(SUB_METER, args[2].filterValue)
        # The split bounds the label as it bounds every input.
        self.assertEqual(TRAINING_END, args[6])
        self.assertEqual(1, len(operator.trained_on))


class TestTargetResolution(unittest.TestCase):
    """
    _compute_evaluation_metric over input and training topics together,
    against hand-built frames; no replay is run here.
    """

    def _config(self, target_series=TARGET_SERIES):
        return Config({
            "evaluation_metric": "mae",
            "evaluation_target_series": target_series,
            "evaluation_prediction_field": "prediction",
            "evaluation_resolution": "1h",
        })

    def _prediction(self, at, value):
        return {
            "time": at, "topic": SERVICE_TOPIC, "selector": None, "device_id": MAIN_METER,
            "result_time": at, "result": json.dumps({"prediction": value}),
        }

    def _frame(self, dest, values):
        return pd.DataFrame({"time": [_naive(10), _naive(50)], dest: values})

    def test_a_target_in_a_training_topic_is_scored_against_its_frame(self):
        inputs = [(InputTopic(WEATHER_INPUT), self._frame("temp", [20.0, 21.0]))]
        training = [(InputTopic(HEAT_PUMP), self._frame("hp", [10.0, 20.0]))]
        rows = [self._prediction("2026-06-01T00:30:00Z", 18.0)]  # |18 - 15| = 3

        status, name, value, n = _compute_evaluation_metric(self._config(), rows, inputs, training)

        self.assertEqual("computed", status)
        self.assertAlmostEqual(3.0, value)
        self.assertEqual(1, n)

    def test_a_target_in_both_an_input_and_a_training_topic_matches_2_mappings(self):
        both = _topic_dict("urn_infai_ses_service_other", "device-other", "hp_in", "value.root.egy_watt")
        inputs = [(InputTopic(both), self._frame("hp_in", [1.0, 2.0]))]
        training = [(InputTopic(HEAT_PUMP), self._frame("hp", [10.0, 20.0]))]
        rows = [self._prediction("2026-06-01T00:30:00Z", 18.0)]

        status, name, value, n = _compute_evaluation_metric(self._config(), rows, inputs, training)

        self.assertEqual("target series matches 2 mappings, expected exactly one", status)
        self.assertNotIn("root.egy_watt", status)
        self.assertIsNone(value)

    def test_a_training_topic_sharing_its_name_with_an_input_topic_is_scored_against_its_own_frame(self):
        # The main meter's frame carries an "hp" column of its own, with other
        # values: anything that finds the target's frame by topic name finds
        # this one first and computes |18 - 105| instead.
        main_frame = pd.DataFrame({
            "time": [_naive(10), _naive(50)], "meter": [1.0, 2.0], "hp": [100.0, 110.0]})
        inputs = [(InputTopic(MAIN), main_frame)]
        training = [(InputTopic(HEAT_PUMP), self._frame("hp", [10.0, 20.0]))]
        self.assertEqual(inputs[0][0].name, training[0][0].name)
        rows = [self._prediction("2026-06-01T00:30:00Z", 18.0)]

        status, name, value, n = _compute_evaluation_metric(self._config(), rows, inputs, training)

        self.assertEqual("computed", status)
        self.assertAlmostEqual(3.0, value)

    def test_an_input_topic_is_still_a_valid_target_next_to_training_topics(self):
        inputs = [(InputTopic(WEATHER_INPUT), self._frame("temp", [20.0, 22.0]))]
        training = [(InputTopic(HEAT_PUMP), self._frame("hp", [10.0, 20.0]))]
        rows = [self._prediction("2026-06-01T00:30:00Z", 25.0)]  # |25 - 21| = 4

        status, name, value, n = _compute_evaluation_metric(
            self._config(target_series="temp"), rows, inputs, training)

        self.assertEqual("computed", status)
        self.assertAlmostEqual(4.0, value)

    def test_a_target_matching_nothing_keeps_its_wording(self):
        # ODE mirrors this wording in its launch warnings.
        inputs = [(InputTopic(MAIN), _main_frame())]
        training = [(InputTopic(HEAT_PUMP), _heat_pump_frame())]

        status, _, _, _ = _compute_evaluation_metric(
            self._config(target_series="sensor.DOES.NOT.EXIST"), [], inputs, training)

        self.assertEqual("target series matches no input or training topic", status)
        self.assertNotIn("sensor.DOES.NOT.EXIST", status)


if __name__ == "__main__":
    unittest.main()
