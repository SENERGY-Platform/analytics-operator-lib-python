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
import tempfile
import unittest
from unittest import mock

import pandas as pd

import mlflow
from mlflow import MlflowClient

from operator_lib.util import clock
from operator_lib.util.config import MissingConfigValueError
from operator_lib.util.model import Config, InputTopic
from operator_lib.util.op_ml import MLOperator, _compute_evaluation_metric

TRAINING_END = datetime.datetime(2026, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
TEST_END = datetime.datetime(2026, 6, 1, 0, 5, 0, tzinfo=datetime.timezone.utc)


class _RecordingOperator(MLOperator):
    """
    Records every call the evaluation makes into it, so the test can inspect
    order and arguments without needing a real model.
    """

    def __init__(self):
        self.infer_calls = []
        self.clock_at_infer = []
        self.train_calls = []
        self.need_retraining_calls = []

    def infer(self, model, data, selector, device_id, timestamp):
        self.infer_calls.append({
            "model": model,
            "data": data,
            "selector": selector,
            "device_id": device_id,
            "timestamp": timestamp,
        })
        self.clock_at_infer.append(clock.now())
        return None, {"seen": True}, None

    def train(self, model, logger):
        self.train_calls.append(model)
        return None

    def need_retraining(self, model):
        self.need_retraining_calls.append(model)
        return False


def _topics_and_frames():
    topic1 = InputTopic({
        "name": "topic1",
        "filterType": "DeviceId",
        "filterValue": "device-a",
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    })
    topic2 = InputTopic({
        "name": "topic2",
        "filterType": "DeviceId",
        "filterValue": "device-b",
        "mappings": [{"dest": "hum", "source": "value.hum"}],
    })
    # Interleaved times: topic1 at :00/:02/:04, topic2 at :01/:03, so the
    # merged, time-sorted replay alternates between the two topics.
    frame1 = pd.DataFrame({
        "time": [
            datetime.datetime(2026, 6, 1, 0, 0, 0),
            datetime.datetime(2026, 6, 1, 0, 2, 0),
            datetime.datetime(2026, 6, 1, 0, 4, 0),
        ],
        "temp": [1.0, 3.0, 5.0],
    })
    frame2 = pd.DataFrame({
        "time": [
            datetime.datetime(2026, 6, 1, 0, 1, 0),
            datetime.datetime(2026, 6, 1, 0, 3, 0),
        ],
        "hum": [50.0, 60.0],
    })
    return [(topic1, frame1), (topic2, frame2)]


def _mlflow_mock():
    m = mock.MagicMock()
    run_obj = mock.MagicMock()
    run_obj.info.run_id = "r1"
    m.start_run.return_value = run_obj
    m.active_run.return_value = None
    m.pyfunc.load_model.return_value = "sentinel-registered-model"
    return m


def _mlflow_client_mock():
    """
    A stand-in for MlflowClient() -- op_ml.py records the evaluation through
    this, with an explicit run id, rather than through the fluent API, so
    the client needs its own mock and its own call assertions, separate
    from the fluent mlflow mock above.
    """
    client = mock.MagicMock()
    client.artifact_calls = []

    def _capture_artifact(run_id, path, artifact_path=None):
        with open(path, "r") as fh:
            content = fh.read()
        client.artifact_calls.append({
            "run_id": run_id,
            "basename": os.path.basename(path),
            "artifact_path": artifact_path,
            "content": content,
        })

    client.log_artifact.side_effect = _capture_artifact
    return client


def _logger_mock():
    logger_cls = mock.MagicMock()
    logger_cls.return_value.trace.return_value.__enter__.return_value = None
    logger_cls.return_value.trace.return_value.__exit__.return_value = False
    return logger_cls


class TestEvaluationMode(unittest.TestCase):
    def setUp(self):
        self.addCleanup(clock.set_fixed, None)

    def test_evaluation_trains_once_and_replays_the_test_window_in_time_order(self):
        mock_mlflow = _mlflow_mock()
        mock_client = _mlflow_client_mock()
        inputs = _topics_and_frames()
        operator = _RecordingOperator()

        with mock.patch("operator_lib.util.op_ml.ray"), \
             mock.patch("operator_lib.util.op_ml.mlflow", mock_mlflow), \
             mock.patch("operator_lib.util.op_ml.MlflowClient", return_value=mock_client), \
             mock.patch("operator_lib.util.op_ml.TrainMlflowLogger", _logger_mock()), \
             mock.patch(
                 "operator_lib.util.helpers.data.read_input_window",
                 return_value=inputs,
             ) as mock_read_window:
            operator.init(
                kafka_consumer=None,
                kafka_producer=None,
                filter_handler=None,
                output_topic=None,
                pipeline_id="p",
                operator_id="o",
                config=Config({
                    "training_end": "2026-06-01T00:00:00Z",
                    "test_end": "2026-06-01T00:05:00Z",
                }),
            )

        # The registry was never consulted: train() saw None, not the
        # sentinel pyfunc.load_model would have returned.
        self.assertEqual([None], operator.train_calls)
        mock_read_window.assert_called_once_with(TRAINING_END, TEST_END)

        # infer() once per row, in time order, clock.now() at the row's time.
        self.assertEqual(5, len(operator.infer_calls))
        expected_times = [
            TRAINING_END,
            TRAINING_END + datetime.timedelta(minutes=1),
            TRAINING_END + datetime.timedelta(minutes=2),
            TRAINING_END + datetime.timedelta(minutes=3),
            TRAINING_END + datetime.timedelta(minutes=4),
        ]
        self.assertEqual(expected_times, [c["timestamp"] for c in operator.infer_calls])
        self.assertEqual(expected_times, operator.clock_at_infer)
        self.assertEqual(
            ["device-a", "device-b", "device-a", "device-b", "device-a"],
            [c["device_id"] for c in operator.infer_calls],
        )
        self.assertEqual({"temp": 1.0}, operator.infer_calls[0]["data"])
        self.assertEqual({"hum": 50.0}, operator.infer_calls[1]["data"])

        # need_retraining is deliberately never consulted during the replay.
        self.assertEqual([], operator.need_retraining_calls)

        # __wrap_training leaves the training run open (train() returned
        # None here, so __update_model never ran); it is closed
        # unconditionally, through the fluent API one last time, before the
        # replay -- everything from here on goes through the client instead.
        mock_mlflow.end_run.assert_called_once_with()

        # The record on the run, through MlflowClient against the run id
        # captured before the replay.
        run_id = "r1"
        mock_client.set_tag.assert_any_call(
            run_id, "operator_lib.history_end", TRAINING_END.isoformat())
        mock_client.set_tag.assert_any_call(
            run_id, "operator_lib.test_end", TEST_END.isoformat())
        # And the phase transition, stamped before the first infer() so that
        # ODE can withhold whatever was written after it.
        stamps = [
            call for call in mock_client.set_tag.call_args_list
            if call.args[1] == "operator_lib.training_ended_at"
        ]
        self.assertEqual(1, len(stamps))
        self.assertEqual(run_id, stamps[0].args[0])
        self.assertRegex(stamps[0].args[2], r"^\d{13}$")
        self.assertEqual(3, mock_client.set_tag.call_count)

        # Exactly these four -- evaluation.completed is gone, ODE never read
        # it, and whether the replay finished is now the run's terminal
        # status (asserted via set_terminated below).
        mock_client.log_param.assert_any_call(run_id, "evaluation.messages", 5)
        mock_client.log_param.assert_any_call(run_id, "evaluation.results", 5)
        mock_client.log_param.assert_any_call(
            run_id, "evaluation.window_start", TRAINING_END.isoformat())
        mock_client.log_param.assert_any_call(
            run_id, "evaluation.window_end", TEST_END.isoformat())
        # A fifth param: none of the four evaluation_* config keys are set on
        # this launch, so metric_status is always logged, but the metric
        # itself is not computed and metric_name/value/n stay absent.
        mock_client.log_param.assert_any_call(
            run_id, "evaluation.metric_status",
            "evaluation_metric, evaluation_target_series, "
            "evaluation_prediction_field, evaluation_resolution not set")
        self.assertEqual(5, mock_client.log_param.call_count)

        # predictions.csv only: inputs.csv is gone along with the write that
        # produced it.
        self.assertEqual(1, len(mock_client.artifact_calls))
        artifact = mock_client.artifact_calls[0]
        self.assertEqual(run_id, artifact["run_id"])
        self.assertEqual("predictions.csv", artifact["basename"])
        self.assertEqual("evaluation", artifact["artifact_path"])
        prediction_lines = artifact["content"].strip().splitlines()
        self.assertEqual(1 + 5, len(prediction_lines))  # header + one per row

        mock_client.set_terminated.assert_called_once_with(run_id, status="FINISHED")

        # The clock is back at training_end once init() has returned.
        self.assertEqual(TRAINING_END, clock.fixed())

    def test_a_result_without_result_time_is_scored_at_its_message_time(self):
        # infer() returning None as its result timestamp means "now", as in
        # run(); the replay's now is each message's own time, so every
        # prediction lands in the bucket of the message that produced it.
        class _NowcastOperator(_RecordingOperator):
            def infer(self, model, data, selector, device_id, timestamp):
                super().infer(model, data, selector, device_id, timestamp)
                return None, {"prediction": 4.0}, None

        mock_client = _mlflow_client_mock()
        operator = _NowcastOperator()

        with mock.patch("operator_lib.util.op_ml.ray"), \
             mock.patch("operator_lib.util.op_ml.mlflow", _mlflow_mock()), \
             mock.patch("operator_lib.util.op_ml.MlflowClient", return_value=mock_client), \
             mock.patch("operator_lib.util.op_ml.TrainMlflowLogger", _logger_mock()), \
             mock.patch(
                 "operator_lib.util.helpers.data.read_input_window",
                 return_value=_topics_and_frames(),
             ):
            operator.init(
                kafka_consumer=None,
                kafka_producer=None,
                filter_handler=None,
                output_topic=None,
                pipeline_id="p",
                operator_id="o",
                config=Config({
                    "training_end": "2026-06-01T00:00:00Z",
                    "test_end": "2026-06-01T00:05:00Z",
                    "evaluation_metric": "mae",
                    "evaluation_target_series": "temp",
                    "evaluation_prediction_field": "prediction",
                    "evaluation_resolution": "2m",
                }),
            )

        params = {call.args[1]: call.args[2] for call in mock_client.log_param.call_args_list}
        self.assertEqual("computed", params["evaluation.metric_status"])
        # temp is 1, 3 and 5 at :00, :02 and :04, so the 2m buckets hold
        # means 1, 3 and 5; the five messages at :00 to :04 predict 4 and
        # land in their own buckets: errors 3, 1 and 1 per bucket.
        self.assertEqual(3, params["evaluation.metric_n"])
        self.assertAlmostEqual(5.0 / 3.0, params["evaluation.metric_value"])

    def test_no_test_end_uses_the_registered_model_and_skips_the_evaluation(self):
        mock_mlflow = _mlflow_mock()
        operator = _RecordingOperator()

        with mock.patch("operator_lib.util.op_ml.ray"), \
             mock.patch("operator_lib.util.op_ml.mlflow", mock_mlflow), \
             mock.patch("operator_lib.util.op_ml.TrainMlflowLogger", _logger_mock()), \
             mock.patch(
                 "operator_lib.util.helpers.data.read_input_window"
             ) as mock_read_window:
            operator.init(
                kafka_consumer=None,
                kafka_producer=None,
                filter_handler=None,
                output_topic=None,
                pipeline_id="p",
                operator_id="o",
                config=Config({}),
            )

        self.assertEqual([], operator.train_calls)
        mock_read_window.assert_not_called()
        self.assertEqual("sentinel-registered-model", operator.model)

    def test_test_end_without_training_end_raises(self):
        operator = _RecordingOperator()

        with mock.patch("operator_lib.util.op_ml.ray"), \
             mock.patch("operator_lib.util.op_ml.mlflow", _mlflow_mock()), \
             mock.patch("operator_lib.util.op_ml.TrainMlflowLogger", _logger_mock()):
            with self.assertRaises(MissingConfigValueError):
                operator.init(
                    kafka_consumer=None,
                    kafka_producer=None,
                    filter_handler=None,
                    output_topic=None,
                    pipeline_id="p",
                    operator_id="o",
                    config=Config({"test_end": "2026-06-01T00:05:00Z"}),
                )


class _StrayMetricOperator(MLOperator):
    """
    infer() here plays the part of code the assistant wrote: a bare
    mlflow.log_metric() call during the replay, on test-window data -- the
    exact thing __evaluate's phase boundary (ending the training run,
    unsetting MLFLOW_RUN_ID, before the first infer()) exists to keep out
    of the run ODE reads back to a model.
    """

    STRAY_METRIC_NAME = "leaked_test_window_metric"

    def infer(self, model, data, selector, device_id, timestamp):
        mlflow.log_metric(self.STRAY_METRIC_NAME, 1.0)
        return None, {"seen": True}, None

    def train(self, model, logger):
        return None

    def need_retraining(self, model):
        return False


class _RememberedRunIdOperator(MLOperator):
    """
    The route the phase boundary does *not* close, pinned so that nobody
    assumes it does.

    op.py is imported before init() runs, so operator code can read
    MLFLOW_RUN_ID at module level and keep it. Unsetting the variable before
    the replay takes nothing away from a value already held, and a client call
    with an explicit run id never consults the environment at all -- so this
    write does reach the run ODE reads, under a name the developer declared in
    evaluation.yaml, and the library cannot stop it.

    What the library does instead is make it visible: the write happens after
    operator_lib.training_ended_at, and ODE withholds on that stamp rather than
    on the name. This fixture exists to hold that contract from the producing
    side.
    """

    DECLARED_METRIC_NAME = "rmse"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # As an import-time read would have left it.
        self.remembered_run_id = os.environ.get("MLFLOW_RUN_ID")

    def infer(self, model, data, selector, device_id, timestamp):
        MlflowClient().log_metric(
            self.remembered_run_id, self.DECLARED_METRIC_NAME, 0.01)
        return None, {"seen": True}, None

    def train(self, model, logger):
        return None

    def need_retraining(self, model):
        return False


class _ModelReturningOperator(MLOperator):
    """
    infer() returns a model on its first call, which sends the replay through
    __update_model -- the one path in the loop that logs an artifact and touches
    the registry.

    The ordering fix in __update_model (start the run before tracing into it,
    not after) is what keeps that write off ODE's run, and nothing else in this
    file exercises it: every other fixture returns None for the model.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.updates = 0

    def infer(self, model, data, selector, device_id, timestamp):
        if self.updates == 0:
            self.updates += 1
            return None, {"seen": True}, mlflow.pyfunc.PythonModel()
        return None, {"seen": True}, None

    def train(self, model, logger):
        return None

    def need_retraining(self, model):
        return False


class TestEvaluationIsolatesODEsRun(unittest.TestCase):
    """
    The property the whole restructuring exists for: nothing the replay
    does can land in the run ODE created and later reads back to a model.
    A mock of `mlflow` would only prove op_ml.py calls the right-looking
    functions -- the property under test is precisely the thing such a
    mock would fake -- so this runs against a real, file-backed MLflow
    tracking store instead. Only ray and read_input_window are patched.
    """

    def setUp(self):
        self.addCleanup(clock.set_fixed, None)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracking_uri = "file://" + tmp.name
        # MLOperator.init() calls mlflow.set_experiment(self.model_id); the
        # run created below has to live in that same experiment, or MLflow
        # refuses to resume it ("active experiment ID does not match
        # environment run ID").
        self.experiment_name = "pipeline-p_operator-o"
        previous_uri = mlflow.get_tracking_uri()
        self.addCleanup(mlflow.set_tracking_uri, previous_uri)
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)
        # Registered last so it runs first: cleanups are LIFO, and the run the
        # replay's stray log_metric() opens has to be closed while the tracking
        # URI is still this test's temporary one. MLflow ends a dangling fluent
        # run at interpreter exit instead, by which point the URI above has been
        # restored to the installed default -- which is sqlite:///mlflow.db as of
        # MLflow 3.8.1 -- and the suite leaves a database in the repository root.
        self.addCleanup(lambda: mlflow.active_run() and mlflow.end_run())

    def _odes_run_id(self) -> str:
        """
        Creates and ends a run the way ODE does, then hands it over the way
        a launch does: MLFLOW_RUN_ID in the environment.
        """
        run = mlflow.start_run(run_name="odes-run")
        run_id = run.info.run_id
        mlflow.end_run()
        os.environ["MLFLOW_RUN_ID"] = run_id
        self.addCleanup(os.environ.pop, "MLFLOW_RUN_ID", None)
        return run_id

    def test_a_metric_logged_from_infer_lands_outside_odes_run(self):
        run_id = self._odes_run_id()
        operator = _StrayMetricOperator()
        inputs = _topics_and_frames()

        with mock.patch("operator_lib.util.op_ml.ray"), \
             mock.patch(
                 "operator_lib.util.helpers.data.read_input_window",
                 return_value=inputs,
             ):
            operator.init(
                kafka_consumer=None,
                kafka_producer=None,
                filter_handler=None,
                output_topic=None,
                pipeline_id="p",
                operator_id="o",
                config=Config({
                    "training_end": "2026-06-01T00:00:00Z",
                    "test_end": "2026-06-01T00:05:00Z",
                    "mlflow_url": self.tracking_uri,
                }),
            )

        client = MlflowClient()
        got = client.get_run(run_id)

        # Not asserted as an empty dict: __wrap_training's own trace() calls
        # legitimately log timing metrics (e.g. "timing.train.seconds") onto
        # this same run while it still holds the training pass, before the
        # phase boundary below closes it -- pre-existing behaviour this
        # change does not touch and not what is under test here. What must
        # hold is that nothing the *replay* computed is among them.
        self.assertNotIn(_StrayMetricOperator.STRAY_METRIC_NAME, got.data.metrics)

        self.assertEqual(
            {
                "evaluation.messages": "5",
                "evaluation.results": "5",
                "evaluation.window_start": TRAINING_END.isoformat(),
                "evaluation.window_end": TEST_END.isoformat(),
                "evaluation.metric_status": (
                    "evaluation_metric, evaluation_target_series, "
                    "evaluation_prediction_field, evaluation_resolution not set"
                ),
            },
            got.data.params,
        )

        non_mlflow_tags = {
            k: v for k, v in got.data.tags.items() if not k.startswith("mlflow.")
        }
        self.assertEqual(
            {
                "operator_lib.history_end",
                "operator_lib.test_end",
                "operator_lib.training_ended_at",
            },
            set(non_mlflow_tags),
        )
        self.assertEqual(TRAINING_END.isoformat(), non_mlflow_tags["operator_lib.history_end"])
        self.assertEqual(TEST_END.isoformat(), non_mlflow_tags["operator_lib.test_end"])

        # The stamp separates the two phases on MLflow's own clock: later than
        # every metric the training pass logged -- __wrap_training's trace()
        # timings are real metrics on this run and are what makes this
        # comparison meaningful -- so a filter on it keeps them and drops
        # whatever the replay wrote.
        stamped_at = int(non_mlflow_tags["operator_lib.training_ended_at"])
        training_points = [
            point.timestamp
            for key in got.data.metrics
            for point in client.get_metric_history(run_id, key)
        ]
        self.assertTrue(training_points, "expected the training pass to have logged a metric")
        self.assertLessEqual(max(training_points), stamped_at)

        self.assertEqual("FINISHED", got.info.status)

        artifacts = client.list_artifacts(run_id, "evaluation")
        self.assertEqual(["evaluation/predictions.csv"], [a.path for a in artifacts])

        experiment = mlflow.get_experiment_by_name(self.experiment_name)
        all_runs = client.search_runs([experiment.experiment_id])
        self.assertGreater(len(all_runs), 1)
        other_runs = [r for r in all_runs if r.info.run_id != run_id]
        self.assertTrue(
            any(
                _StrayMetricOperator.STRAY_METRIC_NAME in r.data.metrics
                for r in other_runs
            ),
            "expected the stray metric in a run other than ODE's",
        )

    def test_a_model_returned_during_the_replay_is_registered_outside_odes_run(self):
        """
        __update_model traces through TrainMlflowLogger, whose _ensure_started
        does a fluent start_run(run_id=...). With the old ordering that logger
        still pointed at ODE's run, so a model returned from infer() reopened
        the run the phase boundary had just closed and logged an artifact into
        it. Nothing else in this suite reaches that path.
        """
        run_id = self._odes_run_id()
        operator = _ModelReturningOperator()
        inputs = _topics_and_frames()

        with mock.patch("operator_lib.util.op_ml.ray"), \
             mock.patch(
                 "operator_lib.util.helpers.data.read_input_window",
                 return_value=inputs,
             ):
            operator.init(
                kafka_consumer=None,
                kafka_producer=None,
                filter_handler=None,
                output_topic=None,
                pipeline_id="p",
                operator_id="o",
                config=Config({
                    "training_end": "2026-06-01T00:00:00Z",
                    "test_end": "2026-06-01T00:05:00Z",
                    "mlflow_url": self.tracking_uri,
                }),
            )

        self.assertEqual(1, operator.updates, "expected infer() to have returned a model")

        client = MlflowClient()
        artifacts = [a.path for a in client.list_artifacts(run_id)]
        self.assertEqual(
            ["evaluation"], artifacts,
            "ODE's run must carry the evaluation artifact and nothing the replay "
            "registered")

    def test_a_remembered_run_id_still_reaches_odes_run_but_lands_after_the_stamp(self):
        """
        Not a leak this library closes, and it must not be mistaken for one.

        A client call with a run id read before init() bypasses the phase
        boundary entirely. What has to hold is the fact ODE filters on: the
        write is stamped at or after operator_lib.training_ended_at, while
        everything the training pass logged is stamped before it. A name-based
        check could not tell the two apart -- the metric here carries the name
        a developer would have declared.
        """
        run_id = self._odes_run_id()
        operator = _RememberedRunIdOperator()
        inputs = _topics_and_frames()

        with mock.patch("operator_lib.util.op_ml.ray"), \
             mock.patch(
                 "operator_lib.util.helpers.data.read_input_window",
                 return_value=inputs,
             ):
            operator.init(
                kafka_consumer=None,
                kafka_producer=None,
                filter_handler=None,
                output_topic=None,
                pipeline_id="p",
                operator_id="o",
                config=Config({
                    "training_end": "2026-06-01T00:00:00Z",
                    "test_end": "2026-06-01T00:05:00Z",
                    "mlflow_url": self.tracking_uri,
                }),
            )

        client = MlflowClient()
        got = client.get_run(run_id)
        name = _RememberedRunIdOperator.DECLARED_METRIC_NAME

        # It did reach ODE's run. That is the point of the test.
        self.assertIn(name, got.data.metrics)

        stamped_at = int(got.data.tags["operator_lib.training_ended_at"])
        written = [point.timestamp for point in client.get_metric_history(run_id, name)]
        self.assertTrue(written)
        self.assertGreaterEqual(
            min(written), stamped_at,
            "the replay's write must be stamped at or after the phase transition, "
            "or ODE cannot tell it from a metric the training logged")


def _metric_input_topic(source="device.sensor.ENERGY.Power", dest="power", name="topic1"):
    """
    One input topic with a single mapping. `_source_path` drops the first
    path element of `source`, so with the default `source` this resolves
    against target series "sensor.ENERGY.Power".
    """
    return InputTopic({
        "name": name,
        "filterType": "DeviceId",
        "filterValue": "device-a",
        "mappings": [{"dest": dest, "source": source}],
    })


def _actuals_frame(topic_name, dest, rows):
    """
    A `merged`-shaped frame carrying the target series' own topic: one row
    per (iso timestamp, value) pair in `rows`.
    """
    return pd.DataFrame({
        "time": [
            datetime.datetime.fromisoformat(t.replace("Z", "+00:00")) for t, _ in rows
        ],
        "topic": [topic_name] * len(rows),
        "selector": [None] * len(rows),
        "device_id": ["device-a"] * len(rows),
        dest: [v for _, v in rows],
    })


def _prediction_row(result_time, result: dict):
    return {
        "time": result_time,
        "topic": "topic1",
        "selector": None,
        "device_id": "device-a",
        "result_time": result_time,
        "result": json.dumps(result),
    }


class TestEvaluationMetric(unittest.TestCase):
    """
    Direct tests of _compute_evaluation_metric against hand-built
    prediction_rows/merged/inputs, the same three things __evaluate's finally
    block already holds in memory -- no replay is run here.
    """

    def _config(self, **overrides):
        values = {
            "evaluation_metric": "mae",
            "evaluation_target_series": "sensor.ENERGY.Power",
            "evaluation_prediction_field": "prediction",
            "evaluation_resolution": "1h",
        }
        values.update(overrides)
        return Config(values)

    def test_mae_matches_a_hand_computed_value(self):
        inputs = [(_metric_input_topic(), None)]
        merged = _actuals_frame("topic1", "power", [
            ("2026-06-01T00:10:00Z", 10.0),
            ("2026-06-01T00:50:00Z", 20.0),
        ])  # bucket [00:00, 01:00) actual mean = 15.0
        prediction_rows = [
            _prediction_row("2026-06-01T00:05:00Z", {"prediction": 17.0}),  # |17-15|=2
            _prediction_row("2026-06-01T00:55:00Z", {"prediction": 13.0}),  # |13-15|=2
        ]

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="mae"), prediction_rows, merged, inputs)

        self.assertEqual("computed", status)
        self.assertEqual("mae", name)
        self.assertAlmostEqual(2.0, value)
        self.assertEqual(1, n)  # both predictions fall into one bucket

    def test_rmse_matches_a_hand_computed_value(self):
        inputs = [(_metric_input_topic(), None)]
        merged = _actuals_frame("topic1", "power", [
            ("2026-06-01T00:10:00Z", 10.0),
            ("2026-06-01T00:50:00Z", 20.0),
        ])  # bucket [00:00, 01:00) actual mean = 15.0
        prediction_rows = [
            _prediction_row("2026-06-01T00:05:00Z", {"prediction": 17.0}),  # error 2
            _prediction_row("2026-06-01T00:55:00Z", {"prediction": 11.0}),  # error -4
        ]

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="rmse"), prediction_rows, merged, inputs)

        self.assertEqual("computed", status)
        self.assertEqual("rmse", name)
        self.assertAlmostEqual(((2.0 ** 2 + 4.0 ** 2) / 2) ** 0.5, value)
        self.assertEqual(1, n)  # both predictions fall into one bucket

    def _unevenly_predicted_buckets(self):
        """
        Two buckets with very different prediction counts: three predictions
        with error 1 in [00:00, 01:00), one with error 5 in [01:00, 02:00) --
        the shape of a replay whose message rate differs between hours.
        """
        inputs = [(_metric_input_topic(), None)]
        merged = _actuals_frame("topic1", "power", [
            ("2026-06-01T00:10:00Z", 10.0),
            ("2026-06-01T01:10:00Z", 10.0),
        ])
        prediction_rows = [
            _prediction_row("2026-06-01T00:05:00Z", {"prediction": 11.0}),
            _prediction_row("2026-06-01T00:25:00Z", {"prediction": 11.0}),
            _prediction_row("2026-06-01T00:45:00Z", {"prediction": 11.0}),
            _prediction_row("2026-06-01T01:05:00Z", {"prediction": 15.0}),
        ]
        return inputs, merged, prediction_rows

    def test_mae_weights_every_bucket_equally_whatever_its_prediction_count(self):
        inputs, merged, prediction_rows = self._unevenly_predicted_buckets()

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="mae"), prediction_rows, merged, inputs)

        self.assertEqual("computed", status)
        # (1 + 5) / 2, not the pooled (1 + 1 + 1 + 5) / 4.
        self.assertAlmostEqual(3.0, value)
        self.assertEqual(2, n)

    def test_rmse_weights_every_bucket_equally_whatever_its_prediction_count(self):
        inputs, merged, prediction_rows = self._unevenly_predicted_buckets()

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="rmse"), prediction_rows, merged, inputs)

        self.assertEqual("computed", status)
        # sqrt((1 + 25) / 2), not the pooled sqrt((1 + 1 + 1 + 25) / 4).
        self.assertAlmostEqual(13.0 ** 0.5, value)
        self.assertEqual(2, n)

    def test_bucket_is_assigned_by_truncation_not_rounding(self):
        inputs = [(_metric_input_topic(), None)]
        # Only the [00:00, 01:00) bucket has an actual; [01:00, 02:00) has
        # none. A prediction one second before the hour must still land in
        # the earlier bucket -- rounding to the nearest hour would put it in
        # the empty one instead and drop it.
        merged = _actuals_frame("topic1", "power", [
            ("2026-06-01T00:01:00Z", 100.0),
        ])
        prediction_rows = [
            _prediction_row("2026-06-01T00:59:59Z", {"prediction": 110.0}),
        ]

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="mae"), prediction_rows, merged, inputs)

        self.assertEqual("computed", status)
        self.assertEqual(1, n)
        self.assertAlmostEqual(10.0, value)

    def test_a_prediction_without_a_matching_actual_is_excluded(self):
        inputs = [(_metric_input_topic(), None)]
        # Actual data only exists for the first hour -- the usual shape at
        # the tail of the test window, where result_time runs past test_end.
        merged = _actuals_frame("topic1", "power", [
            ("2026-06-01T00:10:00Z", 50.0),
        ])
        prediction_rows = [
            _prediction_row("2026-06-01T00:05:00Z", {"prediction": 55.0}),  # matches, error 5
            _prediction_row("2026-06-01T02:00:00Z", {"prediction": 999.0}),  # no actual, excluded
        ]

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="mae"), prediction_rows, merged, inputs)

        self.assertEqual("computed", status)
        self.assertEqual(1, n)
        self.assertAlmostEqual(5.0, value)

    def test_a_row_missing_the_prediction_field_is_excluded(self):
        inputs = [(_metric_input_topic(), None)]
        merged = _actuals_frame("topic1", "power", [
            ("2026-06-01T00:10:00Z", 10.0),
            ("2026-06-01T00:50:00Z", 20.0),
        ])  # bucket mean = 15.0
        prediction_rows = [
            _prediction_row("2026-06-01T00:05:00Z", {"prediction": 17.0}),  # counted
            _prediction_row("2026-06-01T00:10:00Z", {"other_field": 999.0}),  # field missing
        ]

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="mae"), prediction_rows, merged, inputs)

        self.assertEqual("computed", status)
        self.assertEqual(1, n)
        self.assertAlmostEqual(2.0, value)

    def test_an_unknown_metric_name_is_not_computed(self):
        inputs = [(_metric_input_topic(), None)]
        merged = _actuals_frame("topic1", "power", [("2026-06-01T00:10:00Z", 10.0)])
        prediction_rows = [_prediction_row("2026-06-01T00:05:00Z", {"prediction": 1.0})]

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_metric="mape"), prediction_rows, merged, inputs)

        self.assertEqual("unknown metric 'mape'", status)
        self.assertIsNone(name)
        self.assertIsNone(value)
        self.assertIsNone(n)

    def test_an_unresolvable_target_series_is_not_computed(self):
        inputs = [(_metric_input_topic(), None)]
        merged = _actuals_frame("topic1", "power", [("2026-06-01T00:10:00Z", 10.0)])
        prediction_rows = [_prediction_row("2026-06-01T00:05:00Z", {"prediction": 1.0})]

        status, name, value, n = _compute_evaluation_metric(
            self._config(evaluation_target_series="sensor.DOES.NOT.EXIST"),
            prediction_rows, merged, inputs)

        self.assertEqual(
            "target series 'sensor.DOES.NOT.EXIST' matches no input topic", status)
        self.assertIsNone(name)
        self.assertIsNone(value)
        self.assertIsNone(n)

    def test_a_target_series_matching_more_than_one_mapping_is_not_computed(self):
        # Two different topics whose mapping sources both resolve, after
        # dropping the first path element, to the same platform path.
        inputs = [
            (_metric_input_topic(source="device-a.sensor.ENERGY.Power", dest="power", name="topic1"), None),
            (_metric_input_topic(source="device-b.sensor.ENERGY.Power", dest="p2", name="topic2"), None),
        ]
        merged = _actuals_frame("topic1", "power", [("2026-06-01T00:10:00Z", 10.0)])
        prediction_rows = [_prediction_row("2026-06-01T00:05:00Z", {"prediction": 1.0})]

        status, name, value, n = _compute_evaluation_metric(
            self._config(), prediction_rows, merged, inputs)

        self.assertEqual(
            "target series 'sensor.ENERGY.Power' matches 2 mappings, expected exactly one",
            status)
        self.assertIsNone(name)
        self.assertIsNone(value)
        self.assertIsNone(n)

    def test_metric_n_of_zero_is_not_computed(self):
        inputs = [(_metric_input_topic(), None)]
        # Actual data exists, but in a bucket the one prediction never falls
        # into, so nothing is ever paired.
        merged = _actuals_frame("topic1", "power", [("2026-06-01T05:00:00Z", 10.0)])
        prediction_rows = [_prediction_row("2026-06-01T00:05:00Z", {"prediction": 1.0})]

        status, name, value, n = _compute_evaluation_metric(
            self._config(), prediction_rows, merged, inputs)

        self.assertEqual("no prediction has a matching actual value", status)
        self.assertIsNone(name)
        self.assertIsNone(value)
        self.assertIsNone(n)

    def test_missing_config_names_every_unset_key(self):
        inputs = [(_metric_input_topic(), None)]
        merged = _actuals_frame("topic1", "power", [("2026-06-01T00:10:00Z", 10.0)])

        status, name, value, n = _compute_evaluation_metric(
            Config({}), [], merged, inputs)

        self.assertEqual(
            "evaluation_metric, evaluation_target_series, "
            "evaluation_prediction_field, evaluation_resolution not set",
            status)
        self.assertIsNone(name)
        self.assertIsNone(value)
        self.assertIsNone(n)


if __name__ == "__main__":
    unittest.main()
