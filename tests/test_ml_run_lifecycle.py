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
import os
import tempfile
import unittest
from unittest import mock

import mlflow
from mlflow import MlflowClient

from operator_lib.util import clock
from operator_lib.util.model import Config
from operator_lib.util.op_ml import MLOperator

MODEL_ID = "pipeline-p_operator-o"
NOW = datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc)


class _Operator(MLOperator):
    """Returns whatever the test puts into `infer_model`, `train_model` and `retrain`."""

    infer_model = None
    train_model = None
    retrain = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trainings = 0

    def infer(self, model, data, selector, device_id, timestamp):
        return None, None, self.infer_model

    def train(self, model, logger):
        self.trainings += 1
        return self.train_model

    def need_retraining(self, model):
        return self.retrain


class TestDeploymentRunLifecycle(unittest.TestCase):
    """The deployment path of MLOperator (no test_end) against a real, file-backed MLflow store."""

    def setUp(self):
        self.addCleanup(clock.set_fixed, None)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tracking_uri = "file://" + tmp.name
        previous_uri = mlflow.get_tracking_uri()
        self.addCleanup(mlflow.set_tracking_uri, previous_uri)
        os.environ.pop("MLFLOW_RUN_ID", None)
        # Runs before the URI is restored, so a run left open is closed in this store.
        self.addCleanup(lambda: mlflow.active_run() and mlflow.end_run())
        ray = mock.patch("operator_lib.util.op_ml.ray")
        ray.start()
        self.addCleanup(ray.stop)

    def _init(self, operator):
        operator.init(
            kafka_consumer=None,
            kafka_producer=None,
            filter_handler=None,
            output_topic=None,
            pipeline_id="p",
            operator_id="o",
            config=Config({"mlflow_url": self.tracking_uri}),
        )

    def _register_model(self):
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(MODEL_ID)
        with mlflow.start_run():
            logged = mlflow.pyfunc.log_model(
                artifact_path=MODEL_ID, python_model=mlflow.pyfunc.PythonModel())
        version = mlflow.register_model(logged.model_uri, MODEL_ID)
        MlflowClient().set_registered_model_alias(MODEL_ID, "production", version.version)
        return version.version

    def _production_version(self):
        return MlflowClient().get_model_version_by_alias(MODEL_ID, "production").version

    def test_a_model_from_infer_replaces_a_loaded_model(self):
        # init() loads the registered model and never trains, so nothing has
        # started a run when run() hands the new model to __update_model.
        loaded_version = self._register_model()
        operator = _Operator()
        self._init(operator)
        self.assertEqual(0, operator.trainings)

        operator.infer_model = mlflow.pyfunc.PythonModel()
        operator.run({}, "selector", "device", NOW)

        self.assertNotEqual(loaded_version, self._production_version())
        self.assertIsNone(mlflow.active_run())

    def test_a_training_without_a_model_does_not_block_the_next(self):
        operator = _Operator()
        self._init(operator)
        self.assertEqual(1, operator.trainings)
        self.assertIsNone(mlflow.active_run())

        operator.retrain = True
        operator.train_model = mlflow.pyfunc.PythonModel()
        operator.run({}, "selector", "device", NOW)

        self.assertEqual(2, operator.trainings)
        self.assertIsNotNone(self._production_version())
        self.assertIsNone(mlflow.active_run())

    def test_a_failed_training_does_not_block_the_next(self):
        operator = _Operator()
        self._init(operator)

        operator.retrain = True
        with mock.patch.object(_Operator, "train", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                operator.run({}, "selector", "device", NOW)
        self.assertIsNone(mlflow.active_run())

        operator.run({}, "selector", "device", NOW)
        self.assertEqual(2, operator.trainings)


if __name__ == "__main__":
    unittest.main()
