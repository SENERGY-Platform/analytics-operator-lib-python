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

import os
import unittest
from unittest import mock

import mlflow

from operator_lib.util import Config, MLOperator

MODEL_ID = "pipeline-p_operator-o"


class _Operator(MLOperator):
    def infer(self, model, data, selector, device_id, timestamp):
        return None, None, None

    def train(self, model, logger):
        return None

    def need_retraining(self, model) -> bool:
        return False


def _start_run(env):
    """Call MLOperator.__start_run under `env` and return the run_name it passed."""
    operator = _Operator()
    operator.config = Config({})
    operator.model_id = MODEL_ID
    with mock.patch.dict(os.environ, env, clear=False):
        os.environ.pop("MLFLOW_RUN_ID", None)
        os.environ.update(env)
        with mock.patch.object(mlflow, "start_run") as start_run:
            operator._MLOperator__start_run()
    return start_run.call_args.kwargs["run_name"]


class TestStartRun(unittest.TestCase):
    def test_a_handed_over_run_keeps_its_name(self):
        # A development run is created and named by whoever submits the job, which
        # hands it over in MLFLOW_RUN_ID. mlflow's fluent start_run resumes that run
        # and forwards run_name to update_run_info, so a name offered here would
        # overwrite the caller's -- passing None is what leaves it alone.
        self.assertIsNone(_start_run({"MLFLOW_RUN_ID": "b02f3e403a184bfeb2457ba17011b430"}))

    def test_a_deployment_names_its_own_run(self):
        # Nothing hands a deployed operator a run, so it opens one and names it
        # after the model and the moment.
        run_name = _start_run({})
        self.assertIsNotNone(run_name)
        self.assertTrue(run_name.startswith(MODEL_ID + "@"), run_name)


if __name__ == "__main__":
    unittest.main()
