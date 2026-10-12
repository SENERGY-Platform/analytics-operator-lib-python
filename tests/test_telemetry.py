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
import subprocess
import sys
import unittest
from unittest import mock

from operator_lib.util import op_ml

# MLflow switches telemetry off by itself in a CI or test environment; the child runs without them.
_CI_VARIABLES = ("PYTEST_CURRENT_TEST", "GITHUB_ACTIONS", "CI", "CIRCLECI", "JENKINS_URL", "TRAVIS",
                 "BUILDKITE", "GITLAB_CI", "MLFLOW_DISABLE_TELEMETRY", "DO_NOT_TRACK")

_PROBE = ("import operator_lib.util\n"
          "from mlflow.telemetry import get_telemetry_client\n"
          "print(get_telemetry_client() is None)\n")


class TestTelemetry(unittest.TestCase):
    def test_importing_the_lib_leaves_mlflow_without_a_telemetry_client(self):
        env = {k: v for k, v in os.environ.items() if k not in _CI_VARIABLES}
        out = subprocess.run([sys.executable, "-c", _PROBE], env=env, capture_output=True, text=True, check=True)
        self.assertEqual(out.stdout.strip().splitlines()[-1], "True")

    def test_workers_get_the_setting(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MLFLOW_DISABLE_TELEMETRY", None)
            self.assertEqual(op_ml._runtime_env(), {"env_vars": {"MLFLOW_DISABLE_TELEMETRY": "true"}})

    def test_an_operator_can_switch_it_on_again(self):
        with mock.patch.dict(os.environ, {"MLFLOW_DISABLE_TELEMETRY": "false"}):
            self.assertEqual(op_ml._runtime_env(), {"env_vars": {"MLFLOW_DISABLE_TELEMETRY": "false"}})


if __name__ == '__main__':
    unittest.main()
