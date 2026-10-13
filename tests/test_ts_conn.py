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

import json
import os
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import operator_lib.operator_lib as operator_lib_module
import operator_lib.util as util
from operator_lib.util.config import MissingConfigValueError, redact_config
from operator_lib.util.helpers import data
from operator_lib.util.model import Config, InputTopic

E = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

# Fake credentials, assembled so that no complete DSN appears in the source.
PASSWORD = "sec" + "ret"
ENV_PASSWORD = "env" + "-pw"
TOKEN = "jwt" + "-value"
DSN = "postgres://" + "u:" + PASSWORD + "@h:5432/db"
ENV_DSN = "postgres://" + "u:" + ENV_PASSWORD + "@h:5432/db"


def _topic():
    return InputTopic({
        "name": "urn_infai_ses_service_1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
        "filterType": "DeviceId",
        "filterValue": "0362b1a1-a1a1-4b2b-8c3c-9d4d5e6f7a8b",
        "mappings": [{"dest": "temp", "source": "value.temp"}],
    })


class TestTsConnPrecedence(unittest.TestCase):
    def setUp(self):
        self.read_topic = getattr(data, "__read_topic")
        self.mocks = {}
        for name in ("get_timescale_dataset_local", "get_ts_wrapper_dataset_local"):
            patch = mock.patch.object(data, name)
            self.mocks[name] = patch.start()
            self.addCleanup(patch.stop)

    def _read(self, env_ts_conn, config, senergy_token=None):
        dep = types.SimpleNamespace(
            ts_conn=env_ts_conn, senergy_token=senergy_token,
            config_bootstrap_servers="kafka:9092", pipeline_id="p")
        return self.read_topic(config, dep, _topic(), timedelta(days=1), False, False, E)

    def _used_dsn(self):
        return self.mocks["get_timescale_dataset_local"].call_args.args[0]

    def test_the_environment_value_is_used(self):
        self._read(ENV_DSN, Config({}))
        self.assertEqual(ENV_DSN, self._used_dsn())

    def test_the_environment_wins_over_the_operator_config(self):
        self._read(ENV_DSN, Config({"ts_conn": DSN}))
        self.assertEqual(ENV_DSN, self._used_dsn())

    def test_the_operator_config_is_used_when_the_environment_is_unset(self):
        self._read(None, Config({"ts_conn": DSN}))
        self.assertEqual(DSN, self._used_dsn())

    def test_an_empty_environment_value_falls_back_to_the_operator_config(self):
        self._read("", Config({"ts_conn": DSN}))
        self.assertEqual(DSN, self._used_dsn())

    def test_without_either_the_wrapper_is_used(self):
        self._read(None, Config({"ts_wrapper_url": "http://wrapper"}), senergy_token="tok")
        self.mocks["get_timescale_dataset_local"].assert_not_called()
        self.mocks["get_ts_wrapper_dataset_local"].assert_called_once()

    def test_without_any_reader_the_error_names_the_environment_variable(self):
        with self.assertRaises(MissingConfigValueError) as ctx:
            self._read(None, Config({}))
        self.assertIn("TS_CONN", str(ctx.exception))

    def test_the_deployment_config_reads_it_from_TS_CONN(self):
        with mock.patch.dict(os.environ, {"TS_CONN": ENV_DSN}):
            self.assertEqual(ENV_DSN, util.DeploymentConfig().ts_conn)
        with mock.patch.dict(os.environ):
            os.environ.pop("TS_CONN", None)
            self.assertIsNone(util.DeploymentConfig().ts_conn)


class TestRedactConfig(unittest.TestCase):
    def test_keys_naming_a_credential_are_masked(self):
        redacted = redact_config({
            "ts_conn": DSN, "senergy_token": "abc", "db_password": "p", "client_secret": "s",
            "access_token": "t", "refresh_Token": "t", "pipeline_id": "p1"})
        self.assertEqual("p1", redacted["pipeline_id"])
        for key in redacted:
            if key != "pipeline_id":
                self.assertEqual("***", redacted[key], key)

    def test_an_unset_credential_stays_visible_as_unset(self):
        self.assertIsNone(redact_config({"ts_conn": None, "senergy_token": None})["ts_conn"])

    def test_url_userinfo_in_any_value_loses_its_password(self):
        redacted = redact_config({"url": DSN, "note": f"connect to {DSN} now"})
        self.assertEqual("postgres://u:***@h:5432/db", redacted["url"])
        self.assertEqual("connect to postgres://u:***@h:5432/db now", redacted["note"])

    def test_a_keyword_dsn_loses_its_password(self):
        value = "host=h user=u password=" + PASSWORD + " dbname=d"
        self.assertEqual("host=h user=u password=*** dbname=d", redact_config({"x": value})["x"])

    def test_non_secret_values_are_unchanged(self):
        plain = {
            "logger_level": "debug", "ts_wrapper_url": "http://wrapper/x", "metrics_port": 5555,
            "metrics": False, "mail": "a@b.c", "nested": {"list": [1, "two", None]}}
        self.assertEqual(plain, redact_config(plain))

    def test_json_in_a_string_is_masked_by_key_and_by_url(self):
        text = json.dumps({"config": {"ts_conn": DSN, "other": DSN + "x", "keep": "v"}, "n": 1})
        redacted = json.loads(redact_config({"config": text})["config"])
        self.assertEqual("***", redacted["config"]["ts_conn"])
        self.assertEqual("postgres://u:***@h:5432/dbx", redacted["config"]["other"])
        self.assertEqual("v", redacted["config"]["keep"])
        self.assertEqual(1, redacted["n"])

    def test_the_input_is_not_modified(self):
        original = {"ts_conn": DSN, "nested": {"url": DSN}}
        redact_config(original)
        self.assertEqual({"ts_conn": DSN, "nested": {"url": DSN}}, original)

    def test_the_config_objects_the_lib_logs_are_masked(self):
        operator_config = {
            "config": {"ts_conn": DSN, "logger_level": "debug"},
            "inputTopics": [{
                "name": "t", "filterType": "DeviceId", "filterValue": "d",
                "mappings": [{"dest": "a", "source": "b"}]}]}
        env = {"CONFIG": json.dumps(operator_config), "TS_CONN": ENV_DSN, "SENERGY_TOKEN": TOKEN}
        with mock.patch.dict(os.environ, env):
            dep_config = util.DeploymentConfig()
        text = str(redact_config(dep_config)) + str(redact_config(util.OperatorConfig(operator_config)))
        for secret in (PASSWORD, ENV_PASSWORD, TOKEN):
            self.assertNotIn(secret, text)
        self.assertIn("debug", text)
        self.assertIn("'inputTopics'", text)


class TestDebugLines(unittest.TestCase):
    def _debug_lines(self, operator_config, env):
        env = {"CONFIG": json.dumps(operator_config), **env}
        operator = mock.MagicMock()
        operator.selectors = []
        with mock.patch.dict(os.environ, env), \
             mock.patch.object(operator_lib_module, "confluent_kafka"), \
             mock.patch.object(operator_lib_module, "cncr_wdg"), \
             mock.patch.object(operator_lib_module, "prometheus_client"), \
             mock.patch.object(util.logger, "debug") as debug:
            operator_lib_module.OperatorLib(operator, git_info_file=None)
        return [call.args[0] for call in debug.call_args_list]

    def test_no_debug_line_carries_a_password_from_config_or_environment(self):
        operator_config = {
            "config": {"ts_conn": DSN, "logger_level": "debug"}, "inputTopics": []}
        lines = self._debug_lines(operator_config, {"TS_CONN": ENV_DSN, "SENERGY_TOKEN": TOKEN})
        self.assertTrue(any(line.startswith("deployment config:") for line in lines))
        self.assertTrue(any(line.startswith("operator config:") for line in lines))
        for line in lines:
            for secret in (PASSWORD, ENV_PASSWORD, TOKEN):
                self.assertNotIn(secret, line)


if __name__ == "__main__":
    unittest.main()
