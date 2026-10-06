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

import unittest

from operator_lib.util import Config


class CustomConfig(Config):
    retrain_after_s = 86400


class NarrowerConfig(CustomConfig):
    retrain_after_s = 3600
    logger_level = "debug"


SPLIT = {
    "training_end": "2026-09-01T00:00:00Z",
    "test_end": "2026-10-01T00:00:00Z",
    "evaluation_metric": "mae",
    "evaluation_target_series": "sensor.ENERGY.Power",
    "evaluation_prediction_field": "prediction",
    "evaluation_resolution": "1h",
}


class TestConfigSubclass(unittest.TestCase):
    def test_a_subclass_with_fields_of_its_own_receives_the_inherited_ones(self):
        config = CustomConfig({**SPLIT, "ts_wrapper_url": "http://wrapper", "retrain_after_s": 60})
        for name, value in SPLIT.items():
            self.assertEqual(value, getattr(config, name), name)
        self.assertEqual("http://wrapper", config.ts_wrapper_url)
        self.assertEqual(60, config.retrain_after_s)

    def test_a_subclass_keeps_the_inherited_defaults_without_a_config(self):
        config = CustomConfig({})
        self.assertIsNone(config.training_end)
        self.assertIsNone(config.test_end)
        self.assertEqual("warning", config.logger_level)
        self.assertEqual(86400, config.retrain_after_s)

    def test_the_nearest_class_wins_over_an_inherited_default(self):
        config = NarrowerConfig({"test_end": SPLIT["test_end"]})
        self.assertEqual(3600, config.retrain_after_s)
        self.assertEqual("debug", config.logger_level)
        self.assertEqual(SPLIT["test_end"], config.test_end)

    def test_an_inherited_field_can_be_assigned_on_an_instance(self):
        config = CustomConfig({})
        config.test_end = SPLIT["test_end"]
        self.assertEqual(SPLIT["test_end"], config.test_end)

    def test_the_base_config_is_unchanged(self):
        config = Config(SPLIT)
        self.assertEqual(SPLIT["training_end"], config.training_end)
        self.assertFalse(hasattr(Config, "retrain_after_s"))


if __name__ == "__main__":
    unittest.main()
