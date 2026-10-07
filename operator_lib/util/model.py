"""
   Copyright 2022 InfAI (CC SES)

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

__all__ = ("OperatorConfig", "Config", "Selector")

import simple_struct
import typing
import os


class Selector(simple_struct.Structure):
    name: str = None
    args: typing.Set[str] = None

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        self.args = set(self.args)


class Config(simple_struct.Structure):
    logger_level = "warning"
    mlflow_url = "http://mlflow-svc.mlflow.svc.cluster.local:5000"
    ray_url = "ray://cluster-kuberay-head-svc.ray.svc.cluster.local:10001"
    # No default. This is a database credential reaching every series in the
    # instance, and a compiled-in one is how every operator got there without
    # anybody configuring it. The flow engine sets it per deployment; an
    # environment that runs untrusted code sets ts_wrapper_url instead and hands
    # the operator no credential at all.
    ts_conn = None
    # Where history is read through timescale-wrapper, which checks the caller's
    # own permission on the device. Used when ts_conn is absent and a platform
    # token is present.
    ts_wrapper_url = None
    # Both ISO 8601 UTC strings, set by an Operator Development Environment
    # launch that carries a data split: training_end is the bound every history
    # read applies (see operator_lib.util.clock), test_end triggers the
    # evaluation phase in MLOperator.init(). The flow engine sets neither, so a
    # deployed operator is unchanged. simple_struct.Structure reads only
    # declared class attributes, so a library older than this one silently
    # ignores both instead of failing to parse the config.
    training_end = None
    test_end = None
    # Set together with test_end by an Operator Development Environment launch
    # whose evaluation protocol has frozen a target series, an output field and
    # a horizon to score against -- the launch reads them from evaluation.yaml
    # at the commit it deploys. All four are independent of test_end's own
    # presence and of each other: MLOperator.__evaluate() computes a metric
    # only once every one of them is set and evaluation_metric names one it
    # knows, and otherwise logs why it did not, exactly as a launch that has
    # not frozen a scoring target -- or an older ODE that predates this
    # entirely -- already behaves without them.
    #
    # evaluation_target_series is a platform path (e.g. "sensor.ENERGY.Power"),
    # resolved against an input topic mapping's `source`, not its `dest` --
    # the operator author names `dest` freely, so only `source` is the
    # platform's own identity for the series.
    evaluation_metric = None
    evaluation_target_series = None
    evaluation_prediction_field = None
    evaluation_resolution = None
    # A JSON-encoded string (the flow engine's config values are all strings):
    # a list of {topic, import_id, export_id, table, columns}, one per import
    # input whose history the deployer resolved to an analytics-serving export.
    # The deployer checks the user's permission on the export, so the operator
    # reads whatever is named here without checking again. An import topic
    # without an entry is read from Kafka, as before; see
    # operator_lib.util.helpers.exports.
    import_exports = None

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)

    # simple_struct.Structure parses, and lets an instance assign, only the
    # attributes in its class's own __dict__. An operator's `class
    # CustomConfig(Config)` therefore left every field above at its default,
    # whatever the deployment config said -- training_end and test_end
    # included, so a data split never reached init(). Copying the inherited
    # fields into each subclass makes them declared there too; the nearest
    # class wins, so a subclass can still override a default.
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for base in cls.__mro__[1:]:
            if not issubclass(base, Config):
                continue
            for name, value in vars(base).items():
                if not name.startswith("_") and name not in vars(cls):
                    setattr(cls, name, value)


class Mapping(simple_struct.Structure):
    dest: str = None
    source: str = None


class InputTopic(simple_struct.Structure):
    name: str = None
    filterType: str = None
    filterValue: str = None
    mappings: typing.List[Mapping] = None

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        self.mappings = [Mapping(m) for m in self.mappings]


class OperatorConfig(simple_struct.Structure):
    config = Config
    inputTopics: typing.List[InputTopic] = None

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)
        self.inputTopics = [InputTopic(it) for it in self.inputTopics]
