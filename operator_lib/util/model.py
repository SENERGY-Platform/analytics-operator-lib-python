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
    """
        TODO 
        "uv": [
            "git+https://github.com/SENERGY-Platform/analytics-operator-lib-python.git@d7f6fe87f7f79ce27b3271679e1ae526043dcad7",
            "kafka-python[snappy]",
        ],
    """
    ray_runtime_env = {
        "config": {
            "setup_timeout_seconds": 30 * 60,
        },
        "py_executable": "uv run",
    }
    try:
        ray_runtime_env["working_dir"] = os.path.abspath(os.getcwd())
    except FileNotFoundError:
        pass # Happens in ray remote environments
    ts_conn = "postgresql://postgres:tea@timescale-db.timescale.svc.cluster.local/postgres"

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)


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
