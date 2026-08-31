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

__all__ = (
    "DeploymentConfig",
    "MissingDeploymentConfigError",
    "load_operator_config_json",
)

import json

import sevm


class DeploymentConfig(sevm.Config):
    pipeline_id = None
    operator_id = None
    config = None
    consumer_auto_offset_reset_config = None
    output = None
    config_bootstrap_servers = None
    zk_quorum = None
    config_application_id = None
    device_id_path = None
    window_time = None
    zk_brokers_path = "/brokers/ids"
    metrics = False
    metrics_port = 5555


class MissingDeploymentConfigError(RuntimeError):
    def __init__(self):
        super().__init__(
            "no deployment configuration: CONFIG is not set, this operator was not started by the flow engine"
        )


def load_operator_config_json(dep_config: DeploymentConfig) -> dict:
    """
    Parse the operator configuration the flow engine hands over in CONFIG.
    """
    if dep_config.config is None:
        raise MissingDeploymentConfigError()
    return json.loads(dep_config.config)
