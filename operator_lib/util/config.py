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
    "MissingConfigValueError",
    "load_operator_config_json",
    "require_config",
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
    # The platform token the operator reads history with where it has no database
    # credential. An environment variable rather than part of CONFIG: it is a
    # credential with its own lifetime, and it is renewed independently of the
    # configuration a deployment was started with.
    senergy_token = None
    # The OpenTelemetry baggage of the request that started this pipeline, as a W3C
    # baggage header. Put into every log record so a line from this operator can be
    # traced back to the caller's context, for instance to a smart service instance.
    #
    # An environment variable rather than part of CONFIG, because it describes the
    # deployment rather than the operator, and because the same value is what the
    # pod labels carry for the log aggregation.
    baggage = None


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


class MissingConfigValueError(RuntimeError):
    def __init__(self, message: str):
        super().__init__(message)


def require_config(value, name: str, purpose: str):
    """
    Return a config value or fail with a named error, rather than letting a None
    reach whatever would have used it.
    """
    if not value:
        raise MissingConfigValueError(
            f"config value '{name}' is not set, it is required to {purpose}")
    return value
