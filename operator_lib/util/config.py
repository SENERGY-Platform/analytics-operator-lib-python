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
    "redact_config",
    "require_config",
)

import json
import re

import sevm
import simple_struct


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
    # The Timescale DSN, from a Kubernetes Secret, because in CONFIG anyone who can
    # read the pod reads the password. Wins over 'ts_conn' in the operator config.
    ts_conn = None
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


_SECRET_KEYS = ("ts_conn", "senergy_token")
_SECRET_KEY_PARTS = ("password", "secret", "token")
_MASK = "***"
_URL_USERINFO = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)(?P<user>[^\s:/@\"']*):[^\s/\"']*@")
_KEYWORD_PASSWORD = re.compile(r"(?i)(\bpassword\s*=\s*)('(?:[^'\\]|\\.)*'|[^\s\"',}]+)")


def redact_config(value):
    """
    Return a printable copy of a config with credentials masked, for logging.

    Masked are the values of 'ts_conn', 'senergy_token' and any key containing
    'password', 'secret' or 'token', and the password of a URL userinfo or a
    'password=' pair inside any other string. A string holding JSON, such as
    CONFIG, is masked by key as well.
    """
    return _redact(value, None)


def _redact(value, key):
    if value is not None and key is not None:
        lowered = str(key).lower()
        if lowered in _SECRET_KEYS or any(part in lowered for part in _SECRET_KEY_PARTS):
            return _MASK
    if hasattr(value, "__todict__"):
        value = value.__todict__()
    elif isinstance(value, simple_struct.Structure):
        value = dict(value)
    if isinstance(value, dict):
        return {k: _redact(v, k) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_redact(v, None) for v in value]
    if isinstance(value, str):
        return _redact_string(value)
    return value


def _redact_string(value: str) -> str:
    if value.lstrip()[:1] in ("{", "["):
        try:
            return json.dumps(_redact(json.loads(value), None))
        except ValueError:
            pass
    value = _URL_USERINFO.sub(lambda m: f"{m.group('scheme')}{m.group('user')}:{_MASK}@", value)
    return _KEYWORD_PASSWORD.sub(lambda m: m.group(1) + _MASK, value)
