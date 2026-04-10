import ray
from ray.runtime_env import RuntimeEnv
import operator_lib.util as util
import json

_ray_initialized = False

def init_ray_once(): # TODO: provide additional config via operator config defaults
    """Initialize Ray once, regardless of how many times this function is called."""
    
    global _ray_initialized
    if not _ray_initialized:
        dep_config = util.DeploymentConfig()
        config_json = json.loads(dep_config.config)
        opr_config = util.OperatorConfig(config_json)
        
        ray_runtime_env = opr_config.config.ray_runtime_env
        ray.init(address=opr_config.config.ray_url, runtime_env=RuntimeEnv(**ray_runtime_env))
        _ray_initialized = True