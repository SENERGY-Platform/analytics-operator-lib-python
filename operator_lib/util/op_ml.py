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

__all__ = ("MLOperator",)

from .ray_helpers.mlflow_logger import TrainMlflowLoggerCallback

from .op_base import OperatorBase

import typing
import datetime
import typing
import abc
import mlflow
from mlflow import MlflowClient
from mlflow.pyfunc import PyFuncModel, PythonModel
import datetime
import ray
from ray.runtime_env import RuntimeEnv

import mlflow

class MLOperator(OperatorBase):
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        ray.shutdown()
        mlflow.set_tracking_uri(self.config.mlflow_url)
        self.model_id = f"pipeline-{self.get_pipeline_id()}_operator-{self.get_operator_id()}"
        mlflow.set_experiment(self.model_id)
        model = self.load_model()
        if model is None:
            self.__wrap_training()
            

    def update_model(self, model: PythonModel):
        if self.__run is None:
            self.__start_run()

        # Create a new model version and save model
       
        # mlflow.log_metrics(metrics) TODO
        # mlflow.log_params(config) TODO

        new_model = mlflow.pyfunc.log_model(
            artifact_path=self.model_id,
            python_model=model,
            # signature=signature TODO
        )

        created_model_version = mlflow.register_model(new_model.model_uri, self.model_id)
        client = MlflowClient()
        client.set_registered_model_alias(
            self.model_id, "production", created_model_version.version)
        self.model = mlflow.pyfunc.load_model(
            f"models:/{self.model_id}@production")
        mlflow.end_run()
        self.__run = None

    def load_model(self) -> typing.Optional[PyFuncModel]:
        try:
            self.model = mlflow.pyfunc.load_model(
                f"models:/{self.model_id}@production")
        except Exception:
            self.model = None
        return self.model

    @abc.abstractmethod
    def infer(self, model: typing.Optional[PyFuncModel], data: typing.Dict[str, typing.Any], selector: str, device_id: str, timestamp: datetime.datetime) -> typing.Tuple[typing.Optional[typing.Any], typing.Optional[PythonModel]]:
        """
        Subclasses must override this method.
        :param model: The current model
        :param data: Dictionary containing data extracted from a message.
        :param selector: Name of a selector identifying the extracted data.
        :param device_id: ID of the device the message originates from
        :param timestamp: Kafka stored message timestamp.
        :return: Result data or None.
        """
        pass

    @abc.abstractmethod
    def train(self, model: typing.Optional[PyFuncModel], logger: TrainMlflowLoggerCallback) -> typing.Optional[PythonModel]:
        """
        Subclasses must override this method.
        :param model: The current model
        :return: Result data or None.
        """
        return None

    @abc.abstractmethod
    def need_retraining(self, model: typing.Optional[PyFuncModel]) -> bool:
        """
        Subclasses must override this method.
        :param model: The current model
        :return: Result data or None.
        """
        return False
    
    def __start_run(self):
        job_name = f"{self.model_id}@{datetime.datetime.now().isoformat(timespec='microseconds')}"
        self.__run = mlflow.start_run(run_name=job_name)
        self.__mlflow_logger = TrainMlflowLoggerCallback(self.config.mlflow_url, self.model_id, self.__run.info.run_id)
    
    def __wrap_training(self):
        self.__start_run()
        
        ray.init(address=self.config.ray_url,
                 runtime_env=RuntimeEnv(**self.config.ray_runtime_env))
        model = self.train(self.model, self.__mlflow_logger)
        ray.shutdown()
        if model is not None:
            self.update_model(model)

    def run(self, data: typing.Dict[str, typing.Any], selector: str, device_id: str, timestamp: datetime.datetime):
        result, model = self.infer(
            self.model, data, selector, device_id, timestamp)
        if model is not None:  # TODO and not equal self.model
            self.update_model(model)
        # TODO reconsider checking on every message
        if self.need_retraining(self.model):
            self.__wrap_training()
        return result
