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

from .helpers.mlflow_logger import TrainMlflowLogger

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
        model = self.__load_model()
        if model is None:
            self.__wrap_training()

    def __update_model(self, model: PythonModel):
        with self.__mlflow_logger.trace("update_model"):
            if self.__run is None:
                self.__start_run()

            with self.__mlflow_logger.trace("log model"):
                new_model = mlflow.pyfunc.log_model(
                    artifact_path=self.model_id,
                    python_model=model,
                )
            with self.__mlflow_logger.trace("register model"):
                created_model_version = mlflow.register_model(
                    new_model.model_uri, self.model_id)

            with self.__mlflow_logger.trace("set alias"):
                client = MlflowClient()
                client.set_registered_model_alias(
                    self.model_id, "production", created_model_version.version)

            with self.__mlflow_logger.trace("update local model"):
                self.model = mlflow.pyfunc.load_model(
                    f"models:/{self.model_id}@production")
        mlflow.end_run()
        self.__run = None

    def __load_model(self) -> typing.Optional[PyFuncModel]:
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
        It will be called for each message. The current model will be provided. If your ML algorithm changes the model on each inference, you can return the new model as the second return value. This will update the model in mlflow and set it as the current model. If you return None as the second return value, the model is not updated. You should only return a new model if it is actually updated.
        :param model: The current model
        :param data: Dictionary containing data extracted from a message.
        :param selector: Name of a selector identifying the extracted data.
        :param device_id: ID of the device the message originates from
        :param timestamp: Kafka stored message timestamp.
        :return: Result data or None.
        """
        pass

    @abc.abstractmethod
    def train(self, model: typing.Optional[PyFuncModel], logger: TrainMlflowLogger) -> typing.Optional[PythonModel]:
        """
        Subclasses must override this method.
        Training is called if no model is present in mlflow or you return True in need_retraining. If a model already exists it is provided as a parameter. You can return a new model which is then registered in mlflow and set as the current model. If you return None, the current model is not updated.
        :param model: The current model
        :return: Result data or None.
        """
        return None

    @abc.abstractmethod
    def need_retraining(self, model: typing.Optional[PyFuncModel]) -> bool:
        """
        Subclasses must override this method.
        This method is called after each inference. Therefore, computation should be fast. If re-computaiton should not be done after every message, consider setting a timer to avoid frequent recomputation.
        If you return True, the training method is called to update the model.
        :param model: The current model
        :return: Result data or None.
        """
        return False

    def __start_run(self):
        job_name = f"{self.model_id}@{datetime.datetime.now().isoformat(timespec='microseconds')}"
        self.__run = mlflow.start_run(run_name=job_name)
        self.__mlflow_logger = TrainMlflowLogger(
            self.config.mlflow_url, self.model_id, self.__run.info.run_id)

    def __wrap_training(self):
        self.__start_run()

        ray.init(address=self.config.ray_url,
                 runtime_env=RuntimeEnv(**self.config.ray_runtime_env),
            #     log_to_driver=True, # TODO
            #     logging_config=ray.LoggingConfig(
            #         encoding="JSON",
            #         log_level="INFO",
            #     )
        )
        with self.__mlflow_logger.trace("train"):
            model = self.train(self.model, self.__mlflow_logger)
        ray.shutdown()
        if model is not None:
            self.__update_model(model)

    def run(self, data: typing.Dict[str, typing.Any], selector: str, device_id: str, timestamp: datetime.datetime):
        """
        The method will be called by the Operator Lib. It should not be called or overridden by subclasses. Subclasses should implement the infer and train method to provide ML functionality.
        """        
        result, model = self.infer(
            self.model, data, selector, device_id, timestamp)
        if model is not None:
            self.__update_model(model)
        if self.need_retraining(self.model):
            self.__wrap_training()
        return result
