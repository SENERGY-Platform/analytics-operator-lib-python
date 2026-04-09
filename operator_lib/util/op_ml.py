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

from .op_base import OperatorBase

import typing
import datetime
import typing
import abc
import mlflow
from mlflow import MlflowClient
from mlflow.pyfunc import PyFuncModel, PythonModel

from datetime import datetime


class MLOperator(OperatorBase):
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        mlflow.set_tracking_uri(self.config.mlflow_url)
        self.__model_id = f"pipeline-{self.get_pipeline_id}_operator-{self.get_operator_id}"
        model = self.load_model()
        if model is None:
            model = self.train(self.model)
            if model is not None:
                self.update_model(model)
        
        
    def update_model(self, model: PythonModel):
        # This will store the model at MLFlow model registry
        # If it does not exist, it will be created with version 1
        # All following models will increment the version by 1
        # The latest version gets the alias `production`

        mlflow.end_run()
        job_name = f"{self.__model_id}@{datetime.now().isoformat(timespec='microseconds')}"
        mlflow.set_experiment(job_name)
        run_relative_artifact_path = 'models'

        # Create a new model version and save model
        with mlflow.start_run(run_name="store-model") as run:
            # mlflow.log_metrics(metrics) TODO
            # mlflow.log_params(config) TODO

            mlflow.pyfunc.log_model(
                artifact_path=run_relative_artifact_path,
                python_model=model,
                # signature=signature TODO
            )
        
        model_uri = f"runs:/{job_name}/{run_relative_artifact_path}"
       
        created_model_version = mlflow.register_model(model_uri, job_name)
        client = MlflowClient()
        client.set_registered_model_alias(job_name, "production", created_model_version.version)
        self.model = mlflow.pyfunc.load_model(f"models:/{self.__model_id}@production")
        
    def load_model(self) -> typing.Optional[PyFuncModel]:
        try:
            self.model = mlflow.pyfunc.load_model(f"models:/{self.__model_id}@production")
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
    def train(self, model: typing.Optional[PyFuncModel]) -> typing.Optional[PythonModel]:
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

    def run(self, data: typing.Dict[str, typing.Any], selector: str, device_id: str, timestamp: datetime.datetime):
        result, model = self.infer(self.model, data, selector, device_id, timestamp)
        if model is not None: # TODO and not equal self.model
            self.update_model(model)
        if self.need_retraining(self.model): # TODO reconsider checking on every message
            model = self.train(self.model)
            if model is not None:
                self.update_model(model)
        return result
