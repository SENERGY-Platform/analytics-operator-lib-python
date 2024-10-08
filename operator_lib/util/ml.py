import threading
import time 

import pandas as pd
import mlflow
from model_trainer_client.trainer import TrainerClient

from .persistence import save, load

JOB_ID_FILENAME = "training_job_id.pickle"
  
class Downloader(threading.Thread):
    def __init__(
        self, 
        logger,
        detector_class_ref,
        mlflow_url,
        ml_trainer_url,
        check_interval_seconds=60,
    ):
        threading.Thread.__init__(self)
        self.logger = logger 
        self.check_interval_seconds = check_interval_seconds
        self.detector_class_ref = detector_class_ref
        self.ml_trainer_url = ml_trainer_url
        self.__stop = False
        self.check = False
        self.job_id = None
        self.client = TrainerClient(ml_trainer_url, logger)
        mlflow.set_tracking_uri(mlflow_url)

    def run(self):
        self.logger.info("Start Downloader Thread")
        while not self.__stop:
            while self.check:
                self.__check()
                self.__wait()

    def __wait(self):
        time.sleep(self.check_interval_seconds)

    def __check(self):
        if not self.job_id:
            self.logger.debug(f"Job ID missing")
            return 

        if not self.client.is_job_ready(self.job_id):
            self.logger.debug(f"Job {self.job_id} not ready yet")
            return

        self.__download()
        self.disable_check()

    def __download(self):
        model_uri = f"models:/{self.job_id}@production"
        self.logger.debug(f"Try to download model {self.job_id}")
        model = mlflow.pyfunc.load_model(model_uri)
        self.logger.debug(f"Downloading model {self.job_id} was succesfull")
        self.detector_class_ref.model = model
    
    def stop(self):
        self.logger.info("Stop Downloader Loop")
        self.disable_check()
        self.__stop = True

    def enable_check(self, job_id):
        self.logger.info(f"Check for job id: {job_id}")
        self.job_id = job_id
        self.check = True

    def disable_check(self):
        self.check = False

class Trainer():
    # Manages model trainings, polls for status and downloads the model
    # When instantiated, it will check for an existing/persisted job id 
    # Otherwise it will poll for job status after a job was created
    # The polling is done in a background thread within an interval
    
    def __init__(
        self, 
        logger, 
        data_path,
        ml_trainer_url,
        detector_class_ref,
        last_training_time,
        train_interval,
        train_level,
        retrain: bool,
        mlflow_url,
        check_interval_seconds
    ) -> None:
        self.logger = logger
        self.data_path = data_path
        self.ml_trainer_url = ml_trainer_url
        self.job_id = load(data_path, JOB_ID_FILENAME)
        self.last_training_time = last_training_time
        self.train_interval = train_interval
        self.train_level = train_level
        self.retrain = retrain
        self.client = TrainerClient(ml_trainer_url, logger)
        
        self.downloader = Downloader(
            self.logger,
            detector_class_ref,
            mlflow_url,
            ml_trainer_url,
            check_interval_seconds
        )

        self.downloader.start() # Start the downloader thread
        self.check_exisiting_job_id()

    def check_exisiting_job_id(self):
        if self.job_id:
            self.downloader.enable_check(self.job_id)

    def start_training(self, job_request):
        self.job_id = self.client.start_training(job_request)
        save(self.data_path, JOB_ID_FILENAME, self.job_id)
        self.logger.debug(f"Created Training Job with ID: {self.job_id}")
        self.downloader.enable_check(self.job_id)

    def training_shall_start(self, timestamp):
        if not self.retrain and self.job_id:
            self.logger.debug("Retrain is disabled and there a job exists already.")
            return False
        
        self.logger.debug(f"Current Time: {timestamp} - Last Train Time: {self.last_training_time} < {self.train_interval}{self.train_level}")
        if timestamp - self.last_training_time < pd.Timedelta(self.train_interval, self.train_level):
            self.logger.debug("Wait with training until enough data is collected")
            return False 

        if not self.job_id:
            self.logger.debug("No existing JobID -> Start first training") 

        if self.retrain:
            self.logger.debug("Retrain period is over -> Start new traning")

        return True

    def stop(self):
        self.downloader.stop()
        self.downloader.join(timeout=10)

    def join(self):
        self.downloader.join(timeout=30)
  