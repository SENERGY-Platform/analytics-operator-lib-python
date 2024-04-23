import datetime

__all__ = ("setup_operator_starttime", )

from operator_lib.util.persistence import load, save
from operator_lib.util.logger import logger

FILE_NAME_OPERATOR_START_TIME = "operator_start_time.pickle"

def load_operator_start_time(data_path):
    return load(data_path, FILE_NAME_OPERATOR_START_TIME)

def save_operator_start_time(data_path, timestamp):
    save(data_path, FILE_NAME_OPERATOR_START_TIME, timestamp)

def setup_operator_starttime(self, data_path):
    operator_start_time = load_operator_start_time(data_path)
    if not operator_start_time:
        operator_start_time = datetime.datetime.now()
        logger.info(f"Store operator start time not found -> create and save")
        save_operator_start_time(data_path, operator_start_time)
    logger.info(f"Operator start time: {operator_start_time}")
    return operator_start_time

