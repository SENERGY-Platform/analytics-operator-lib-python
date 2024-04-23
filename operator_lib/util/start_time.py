import datetime

__all__ = ("setup_operator_starttime", )

from operator_lib.util.persistence import load, save
from operator_lib.util.logger import logger
from operator_lib.util.timestamps import timestamp_to_str

FILE_NAME_OPERATOR_START_TIME = "operator_start_time.pickle"

def load_operator_start_time(data_path):
    start_time = load(data_path, FILE_NAME_OPERATOR_START_TIME)
    # fallback when start time was saved without timezone
    if not start_time.tzinfo:
        start_time = start_time.astimezone(datetime.timezone.utc)
    return start_time

def save_operator_start_time(data_path, timestamp):
    save(data_path, FILE_NAME_OPERATOR_START_TIME, timestamp)

def setup_operator_starttime(data_path):
    operator_start_time = load_operator_start_time(data_path)
    if not operator_start_time:
        operator_start_time = datetime.datetime.now(datetime.timezone.utc)
        logger.info(f"Store operator start time not found -> create and save")
        save_operator_start_time(data_path, operator_start_time)
    logger.info(f"Operator start time: {timestamp_to_str(operator_start_time)}")
    return operator_start_time

