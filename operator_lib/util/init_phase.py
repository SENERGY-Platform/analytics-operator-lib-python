from operator_lib.util.persistence import load, save
from operator_lib.util.logger import logger
from operator_lib.util.start_time import load_operator_start_time

__all__ = ("InitPhase", )

FILE_NAME_INIT_PHASE_RESET = "init_phase_was_resetted.pickle"
FILE_NAME_INIT_PHASE_SENT = "init_phase_was_sent.pickle"

class InitPhase():
    def __init__(
        self, 
        data_path,
        init_phase_duration
    ):
        self.data_path = data_path
        self.init_phase_duration = init_phase_duration
        self.__load_state()
        self.operator_start_time = load_operator_start_time(data_path)
        if not self.operator_start_time:
            raise Exception("Operator Start Time File is missing!")

    def generate_init_msg(self, timestamp, value_dict):
        logger.debug(f"Still in initialisation phase! {timestamp} - {self.operator_start_time} < {self.init_phase_duration}")
        td_until_start = self.init_phase_duration - (timestamp - self.operator_start_time)
        minutes_until_start = int(td_until_start.total_seconds()/60)
        return self.__create_message(value_dict, minutes_until_start)
    
    def first_init_msg_needs_to_send(self):
        return not self.init_phase_resetted and not self.init_phase_was_sent

    def generate_first_init_msg(self, value_dict):
        return self.__create_message(value_dict)

    def __create_message(self, value_dict, minutes_until_start=None):
        if not minutes_until_start:
            minutes_until_start = int(self.init_phase_duration.total_seconds()/60)

        value_dict["initial_phase"] = f"Die Anwendung befindet sich noch für ca. {minutes_until_start} Minuten in der Initialisierungsphase"
        return value_dict

    def __load_state(self):
        self.init_phase_resetted = load(self.data_path, FILE_NAME_INIT_PHASE_RESET)
        self.init_phase_was_sent = load(self.data_path, FILE_NAME_INIT_PHASE_SENT)

    def __save_init_phase_was_resetted(self):
        save(self.data_path, FILE_NAME_INIT_PHASE_RESET, True)

    def operator_is_in_init_phase(self, timestamp):
        # sometimes timestamp might be in wrong order, which could trigger an acitve init phase
        return timestamp-self.operator_start_time < self.init_phase_duration and not self.init_phase_resetted

    def init_phase_needs_to_be_reset(self):
        return not self.init_phase_resetted

    def reset_init_phase(self, value_dict):
        logger.debug("Reset init phase message")
        self.init_phase_resetted = True
        self.__save_init_phase_was_resetted()
        value_dict["initial_phase"] = ""
        return value_dict