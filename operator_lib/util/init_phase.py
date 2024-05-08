from operator_lib.util.persistence import load, save
from operator_lib.util.logger import logger

__all__ = ("InitPhase", )

FILE_NAME_INIT_PHASE_RESET = "init_phase_was_resetted.pickle"
FILE_NAME_INIT_PHASE_SENT = "init_phase_was_sent.pickle"

class InitPhase():
    def __init__(
        self, 
        data_path,
        init_phase_duration,
        first_data_time,
        produce
    ):
        self.data_path = data_path
        self.init_phase_duration = init_phase_duration
        self.__load_state()
        self.first_data_time = first_data_time
        self.produce = produce

    def generate_init_msg(self, timestamp, value_dict):
        td_until_start = self.init_phase_duration - (timestamp - self.first_data_time)
        minutes_until_start = int(td_until_start.total_seconds()/60)
        return self.__create_message(value_dict, minutes_until_start)
    
    def send_first_init_msg(self, value_dict):
        if not self.init_phase_resetted and not self.init_phase_was_sent:
            init_msg = self.__create_message(value_dict)
            self.produce(init_msg)

    def __create_message(self, value_dict, minutes_until_start=None):
        if not minutes_until_start:
            minutes_until_start = int(self.init_phase_duration.total_seconds()/60)

        value_dict["initial_phase"] = f"Die Anwendung befindet sich noch für ca. {minutes_until_start} Minuten in der Initialisierungsphase"
        return value_dict

    def __load_state(self):
        self.init_phase_resetted = load(self.data_path, FILE_NAME_INIT_PHASE_RESET)
        self.init_phase_was_sent = load(self.data_path, FILE_NAME_INIT_PHASE_SENT)        

    def operator_is_in_init_phase(self, timestamp):
        # sometimes timestamp might be in wrong order, which could trigger an acitve init phase
        init_active = timestamp-self.first_data_time < self.init_phase_duration and not self.init_phase_resetted
        if init_active:
            logger.debug(f"Still in initialisation phase! {timestamp} - {self.first_data_time} < {self.init_phase_duration}")
        return init_active

    def init_phase_needs_to_be_reset(self):
        return not self.init_phase_resetted

    def reset_init_phase(self, value_dict):
        logger.debug("Reset init phase message")
        self.init_phase_resetted = True
        save(self.data_path, FILE_NAME_INIT_PHASE_RESET, True)
        value_dict["initial_phase"] = ""
        return value_dict