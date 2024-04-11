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

__all__ = ("OperatorBase",)

from .logger import logger
from .model import Config, Selector
import confluent_kafka
import mf_lib
import json
import typing
import datetime
import typing


def log_kafka_sub_action(action: str, partitions: typing.List):
    for partition in partitions:
        logger.info(
            f"subscription event: action={action} topic={partition.topic} partition={partition.partition} offset={partition.offset}"
        )


def on_assign(_, p):
    log_kafka_sub_action("assign", p)


def on_revoke(_, p):
    log_kafka_sub_action("revoke", p)


def on_lost(_, p):
    log_kafka_sub_action("lost", p)


class OperatorBase:
    configType = Config # Subclasses may override this with a subclass of Config.
    config = configType({})
    selectors: typing.List[Selector] = [] # Subclasses should fill these.

    def __new__(cls, *args, **kwargs):
        obj = super(OperatorBase, cls).__new__(cls)
        setattr(obj, f"_{OperatorBase.__name__}__kafka_consumer", None)
        setattr(obj, f"_{OperatorBase.__name__}__kafka_producer", None)
        setattr(obj, f"_{OperatorBase.__name__}__filter_handler", None)
        setattr(obj, f"_{OperatorBase.__name__}__output_topic", None)
        setattr(obj, f"_{OperatorBase.__name__}__pipeline_id", None)
        setattr(obj, f"_{OperatorBase.__name__}__operator_id", None)
        setattr(obj, f"_{OperatorBase.__name__}__poll_timeout", None)
        setattr(obj, f"_{OperatorBase.__name__}__stop", False)
        setattr(obj, f"_{OperatorBase.__name__}__stopped", False)
        return obj

    def __call_run(self, message, topic):
        run_results = list()
        try:
            for result in self.__filter_handler.get_results(message=message):
                if not result.ex:
                    for f_id in result.filter_ids:
                        run_result = self.run(
                            selector=self.__filter_handler.get_filter_args(id=f_id)["selector"],
                            data=result.data,
                            topic=topic,
                            filter_value=f_id
                        )
                        if run_result is not None:
                            if isinstance(run_result, list):
                                run_results += run_result
                            else:
                                run_results.append(run_result)
                else:
                    logger.error(result.ex)
                    self.__handle_result_error(result.ex, message, self.produce)

        except mf_lib.exceptions.NoFilterError:
            pass
        except mf_lib.exceptions.MessageIdentificationError as ex:
            logger.error(ex)
        return run_results

    def __route(self):
        msg_obj = self.__kafka_consumer.poll(timeout=self.__poll_timeout)
        if msg_obj:
            if not msg_obj.error():
                results = self.__call_run(json.loads(msg_obj.value()), json.loads(msg_obj.value()).get('device_id'))
                for result in results:
                    self.__kafka_producer.produce(
                        self.__output_topic,
                        json.dumps(
                            {
                                "pipeline_id": self.__pipeline_id,
                                "operator_id": self.__operator_id,
                                "analytics": result,
                                "time": "{}Z".format(datetime.datetime.utcnow().isoformat())
                            }
                        ),
                        self.__operator_id
                    )
            else:
                raise confluent_kafka.KafkaException(msg_obj.error())

    def __loop(self):
        q = 0
        i = 0
        while not self.__stop:
            try:
                self.__route()
            except Exception as ex:
                logger.exception(ex)
                self.__stop = True
            if len(self.__kafka_producer) > 0:
                if q == len(self.__kafka_producer):
                    i = i+1
                else: 
                    i = 0
                    q = len(self.__kafka_producer)
                if q > 10 or i == 3: # 10 msgs queued or waited for 3 cycles without new msgs
                    self.__kafka_producer.flush()
                    i = 0
                    q = 0
            else:
                q = 0
                i = 0
        self.__stopped = True

    def init(
        self, 
        kafka_consumer: confluent_kafka.Consumer, 
        kafka_producer: confluent_kafka.Producer, 
        filter_handler: mf_lib.FilterHandler, 
        output_topic: str, 
        pipeline_id: str, 
        operator_id: str, 
        poll_timeout: float = 1.0, 
        config: Config = Config({}),
        result_error_handler = None
    ):
        self.__kafka_consumer = kafka_consumer
        self.__kafka_producer = kafka_producer
        self.__filter_handler = filter_handler
        self.__output_topic = output_topic
        self.__pipeline_id = pipeline_id
        self.__operator_id = operator_id
        self.__poll_timeout = poll_timeout
        self.__handle_result_error = result_error_handler
        self.config = config

    def get_pipeline_id(self) -> str:
        return self.__pipeline_id

    def get_operator_id(self) -> str:
        return self.__operator_id

    def start(self):
        sources = self.__filter_handler.get_sources()
        if sources:
            self.__kafka_consumer.subscribe(
                sources,
                on_assign=on_assign,
                on_revoke=on_revoke,
                on_lost=on_lost
            )
        else:
            raise RuntimeError("no sources")
        self.__loop()

    def stop(self):
        self.__stop = True

    def is_alive(self) -> bool:
        return not self.__stopped

    def produce(self, value):
        self.__kafka_producer.produce(
            self.__output_topic,
            json.dumps({
                "pipeline_id": self.__pipeline_id,
                "operator_id": self.__operator_id,
                "analytics": value,
                "time": "{}Z".format(datetime.datetime.utcnow().isoformat())
            }),
            self.__operator_id
        )

    def run(self, data: typing.Dict[str, typing.Any], selector: str, topic: str, filter_value: str):
        """
        Subclasses must override this method.
        :param data: Dictionary containing data extracted from a message.
        :param selector: Name of a selector identifying the extracted data.
        :return: Result data or None.
        """
        pass
