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

import operator_lib.util as util
import json
import confluent_kafka
import mf_lib
import cncr_wdg
import signal

class OperatorLib:
    def __init__(self, operator: util.OperatorBase, name: str = "OperatorLib", git_info_file="git_commit"):
        util.print_init(name=name, git_info_file=git_info_file)
        dep_config = util.DeploymentConfig()
        opr_config = util.OperatorConfig(json.loads(dep_config.config))
        util.init_logger(opr_config.config.logger_level)
        util.logger.debug(f"deployment config: {dep_config}")
        util.logger.debug(f"operator config: {opr_config}")
        filter_handler = mf_lib.FilterHandler()
        for it in opr_config.inputTopics:
            filter_handler.add_filter(util.gen_filter(input_topic=it, selectors=operator.selectors, pipeline_id=dep_config.pipeline_id))
        kafka_brokers = ",".join(util.get_kafka_brokers(zk_hosts=dep_config.zk_quorum, zk_path=dep_config.zk_brokers_path))
        kafka_consumer_config = {
            "metadata.broker.list": kafka_brokers,
            "group.id": dep_config.config_application_id,
            "auto.offset.reset": dep_config.consumer_auto_offset_reset_config,
            "max.poll.interval.ms": 6000000
        }
        kafka_producer_config = {
            "metadata.broker.list": kafka_brokers,
        }
        util.logger.debug(f"kafka consumer config: {kafka_consumer_config}")
        util.logger.debug(f"kafka producer config: {kafka_producer_config}")
        kafka_consumer = confluent_kafka.Consumer(kafka_consumer_config, logger=util.logger)
        kafka_producer = confluent_kafka.Producer(kafka_producer_config, logger=util.logger)
        operator.init(
            kafka_consumer=kafka_consumer,
            kafka_producer=kafka_producer,
            filter_handler=filter_handler,
            output_topic=dep_config.output,
            pipeline_id=dep_config.pipeline_id,
            operator_id=dep_config.operator_id,
            config=operator.configType(json.loads(dep_config.config).config)
        )
        watchdog = cncr_wdg.Watchdog(
            monitor_callables=[operator.is_alive],
            shutdown_callables=[operator.stop],
            join_callables=[kafka_consumer.close, kafka_producer.flush],
            shutdown_signals=[signal.SIGTERM, signal.SIGINT, signal.SIGABRT],
            logger=util.logger
        )
        watchdog.start(delay=5)
        operator.start()
        watchdog.join()
