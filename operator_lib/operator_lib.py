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
import prometheus_client 
class OperatorLib:
    def __init__(self, operator: util.OperatorBase, name: str = "OperatorLib", git_info_file="git_commit"):
        util.print_init(name=name, git_info_file=git_info_file)
        dep_config = util.DeploymentConfig()
        self.__dep_config = dep_config
        config_json = json.loads(dep_config.config)
        opr_config = util.OperatorConfig(config_json)
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
        if dep_config.metrics:
            metrics_port = 5555
            util.logger.info(f"Launching with metrics server on port {metrics_port}")
            kafka_consumer_config["statistics.interval.ms"] = 30000
            kafka_consumer_config["stats_cb"] = self.__consumer_stats
            kafka_producer_config["statistics.interval.ms"] = 30000
            kafka_producer_config["stats_cb"] = self.__producer_stats
            self.__kafka_consumer_consumer_fetch_manager_metrics_records_consumed_total = prometheus_client.Gauge('kafka_consumer_consumer_fetch_manager_metrics_records_consumed_total', 'The total number of records consumed kafka.consumer:name=null,type=consumer-fetch-manager-metrics,attribute=records-consumed-total', ['client_id', 'topic'])
            self.__kafka_consumer_consumer_fetch_manager_metrics_bytes_consumed_total = prometheus_client.Gauge('kafka_consumer_consumer_fetch_manager_metrics_bytes_consumed_total', 'The total number of bytes consumed kafka.consumer:name=null,type=consumer-fetch-manager-metrics,attribute=bytes-consumed-total', ['client_id', 'topic'])
            self.__kafka_producer_producer_topic_metrics_record_send_total = prometheus_client.Gauge('kafka_producer_producer_metrics_record_send_total', 'The total number of records sent. kafka.producer:name=null,type=producer-metrics,attribute=record-send-total', ['client_id'])
            self.__kafka_producer_producer_topic_metrics_byte_total = prometheus_client.Gauge('kafka_producer_producer_topic_metrics_byte_total', 'The total number of bytes sent for a topic. kafka.producer:name=null,type=producer-topic-metrics,attribute=byte-total', ['client_id', 'topic'])
            prometheus_client.start_http_server(metrics_port)

        util.logger.debug(f"kafka consumer config: {kafka_consumer_config}")
        util.logger.debug(f"kafka producer config: {kafka_producer_config}")
        kafka_consumer = confluent_kafka.Consumer(kafka_consumer_config, logger=util.logger)
        kafka_producer = confluent_kafka.Producer(kafka_producer_config, logger=util.logger)
        typed_config = operator.configType({})
        if "config" in config_json:
            typed_config = operator.configType(config_json['config'])
        operator.init(
            kafka_consumer=kafka_consumer,
            kafka_producer=kafka_producer,
            filter_handler=filter_handler,
            output_topic=dep_config.output,
            pipeline_id=dep_config.pipeline_id,
            operator_id=dep_config.operator_id,
            config=typed_config
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

    def __consumer_stats(self, stats_json_str):
        stats_json = json.loads(stats_json_str)
        client_id = self.__dep_config.config_application_id+'_'+stats_json['client_id']
        self.__kafka_consumer_consumer_fetch_manager_metrics_records_consumed_total.labels(client_id=client_id, topic="").set(stats_json['rxmsgs'])
        self.__kafka_consumer_consumer_fetch_manager_metrics_bytes_consumed_total.labels(client_id=client_id, topic="").set(stats_json['rx_bytes'])
        for topic, d in stats_json['topics'].items():
            rxmsgs = 0
            rx_bytes = 0
            for p, d in d['partitions'].items():
                rxmsgs = rxmsgs + d['rxmsgs']
                rx_bytes = rx_bytes + d['rxbytes']
            self.__kafka_consumer_consumer_fetch_manager_metrics_records_consumed_total.labels(client_id=client_id, topic=topic).set(rxmsgs)
            self.__kafka_consumer_consumer_fetch_manager_metrics_bytes_consumed_total.labels(client_id=client_id, topic=topic).set(rx_bytes)

    def __producer_stats(self, stats_json_str):
        stats_json = json.loads(stats_json_str)
        stats_json = json.loads(stats_json_str)
        client_id = self.__dep_config.config_application_id+'_'+stats_json['client_id']
        self.__kafka_producer_producer_topic_metrics_record_send_total.labels(client_id=client_id,).set(stats_json['txmsgs'])
        self.__kafka_producer_producer_topic_metrics_byte_total.labels(client_id=client_id, topic="").set(stats_json['tx_bytes'])
        for topic, d in stats_json['topics'].items():
            tx_bytes = 0
            for p, d in d['partitions'].items():
                tx_bytes = tx_bytes + d['txbytes']
            self.__kafka_producer_producer_topic_metrics_byte_total.labels(client_id=client_id, topic=topic).set(tx_bytes)