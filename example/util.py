"""
   Copyright 2024 InfAI (CC SES)

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



# This file contains sample data for the example operator.
# Do not call or copy this when implementing your own operator!


def setup():
    # Set up variables, will be set by flow-engine
    import os
    os.environ["ZK_QUORUM"] = "localhost:2181"
    os.environ["CONFIG_APPLICATION_ID"] = "analytics-test"
    os.environ["PIPELINE_ID"] = "fake-pipeline-id"
    os.environ["WINDOW_TIME"] = "30"
    os.environ["CONSUMER_AUTO_OFFSET_RESET_CONFIG"] = "earliest"
    os.environ["DEVICE_ID_PATH"] = "device_id"
    os.environ["OPERATOR_ID"] = "fake-operator-id"
    os.environ["CONFIG"] = """
    {
        "config": {
            "myconfig": "configvalue",
            "logging_level": "debug"
        },
        "inputTopics": [
            {
                "name": "topic",
                "filterType": "DeviceId",
                "filterValue": "test",
                "mappings": [
                    {
                    "dest": "value",
                    "source": "value.sensor"
                    }
                ]
            }
        ]
    }"""

    # Ingest some sample data
    import confluent_kafka
    kafka_producer_config = {
            "metadata.broker.list": "127.0.0.1",
        }
    kafka_producer = confluent_kafka.Producer(kafka_producer_config)
    i = 0
    while i < 5:
        i = i + 1
        kafka_producer.produce("topic", value='{"service_id":"topic","device_id": "test","value": {"sensor": '+str(i)+'}}')
    kafka_producer.flush()