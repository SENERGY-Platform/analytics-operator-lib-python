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

from .config import *
from .logger import *
from .model import *
from .op_base import *
from .init_phase import *
from .timestamps import *
from .start_time import *
import math
import kazoo.client
import json
import typing
import hashlib

import mf_lib
from copy import deepcopy

def print_init(name, git_info_file):
    lines = list()
    l_len = len(name)
    with open(git_info_file, "r") as file:
        for line in file:
            key, value = line.strip().split("=")
            line = f"{key}: {value}"
            lines.append(line)
            if len(line) > l_len:
                l_len = len(line)
    if len(name) < l_len:
        l_len = math.ceil((l_len - len(name) - 2) / 2)
        print("*" * l_len + f" {name} " + "*" * l_len)
        l_len = 2 * l_len + len(name) + 2
    else:
        print(name)
    for line in lines:
        print(line)
    print("*" * l_len)


def get_kafka_brokers(zk_hosts: str, zk_path: str):
    zk_client = kazoo.client.KazooClient(hosts=zk_hosts)
    zk_client.start()
    brokers = list()
    for id in zk_client.get_children(zk_path):
        data, _ = zk_client.get(f"{zk_path}/{id}")
        data = json.loads(data)
        brokers.append(f"{data['host']}:{data['port']}")
    zk_client.stop()
    return brokers


def gen_identifiers(name: str, f_type: str, f_value: str, pipeline_id: str):
    if f_type == "DeviceId":
        return [
            {
                "key": "device_id",
                "value": f_value
            },
            {
                "key": "service_id",
                "value": name.replace("_", ":")
            }
        ]
    elif f_type == "OperatorId":
        if f_value.find(":") != -1:
            o_id, p_id = f_value.split(":")
        else:
            o_id = f_value
            p_id = pipeline_id
        return [
            {
                "key": "operator_id",
                "value": o_id
            },
            {
                "key": "pipeline_id",
                "value": p_id
            }
        ]
    elif f_type == "ImportId":
        return [
            {
                "key": "import_id",
                "value": f_value
            }
        ]


def get_selector(mappings: typing.List, selectors: typing.List):
    dst_fields = set(m.dest for m in mappings)
    for s in selectors:
        if dst_fields == s.args:
            return s.name
    raise RuntimeError(f"no selector for {dst_fields}")


def hash_str(obj: str) -> str:
    return hashlib.sha256(obj.encode()).hexdigest()


def hash_list(obj: typing.List) -> str:
    return hash_str("".join(obj))


def hash_dict(obj: typing.Dict) -> str:
    items = ["{}{}".format(key, value) for key, value in obj.items()]
    items.sort()
    return hash_list(items)


def gen_filter(input_topic, pipeline_id: str, selectors=None):
    filter = {
        "source": input_topic.name,
        "mappings": {f"{m.dest}:data": ("analytics." if input_topic.filterType == "OperatorId" else "") + m.source for m
                     in input_topic.mappings},
        "identifiers": gen_identifiers(name=input_topic.name, f_type=input_topic.filterType,
                                       f_value=input_topic.filterValue, pipeline_id=pipeline_id),
        "args": {
            "selector": get_selector(mappings=input_topic.mappings, selectors=selectors) if selectors else None
        }
    }
    items = [
        filter["source"],
        hash_dict(filter["mappings"]),
        hash_dict(filter["args"])
    ]
    for i in filter["identifiers"]:
        items.append(hash_dict(i))
    filter["id"] = hash_list(items)
    return filter

def create_filter_handler(input_topics, pipeline_id, selectors):
    filter_handler = mf_lib.FilterHandler()

    for input_topic_tmp in input_topics:
        # filterValue can be a list, e.g. when device group with the same service is used as input
        filter_values = input_topic_tmp.filterValue.split(',')
        for filter_value in filter_values:
            input_topic = deepcopy(input_topic_tmp)
            input_topic.filterValue = filter_value
            msg_filter = gen_filter(input_topic=input_topic, selectors=selectors, pipeline_id=pipeline_id)
            filter_handler.add_filter(msg_filter)
            print(f"added filter: {msg_filter} for filterValue: {input_topic.filterValue}")
    
    return filter_handler