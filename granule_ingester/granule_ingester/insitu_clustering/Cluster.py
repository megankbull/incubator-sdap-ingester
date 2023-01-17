# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABC, abstractmethod
from argparse import Namespace
from typing import List, Tuple

import pysolr
from yaml import dump

try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper

from granule_ingester.insitu_clustering.MessagePublisher import MessagePublisher

import logging

logger = logging.getLogger(__name__)


class ClusterSearch(ABC):
    def __init__(self, args: Namespace, solr: pysolr.Solr):
        self._args: Namespace = args
        self._solr: pysolr.Solr = solr

        self._rmq_host = args.rabbitmq_host
        self._rmq_user = args.rabbitmq_username
        self._rmq_password = args.rabbitmq_password
        self._rmq_queue = args.insitu_rmq

        self._rmq = MessagePublisher(self._rmq_host, self._rmq_user, self._rmq_password, self._rmq_queue)

    # Returns uuid list & dataset_s
    @abstractmethod
    def _detect_clusters(self) -> Tuple[List[str], str]:
        pass

    # method: all = empty stage, isolated, max
    # returns list of cluster tuples
    @abstractmethod
    def flush(self, method, **kwargs) -> List[Tuple[List[str], str]]:
        pass

    @staticmethod
    def _build_message(self, uuids: List[str], dataset: str) -> str:
        msg_dict = {
            'type': 'tile',
            'dataset': dataset,
            'ids': uuids
        }

        return dump(msg_dict, Dumper=Dumper, sort_keys=False)

    def start_detecting(self):
        logger.info('Beginning cluster detection')

        for uuids, dataset in self._detect_clusters():
            logger.info(f'Found cluster of {len(uuids)} observations for dataset {dataset}')
            logger.debug('Building rmq message')
            message = ClusterSearch._build_message(uuids, dataset)

            logger.debug('Publishing to rmq')
            async with self._rmq:
                await self._rmq.publish_message(message)

            logger.debug('Message published')
