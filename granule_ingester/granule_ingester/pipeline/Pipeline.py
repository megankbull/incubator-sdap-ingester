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

import logging
from abc import ABC, abstractmethod

import yaml
from granule_ingester.exceptions import PipelineBuildingError
# from granule_ingester.pipeline.NexusprotoPipeline import NexusprotoPipeline
# from granule_ingester.pipeline.CoGPipeline import CoGPipeline
from granule_ingester.pipeline.Modules import \
    modules as processor_module_mappings

logger = logging.getLogger(__name__)


class Pipeline(ABC):
    @classmethod
    @abstractmethod
    def build_pipeline(cls,
                       config: dict,
                       data_store_factory,
                       metadata_store_factory,
                       module_mappings: dict,
                       max_concurrency: int,
                       ):
        pass

    @abstractmethod
    async def run(self):
        pass

    @abstractmethod
    def set_log_level(self, log_level):
        pass
