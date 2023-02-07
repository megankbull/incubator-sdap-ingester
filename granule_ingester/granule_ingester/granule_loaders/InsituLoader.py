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

from granule_ingester.granule_loaders.GranuleLoader import GranuleLoader

import logging
import os
import gzip
from urllib import parse

import json

from granule_ingester.exceptions import GranuleLoadingError

logger = logging.getLogger(__name__)


class InsituLoader:
    def __init__(self, resource: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._granule_temp_file = None
        self._resource = resource

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, type, value, traceback):
        if self._granule_temp_file:
            self._granule_temp_file.close()

    async def open(self) -> (dict, str):
        logger.info(f'Trying to open insitu file {self._resource}')
        resource_url = parse.urlparse(self._resource)
        if resource_url.scheme == 's3':
            # We need to save a reference to the temporary granule file so we can delete it when the context manager
            # closes. The file needs to be kept around until nothing is reading the dataset anymore.
            self._granule_temp_file = await GranuleLoader._download_s3_file(self._resource)
            file_path = self._granule_temp_file.name
        elif resource_url.scheme == '':
            file_path = self._resource
        else:
            raise RuntimeError("Granule path scheme '{}' is not supported.".format(resource_url.scheme))

        granule_name = os.path.basename(self._resource)
        ext = os.path.splitext(file_path)[-1]
        try:
            if ext == '.json':
                return json.load(open(file_path)), granule_name
            elif ext == '.gz':
                with gzip.open(file_path, 'r') as gz:
                    json_str = gz.read().decode('utf-8')

                return json.loads(json_str), granule_name
            else:
                raise GranuleLoadingError(f"The insitu file {self._resource} is of an unsupported file type {ext}.")
        except GranuleLoadingError:
            raise
        except FileNotFoundError:
            raise GranuleLoadingError(f"The insitu file {self._resource} does not exist.")
        except Exception:
            raise GranuleLoadingError(f"The insitu file {self._resource} is not a valid file.")