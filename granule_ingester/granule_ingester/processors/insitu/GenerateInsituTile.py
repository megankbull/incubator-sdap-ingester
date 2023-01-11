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

import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import List, Tuple

import dateutil.parser as dp
from granule_ingester.granule_loaders import InsituLoader
from granule_ingester.writers import SolrStore
from nexusproto.DataTile_pb2 import InsituTile, InsituTileObservation, NexusTile, Platform, TileSummary

MAX_SOLR_FQ = 150
determine_geo = SolrStore.determine_geo

logger = logging.getLogger(__name__)


def _solr_query(solr, ids):
    q = '*:*'

    params = {
        'fq': "{!terms f=id}%s" % ','.join(ids),
        'rows': MAX_SOLR_FQ
    }

    logger.debug(f'Running Solr query *:* with params {params}')

    docs = solr.search(q, **params).docs

    logger.debug('Finished solr query')

    return docs


def _min(a, b):
    if a is None:
        return b

    return a if a < b else b


def _max(a, b):
    if a is None:
        return b

    return a if a > b else b


def _generate_metadata(observations, files):
    min_lat, max_lat, min_lon, max_lon, min_depth, max_depth, min_time, max_time = None, None, None, None, None, None, None, None
    count = len(observations)

    for o in observations:
        min_lat = _min(min_lat, o.latitude)
        min_lon = _min(min_lon, o.longitude)
        min_depth = _min(min_depth, o.depth)
        min_time = _min(min_time, o.time)
        max_lat = _max(max_lat, o.latitude)
        max_lon = _max(max_lon, o.longitude)
        max_depth = _max(max_depth, o.depth)
        max_time = _max(max_time, o.time)

    bbox = TileSummary.BBox()
    bbox.lon_min = min_lon
    bbox.lat_min = min_lat
    bbox.lon_max = max_lon
    bbox.lat_max = max_lat

    geo = determine_geo(bbox)

    return {
        "geo": geo,
        "id": None,
        "dataset_s": None,
        "files_ss": files,
        "solr_id_s": None,
        "tile_min_lon": min_lon,
        "tile_max_lon": max_lon,
        "tile_min_lat": min_lat,
        "tile_max_lat": max_lat,
        "tile_min_depth": min_depth,
        "tile_max_depth": max_depth,
        "tile_min_time_dt": min_time,
        "tile_max_time_dt": max_time,
        "tile_count_i": count
    }


class GenerateInsituTile:
    def __init__(self, solr, max_threads):
        self._solr = solr
        self._max_threads = max_threads

    @staticmethod
    async def generate(uuids: List[str], dataset, solr, max_threads=4) -> Tuple[NexusTile, dict]:
        gen = GenerateInsituTile(solr, max_threads)
        return await gen._generate_tile(uuids, dataset)

    async def _generate_tile(self, uuids: List[str], dataset) -> Tuple[NexusTile, dict]:
        uuids.sort()

        logger.info(f'Generating insitu tile from {len(uuids)} observation(s)')

        logger.info('Collecting insitu info from Solr')

        pool = ThreadPoolExecutor(max_workers=self._max_threads, thread_name_prefix='solr-query-worker')
        batches = [uuids[i:i + MAX_SOLR_FQ] for i in range(0, len(uuids), MAX_SOLR_FQ)]
        func = partial(_solr_query, self._solr)

        observations = []

        pool_result = pool.map(func, batches)

        for result in pool_result:
            observations.extend(result)

        pool.shutdown()

        file_map = {}

        logger.debug('Grouping observations by file_url')

        for observation in observations:
            if observation['file_url'] not in file_map:
                file_map[observation['file_url']] = [observation]
            else:
                file_map[observation['file_url']].append(observation)

        tile_observations = []

        logger.debug('Building InsituTileObservation objects')

        for file in file_map:
            async with InsituLoader(file) as (insitu_data, file_url):
                insitu_observations = insitu_data['observations']
                insitu_observations = [insitu_observations[i] for i in [d['index_i'] for d in file_map[file]]]

                for observation in insitu_observations:
                    insitu_observation = InsituTileObservation()
                    for field in observation:
                        if field == 'time':
                            setattr(insitu_observation, field, observation[field])
                            insitu_observation.timestamp = int(dp.parse(observation[field]).timestamp())
                        elif field == 'platform':
                            platform = Platform()
                            o_platform = observation[field]

                            platform.code = o_platform['code']

                            if 'id' in o_platform:
                                platform.id = o_platform['id']
                            if 'type' in o_platform:
                                platform.type = o_platform['type']

                            insitu_observation.platform.CopyFrom(platform)
                        elif field == 'meta':
                            meta = observation[field]
                            observation.meta = meta if type(meta) == str else json.dumps(meta)
                        else:
                            setattr(insitu_observation, field, observation[field])

                    tile_observations.append(insitu_observation)

        logger.debug('Creating NexusTile')

        insitu_tile_data = InsituTile()
        insitu_tile_data.observations.extend(tile_observations)

        metadata = _generate_metadata(tile_observations, list(file_map.keys()))

        tile = NexusTile()

        tile.tile.insitu_tile.CopyFrom(insitu_tile_data)

        tid = str(uuid.uuid3(uuid.NAMESPACE_DNS, ''.join([str(id) for id in uuids])))

        tile.tile.tile_id = tid

        summary = TileSummary()
        summary.tile_id = tid

        tile.summary.CopyFrom(summary)

        metadata['id'] = tid
        metadata['dataset_s'] = dataset
        metadata['solr_id_s'] = f'{dataset}!{tid}'

        logger.debug(f'Generated tile metadata:\n' + json.dumps(metadata, indent=4))

        return tile, metadata

