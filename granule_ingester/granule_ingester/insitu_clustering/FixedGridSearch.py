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
import json
from argparse import Namespace
from itertools import cycle
from time import sleep
from typing import Tuple, List

import numpy as np
import pysolr
from aio_pika import connect_robust
from granule_ingester.insitu_clustering import ClusterSearch

logger = logging.getLogger(__name__)

DELAY = 0.1


class FixedGridSearch(ClusterSearch):
    def __init__(self, args: Namespace, solr: pysolr.Solr):
        ClusterSearch.__init__(self, args, solr)

        self.__geos = self.__generate_geo_params()

        self.__pass = 0
        self.__prev_pass = 0

        self.__mapping = {}  # maps geo to map of time bins to obs counts, used to reap most isolated / max to tiles

    def __param(self, param):
        return getattr(self._args, f'FixedGrid:{param}')

    def __generate_geo_params(self):
        geos = []

        step_size_lat = self.__param('lat_step')
        step_size_lon = self.__param('lon_step')

        lat_shift = self.__param('lat_shift')
        lon_shift = self.__param('lon_shift')

        start_lon = -180
        start_lat = -90

        geos.extend(FixedGridSearch.__geo(start_lat,
                                          start_lon,
                                          step_size_lat,
                                          step_size_lon))

        # if lat_shift:
        #     geos.extend(FixedGridSearch.__geo(start_lat + (step_size_lat / 2),
        #                                       start_lon,
        #                                       step_size_lat,
        #                                       step_size_lon))
        #
        # if lon_shift:
        #     geos.extend(FixedGridSearch.__geo(start_lat,
        #                                       start_lon + (step_size_lon / 2),
        #                                       step_size_lat,
        #                                       step_size_lon))
        #
        # if lat_shift and lon_shift:
        #     geos.extend(FixedGridSearch.__geo(start_lat + (step_size_lat / 2),
        #                                       start_lon + (step_size_lon / 2),
        #                                       step_size_lat,
        #                                       step_size_lon))

        geos.sort()

        logger.debug('Generated geo search param list:\n' + json.dumps(
            [f"[{geo[1]},{geo[0]} TO {geo[3]},{geo[2]}]" for geo in geos], indent=4)
                     )

        return geos

    @staticmethod
    def __geo(start_lat, start_lon, step_lat, step_lon):
        geos = []

        for lon in np.arange(start_lon, 180+step_lon, step_lon):
            min_lon = lon
            max_lon = min(180, lon + step_lon)

            if min_lon == max_lon:
                continue

            for lat in np.arange(start_lat, 90+step_lat, step_lat):
                min_lat = lat
                max_lat = min(90, lat + step_lat)

                if min_lat == max_lat:
                    continue

                geos.append((min_lon, min_lat, max_lon, max_lat))

        return geos

    def __get_queue_length(self) -> int:
        connection_string = f"amqp://{self._rmq_user}:{self._rmq_password}@{self._rmq_host}/"
        connection = await connect_robust(connection_string)
        channel = await connection.channel()

        res = await channel.declare_queue(self._args.insitu_rmq_stage, durable=True)

        length = int(res.declaration_result.message_count)
        logger.info(f'There are {length} files waiting to be staged')

        return length

    def __get_stage_count(self) -> int:
        count = self._solr.search('*:*', **{'rows': 0}).hits
        logger.info(f'There is a total of {count} observations staged in Solr')
        return count

    def __query_solr(self, q='*:*', additional_params=None):
        solr_docs = []
        if additional_params is None:
            additional_params = {'cursorMark': '*'}

        if 'cursorMark' not in additional_params:
            additional_params['cursorMark'] = '*'

        while True:
            solr_result = self._solr.search(q, **additional_params)

            solr_docs.extend(solr_result.docs)

            if solr_result.nextCursorMark == additional_params['cursorMark']:
                break
            else:
                additional_params['cursorMark'] = solr_result.nextCursorMark

        return solr_docs

    def __query_geo(self, geo):
        q = '*:*'
        additional_params = {
            'fq': [f"geo:[{geo[1]},{geo[0]} TO {geo[3]},{geo[2]}]"],
            'rows': 10000,
            'sort': 'time asc, id asc',
            'cursorMark': '*'
        }

        return self.__query_solr(q, additional_params)

    @staticmethod
    def __bin_by_time(observations, bin_length):
        times = [o['time'] for o in observations]
        min_time = min(times)
        max_time = max(times)

        bins = list(range(min_time, max_time, bin_length))

        if bins[-1] != max_time:
            bins.append(max_time)

        logger.debug(f'Binning {len(observations)} into {len(bins)} bins')

        ret = {}

        for i, bin_min in enumerate(bins[:-1]):
            bin_max = bins[i+1]

            for observation in observations[:]:
                if bin_min <= observation['time'] <= bin_max:
                    if bin_min not in ret:
                        ret[bin_min] = []

                    ret[bin_min].append(observation)
                    observations.remove(observation)

        logger.debug('Binned observation counts:')
        for bin_min in ret:
            logger.debug(f'{bin_min}: {len(ret[bin_min])}')

        return ret

    def _detect_clusters(self) -> Tuple[List[str], str]:
        geos = cycle(self.__geos)

        def geo_to_str(geo_tuple):
            return ' '.join([str(g) for g in geo_tuple])

        logger.info('Starting cluster detection')

        while True:
            # If we've preprocessed all incoming granules; flush everything since we won't be getting a new cluster with
            # no more incoming observations
            if self.__get_queue_length() == 0 and len(self.__mapping) == len(self.__geos):
                logger.info('Staging queue is empty, flushing stage to tiles and waiting for new inputs')

                for uuids, dataset in self._flush('all'):
                    yield uuids, dataset
                geos = cycle(self.__geos)

                while self.__get_queue_length() == 0 and self.__get_stage_count() == 0:
                    sleep(5)
            # If the staging collection is full, flush both the isolated observations and the observations closest to
            # forming acceptable clusters until we can add more observations to staging.
            elif self.__get_stage_count() >= self._args.stage_limit and len(self.__mapping) == len(self.__geos):
                logger.info('Insitu stage is full, flushing some observations to tiles')

                to_flush = self._flush('isolated', count=2)
                to_flush.extend(self._flush('max', count=5))

                for uuids, dataset in to_flush:
                    yield uuids, dataset

                while self.__get_stage_count() >= self._args.stage_limit - self._args.stage_limit_hysteresis:
                    for uuids, dataset in self._flush('max', count=5):
                        yield uuids, dataset
            else:
                geo = next(geos)

                observations = self.__query_geo(geo)
                binned = FixedGridSearch.__bin_by_time(observations, self.__param('time'))

                for bin_time in list(binned.keys()):
                    current_bin = binned[bin_time]
                    bin_count = len(current_bin)

                    if bin_count >= self._args.tile_min:
                        logger.info('Detected cluster - Prepping info for tile generation')
                        datasets = set([o['dataset_s'] for o in current_bin])
                        for dataset in datasets:
                            tile_observations = [o for o in current_bin if o['dataset_s'] == dataset]

                            for o in tile_observations:
                                current_bin.remove(o)

                            uuids = [o['id'] for o in tile_observations]

                            yield uuids, dataset

                self.__mapping[geo_to_str(geo)] = binned

            sleep(DELAY)

    def _flush(self, method, **kwargs) -> List[Tuple[List[str], str]]:
        if method == 'all':
            ret = []

            for geo in self.__mapping:
                for bin_time in self.__mapping[geo]:
                    current_bin = self.__mapping[geo][bin_time]

                    datasets = set([o['dataset_s'] for o in current_bin])
                    for dataset in datasets:
                        tile_observations = [o for o in current_bin if o['dataset_s'] == dataset]
                        uuids = [o['id'] for o in tile_observations]
                        ret.append((uuids, dataset))

            self.__mapping = {}
            return ret
        elif method == 'isolated' or method == 'max':
            length_map = {}

            for geo in self.__mapping:
                for bin_time in self.__mapping[geo]:
                    current_bin = self.__mapping[geo][bin_time]

                    datasets = set([o['dataset_s'] for o in current_bin])
                    for dataset in datasets:
                        tile_observations = [o for o in current_bin if o['dataset_s'] == dataset]
                        uuids = [o['id'] for o in tile_observations]

                        length = len(uuids)

                        if length not in length_map:
                            length_map = []

                        length_map[length].append({
                            'uuids': uuids,
                            'ds': dataset,
                            'geo': geo,
                            'bin_time': bin_time
                        })

            lengths = list(length_map.keys())
            if 'count' not in kwargs:
                count = 5
            else:
                count = kwargs['count']

            if method == 'isolated':
                lengths.sort()
            else:
                lengths.sort(reverse=True)

            selection = lengths[:count]

            ret = []

            for length in selection:
                l = length_map[length]
                for tile in l:
                    ret.append((tile['uuids'], tile['ds']))
                    self.__mapping[tile['geo']][tile['bin_time']] = []

            return ret
        else:
            raise ValueError(f'Invalid flush method {method}')
