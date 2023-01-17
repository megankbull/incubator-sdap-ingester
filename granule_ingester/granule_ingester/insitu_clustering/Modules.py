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

import pandas as pd

from granule_ingester.insitu_clustering import FixedGridSearch


class Dummy:
    pass


def _parse_td(td):
    return int(pd.Timedelta(td).total_seconds())


modules = {
    'dummy': {
        'cls': Dummy,
        'args': [
            {
                'args': ('--cl-arg',),
                'kwargs': {
                    'default': 'foo',
                    'metavar': 'bar',
                    'dest': 'arg'
                }
            }
        ]
    },
    'fixed-grid': {
        'cls': FixedGridSearch,
        'args': [
            {
                'args': ('--FixedGrid:lat-step',),
                'kwargs': {
                    'default': 5,
                    'metavar': 'step size',
                    'dest': 'FixedGrid:lat_step',
                    'type': float,
                    'help': 'Latitude step size for fixed grid. (Default: 5)'
                }
            },
            {
                'args': ('--FixedGrid:lon-step',),
                'kwargs': {
                    'default': 5,
                    'metavar': 'step size',
                    'dest': 'FixedGrid:lon_step',
                    'type': float,
                    'help': 'Longitude step size for fixed grid. (Default: 5)'
                }
            },
            {
                'args': ('--FixedGrid:no-lat-shift',),
                'kwargs': {
                    'action': 'store_false',
                    'dest': 'FixedGrid:lat_shift',
                    'help': 'Do not additionally shift latitude by half step'
                }
            },
            {
                'args': ('--FixedGrid:no-lon-shift',),
                'kwargs': {
                    'action': 'store_false',
                    'dest': 'FixedGrid:lon_shift',
                    'help': 'Do not additionally shift longitude by half step'
                }
            },
            {
                'args': ('--FixedGrid:time',),
                'kwargs': {
                    'default': '1d',
                    'metavar': 'timedelta',
                    'dest': 'FixedGrid:time',
                    'type': _parse_td,
                    'help': 'Length of time to bin observations in grid by. String parsable by pandas.Timedelta. '
                            '(Default: 1d)'
                }
            },
            # {
            #     'args': ('--FixedGrid:no-time-shift',),
            #     'kwargs': {
            #         'action': 'store_false',
            #         'dest': 'FixedGrid:time_shift',
            #         'help': 'Do not additionally shift time by half step'
            #     }
            # },
        ]
    }
}
