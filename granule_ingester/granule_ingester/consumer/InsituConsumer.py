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

import aio_pika
import dateutil.parser as dp
import requests
from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from granule_ingester.exceptions import RabbitMQLostConnectionError, \
    RabbitMQFailedHealthCheckError, LostConnectionError
from granule_ingester.healthcheck import HealthCheck
from granule_ingester.granule_loaders import InsituLoader

logger = logging.getLogger(__name__)


class InsituConsumer(HealthCheck):
    def __init__(self,
                 message_type,
                 rabbitmq_host,
                 rabbitmq_username,
                 rabbitmq_password,
                 rabbitmq_queue,
                 solr_url,
                 solr_collection,
                 metadata_store_factory,
                 data_store_factory=None,):
        self._rabbitmq_queue = rabbitmq_queue
        self._metadata_store_factory = metadata_store_factory
        self._data_store_factory = data_store_factory

        self._connection_string = "amqp://{username}:{password}@{host}/".format(username=rabbitmq_username,
                                                                                password=rabbitmq_password,
                                                                                host=rabbitmq_host)
        self._connection: aio_pika.Connection = None

        self._type = message_type

        self._url_prefix = f"{solr_url.strip('/')}/solr"
        self._collection = solr_collection
        self._create_collection_if_needed(self._type)

    async def health_check(self) -> bool:
        try:
            connection = await self._get_connection()
            await connection.close()
            return True
        except Exception:
            raise RabbitMQFailedHealthCheckError(f"Cannot connect to RabbitMQ! "
                                                 f"Connection string was {self._connection_string}")

    async def _get_connection(self) -> aio_pika.Connection:
        return await aio_pika.connect_robust(self._connection_string)

    async def __aenter__(self):
        self._connection = await self._get_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            await self._connection.close()

    def _create_collection_if_needed(self, message_type):
        try:
            session = requests.session()

            logger.debug('Checking if needed collection is present in Solr')

            payload = {'action': 'CLUSTERSTATUS'}
            collections_endpoint = f"{self._url_prefix}/admin/collections"
            result = session.get(collections_endpoint, params=payload)
            response = result.json()
            node_number = len(response['cluster']['live_nodes'])

            existing_collections = response['cluster']['collections'].keys()

            if self._collection not in existing_collections:
                logger.warning('Collection is not present, creating it now')

                # Create collection
                payload = {'action': 'CREATE',
                           'name': self._collection,
                           'numShards': node_number
                           }
                result = session.get(collections_endpoint, params=payload)
                response = result.json()
                logger.info(f"solr collection created {response}")

                # Update schema
                schema_endpoint = f"{self._url_prefix}/{self._collection}/schema"

                field_type_payload = json.dumps({
                    "add-field-type": {
                        "name": "geo",
                        "class": "solr.SpatialRecursivePrefixTreeFieldType",
                        "geo": "true",
                        "precisionModel": "fixed",
                        "maxDistErr": "0.000009",
                        "spatialContextFactory": "com.spatial4j.core.context.jts.JtsSpatialContextFactory",
                        "precisionScale": "1000",
                        "distErrPct": "0.025",
                        "distanceUnits": "degrees"}})

                logging.info("Creating field-type 'geo'...")
                field_type_response = requests.post(url=schema_endpoint, data=field_type_payload)
                logging.debug(field_type_response.status_code)

                if message_type == 'preprocess':
                    self._add_field(schema_endpoint, "dataset_s", "string", session)
                    self._add_field(schema_endpoint, "project_s", "string", session)
                    self._add_field(schema_endpoint, "provider_s", "string", session)
                    self._add_field(schema_endpoint, "file_url", "string", session)
                    self._add_field(schema_endpoint, "index_i", "pint", session)
                    self._add_field(schema_endpoint, "geo", "geo", session)
                    self._add_field(schema_endpoint, "time_dt", "pdate", session)
                    self._add_field(schema_endpoint, "time", "plong", session)
                elif message_type == 'tile':
                    pass
                    # TODO tile schema
        except requests.exceptions.RequestException as e:
            logger.error(f"solr instance unreachable {self._url_prefix}")
            raise e

    def _add_field(self, schema_url, field_name, field_type, session):
        """
        Helper to add a string field in a solr schema
        :param schema_url:
        :param field_name:
        :param field_type
        :return:
        """
        logger.debug(f'Adding field {field_name} of type {field_type}')

        add_field_payload = {
            "add-field": {
                "name": field_name,
                "type": field_type
            }
        }

        response = session.post(schema_url, data=str(add_field_payload).encode('utf-8'))

        logger.debug(response.text)

        return response

    @staticmethod
    async def _received_message(message: aio_pika.IncomingMessage,
                                data_store_factory,
                                metadata_store_factory,
                                message_type):
        message_str = message.body.decode('utf-8')
        logger.debug(message_str)
        input_config = load(message_str, Loader=Loader)

        if type(input_config) is not dict:
            raise ValueError("Received message is not valid YAML.")

        try:
            if message_type == 'preprocess':
                dataset_s = input_config['granule']['dataset']
                file_url = input_config['granule']['resource']

                loader = InsituLoader(resource=file_url)

                observation_docs = []

                async with loader as (insitu_data, file):
                    project = insitu_data['project']
                    provider = insitu_data['provider']

                    for i, observation in enumerate(insitu_data['observations']):
                        # time_dt, time, id
                        geo = 'POINT({:.3f} {:.3f})'.format(round(observation['longitude'], 3),
                                                            round(observation['latitude'], 3))
                        time_dt = observation['time']
                        time = int(dp.parse(time_dt).timestamp())

                        observation_id = uuid.uuid3(uuid.NAMESPACE_DNS,
                                                    dataset_s + file_url + geo + time_dt + project + provider + str(i))

                        observation_docs.append({
                            "dataset_s": dataset_s,
                            "project_s": project,
                            "provider_s": provider,
                            "file_url": file_url,
                            "index_i": i,
                            "id": str(observation_id),
                            "geo": geo,
                            "time_dt": time_dt,
                            "time": time
                        })

                solr = metadata_store_factory()

                await solr.save_batch_insitu(observation_docs)
            elif message_type == 'tile':
                pass
        except KeyError:
            logger.error('Invalid message')
            raise
        except LostConnectionError:
            # Let main() handle this
            raise
        except Exception as e:
            await message.reject(requeue=True)
            logger.exception(f"Processing message failed. Message will be re-queued. The exception was:\n{e}")

    async def start_consuming(self):
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(self._rabbitmq_queue, durable=True, arguments={'x-max-priority': 10})
        queue_iter = queue.iterator()
        async for message in queue_iter:
            try:
                await self._received_message(message,
                                             self._data_store_factory,
                                             self._metadata_store_factory,
                                             self._type)
            except aio_pika.exceptions.MessageProcessError:
                # Do not try to close() the queue iterator! If we get here, that means the RabbitMQ
                # connection has died, and attempting to close the queue will only raise another exception.
                raise RabbitMQLostConnectionError("Lost connection to RabbitMQ while processing a granule.")
            except Exception as e:
                await queue_iter.close()
                await channel.close()
                raise e
