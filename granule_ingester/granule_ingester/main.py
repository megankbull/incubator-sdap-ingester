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

import argparse
import asyncio
import logging
import sys
from functools import partial
from typing import List

from granule_ingester.consumer import MessageConsumer, InsituConsumer
from granule_ingester.exceptions import FailedHealthCheckError, LostConnectionError
from granule_ingester.healthcheck import HealthCheck
from granule_ingester.insitu_clustering.Modules import modules as clustering_methods
from granule_ingester.insitu_clustering.Cluster import ClusterSearch
from granule_ingester.writers import CassandraStore, SolrStore
from granule_ingester.writers.ElasticsearchStore import ElasticsearchStore

logging.getLogger('asyncio').setLevel(logging.INFO)


# python main.py --rabbitmq-username user --rabbitmq-password bitnami --insitu preprocess -v
# python main.py --rabbitmq-username user --rabbitmq-password bitnami --cassandra-username cassandra --cassandra-password cassandra --insitu tile -v

def cassandra_factory(contact_points, port, keyspace, username, password):
    store = CassandraStore(contact_points=contact_points, port=port, keyspace=keyspace, username=username,
                           password=password)
    store.connect()
    return store


def solr_factory(solr_host_and_port, zk_host_and_port, collection='nexustiles'):
    store = SolrStore(zk_url=zk_host_and_port, collection=collection) if zk_host_and_port else SolrStore(
        solr_url=solr_host_and_port, collection=collection)
    store.connect()
    return store


def elasticsearch_factory(elastic_url, username, password, index):
    store = ElasticsearchStore(elastic_url, username, password, index)
    store.connect()
    return store


async def run_health_checks(dependencies: List[HealthCheck]):
    for dependency in dependencies:
        if not await dependency.health_check():
            return False
    return True


async def main(loop):
    parser = argparse.ArgumentParser(description='Listen to RabbitMQ for granule ingestion instructions, and process '
                                                 'and ingest a granule for each message that comes through.')
    main_args = parser.add_argument_group('Main args')

    # RABBITMQ
    rmq_args = parser.add_argument_group('RabbitMQ args')
    rmq_args.add_argument('--rabbitmq-host',
                          default='localhost',
                          metavar='HOST',
                          help='RabbitMQ hostname to connect to. (Default: "localhost")')
    rmq_args.add_argument('--rabbitmq-username',
                          default='guest',
                          metavar='USERNAME',
                          help='RabbitMQ username. (Default: "guest")')
    rmq_args.add_argument('--rabbitmq-password',
                          default='guest',
                          metavar='PASSWORD',
                          help='RabbitMQ password. (Default: "guest")')
    rmq_args.add_argument('--rabbitmq-queue',
                          default="nexus",
                          metavar="QUEUE",
                          help='Name of the RabbitMQ queue to consume from. (Default: "nexus")')

    # CASSANDRA
    cassandra_args = parser.add_argument_group('Cassandra args')
    cassandra_args.add_argument('--cassandra-contact-points',
                                default=['localhost'],
                                metavar="HOST",
                                nargs='+',
                                help='List of one or more Cassandra contact points, separated by spaces. (Default: "localhost")')
    cassandra_args.add_argument('--cassandra-port',
                                default=9042,
                                metavar="PORT",
                                help='Cassandra port. (Default: 9042)')
    cassandra_args.add_argument('--cassandra-keyspace',
                                default='nexustiles',
                                metavar='KEYSPACE',
                                help='Cassandra Keyspace (Default: "nexustiles")')
    cassandra_args.add_argument('--cassandra-username',
                                metavar="USERNAME",
                                default=None,
                                help='Cassandra username. Optional.')
    cassandra_args.add_argument('--cassandra-password',
                                metavar="PASSWORD",
                                default=None,
                                help='Cassandra password. Optional.')

    # METADATA STORE
    main_args.add_argument('--metadata-store',
                           default='solr',
                           metavar='STORE',
                           help='Which metadata store to use')

    # SOLR + ZK
    solr_args = parser.add_argument_group('Solr + ZK args')
    solr_args.add_argument('--solr-host-and-port',
                           default='http://localhost:8983',
                           metavar='HOST:PORT',
                           help='Solr host and port. (Default: http://localhost:8983)')
    solr_args.add_argument('--zk-host-and-port',
                           metavar="HOST:PORT")

    # ELASTIC
    elasticsearch_args = parser.add_argument_group('Elasticsearch args')
    elasticsearch_args.add_argument('--elastic-url',
                                    default='http://localhost:9200',
                                    metavar='ELASTIC_URL',
                                    help='ElasticSearch URL:PORT (Default: http://localhost:9200)')
    elasticsearch_args.add_argument('--elastic-username',
                                    metavar='ELASTIC_USER',
                                    help='ElasticSearch username')
    elasticsearch_args.add_argument('--elastic-password',
                                    metavar='ELASTIC_PWD',
                                    help='ElasticSearch password')
    elasticsearch_args.add_argument('--elastic-index',
                                    default='nexustiles',
                                    metavar='ELASTIC_INDEX',
                                    help='ElasticSearch index')

    # OTHERS
    main_args.add_argument('--max-threads',
                           default=16,
                           metavar='MAX_THREADS',
                           help='Maximum number of threads to use when processing granules. (Default: 16)')
    main_args.add_argument('-v',
                           '--verbose',
                           action='store_true',
                           help='Print verbose logs.')

    # Insitu
    insitu_args = parser.add_argument_group('Insitu args')
    insitu_args.add_argument('--insitu',
                             default=None,
                             choices=['preprocess', 'cluster', 'tile'],
                             metavar='INSITU_MODE',
                             dest='insitu',
                             help='Run granule ingester for insitu data in the specified mode: \n'
                                  'preprocess: Load insitu JSON from RMQ and stage in Solr.\n'
                                  'cluster: Detect clusters of insitu observations to make into tiles.\n'
                                  'tile: Create and write tile data.')
    insitu_args.add_argument('--insitu-queue',
                             default='cluster',
                             metavar='insitu rmq queue',
                             dest='insitu_rmq',
                             help='RMQ queue for insitu clusters. (Default: cluster)')
    insitu_args.add_argument('--insitu-stage-queue',
                             default='insitu-stage',
                             metavar='insitu rmq queue',
                             dest='insitu_rmq_stage',
                             help='RMQ queue for insitu input JSON files. (Default: insitu-stage)')
    insitu_args.add_argument('--insitu-collection',
                             default='insitutiles',
                             dest='insitu_solr',
                             help='Solr collection for insitu tiles. (Default: insitutiles)')
    insitu_args.add_argument('--insitu-stage',
                             default='insitustage',
                             dest='insitu_stage',
                             help='Solr collection to stage insitu observations. (Default: insitustage)')
    insitu_args.add_argument('--cluster-method',
                             default=None,
                             choices=list(clustering_methods.keys()),
                             metavar='method',
                             dest='cluster_method',
                             help='Method to use for determining insitu observation clusters. For a list of available '
                                  'methods, use --list-cluster-methods')
    insitu_args.add_argument('--list-cluster-methods',
                             action='store_true',
                             dest='list_methods',
                             help='List available clustering methods and exit')

    # Insitu limiting
    insitu_args.add_argument('--stage-limit',
                             default=500000,
                             metavar='observations',
                             dest='stage_limit',
                             type=int,
                             help='Soft limit for number of observations staged in Solr. Can be exceeded, but no '
                                  'further inputs will be processed until the number of staged observations falls below'
                                  ' this value. (Default: 500,000)')
    insitu_args.add_argument('--stage-limit-hysteresis',
                             default=100000,
                             metavar='observations',
                             dest='stage_limit_hysteresis',
                             type=int,
                             help=argparse.SUPPRESS)
    insitu_args.add_argument('--min-tile-size',
                             default=250,
                             metavar='observations',
                             dest='tile_min',
                             type=int,
                             help='Min number of observations required for an insitu tile. (Default: 250)')
    insitu_args.add_argument('--max-tile-size',
                             default=0,
                             metavar='observations',
                             dest='tile_limit',
                             type=int,
                             help='Max number of observations per insitu tile. A value of 0 or less will result in no '
                                  'limit. (Default: 0)')

    insitu_clustering_args = parser.add_argument_group('Insitu Clustering args')

    for method in list(clustering_methods.keys()):
        # method_grp = insitu_clustering_args.add_argument_group(f'{method} args')
        for args in clustering_methods[method]['args']:
            insitu_clustering_args.add_argument(*args['args'], **args['kwargs'])

    args = parser.parse_args()

    if args.list_methods:
        for m in list(clustering_methods.keys()):
            print(m)
        return

    logging_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=logging_level)
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        logger.setLevel(logging_level)

    logger = logging.getLogger(__name__)

    config_values_str = "\n".join(["{} = {}".format(arg, getattr(args, arg)) for arg in vars(args)])
    logger.info("Using configuration values:\n{}".format(config_values_str))
    cassandra_username = args.cassandra_username
    cassandra_password = args.cassandra_password
    cassandra_contact_points = args.cassandra_contact_points
    cassandra_port = args.cassandra_port
    cassandra_keyspace = args.cassandra_keyspace

    metadata_store = args.metadata_store

    solr_host_and_port = args.solr_host_and_port
    zk_host_and_port = args.zk_host_and_port

    elastic_url = args.elastic_url
    elastic_username = args.elastic_username
    elastic_password = args.elastic_password
    elastic_index = args.elastic_index

    insitu_mode = args.insitu

    if insitu_mode is None:
        if metadata_store == 'solr':
            consumer = MessageConsumer(rabbitmq_host=args.rabbitmq_host,
                                       rabbitmq_username=args.rabbitmq_username,
                                       rabbitmq_password=args.rabbitmq_password,
                                       rabbitmq_queue=args.rabbitmq_queue,
                                       data_store_factory=partial(cassandra_factory,
                                                                  cassandra_contact_points,
                                                                  cassandra_port,
                                                                  cassandra_keyspace,
                                                                  cassandra_username,
                                                                  cassandra_password),
                                       metadata_store_factory=partial(solr_factory, solr_host_and_port,
                                                                      zk_host_and_port))
            try:
                solr_store = SolrStore(zk_url=zk_host_and_port) if zk_host_and_port else SolrStore(
                    solr_url=solr_host_and_port)
                await run_health_checks([CassandraStore(cassandra_contact_points,
                                                        cassandra_port,
                                                        cassandra_keyspace,
                                                        cassandra_username,
                                                        cassandra_password),
                                         solr_store,
                                         consumer])
                async with consumer:
                    logger.info(
                        "All external dependencies have passed the health checks. Now listening to message queue.")
                    await consumer.start_consuming(args.max_threads)
            except FailedHealthCheckError as e:
                logger.error(f"Quitting because not all dependencies passed the health checks: {e}")
            except LostConnectionError as e:
                logger.error(f"{e} Any messages that were being processed have been re-queued. Quitting.")
            except Exception as e:
                logger.exception(f"Shutting down because of an unrecoverable error:\n{e}")
            finally:
                sys.exit(1)

        else:
            consumer = MessageConsumer(rabbitmq_host=args.rabbitmq_host,
                                       rabbitmq_username=args.rabbitmq_username,
                                       rabbitmq_password=args.rabbitmq_password,
                                       rabbitmq_queue=args.rabbitmq_queue,
                                       data_store_factory=partial(cassandra_factory,
                                                                  cassandra_contact_points,
                                                                  cassandra_port,
                                                                  cassandra_keyspace,
                                                                  cassandra_username,
                                                                  cassandra_password),
                                       metadata_store_factory=partial(elasticsearch_factory,
                                                                      elastic_url,
                                                                      elastic_username,
                                                                      elastic_password,
                                                                      elastic_index))
            try:
                es_store = ElasticsearchStore(elastic_url, elastic_username, elastic_password, elastic_index)
                await run_health_checks([CassandraStore(cassandra_contact_points,
                                                        cassandra_port,
                                                        cassandra_keyspace,
                                                        cassandra_username,
                                                        cassandra_password),
                                         es_store,
                                         consumer])

                async with consumer:
                    logger.info(
                        "All external dependencies have passed the health checks. Now listening to message queue.")
                    await consumer.start_consuming(args.max_threads)
            except FailedHealthCheckError as e:
                logger.error(f"Quitting because not all dependencies passed the health checks: {e}")
            except LostConnectionError as e:
                logger.error(f"{e} Any messages that were being processed have been re-queued. Quitting.")
            except Exception as e:
                logger.exception(f"Shutting down because of an unrecoverable error:\n{e}")
            finally:
                sys.exit(1)
    elif insitu_mode == 'preprocess':
        consumer = InsituConsumer(message_type='preprocess',
                                  rabbitmq_host=args.rabbitmq_host,
                                  rabbitmq_username=args.rabbitmq_username,
                                  rabbitmq_password=args.rabbitmq_password,
                                  rabbitmq_queue=args.insitu_rmq_stage,
                                  solr_url=args.solr_host_and_port,
                                  solr_collection=args.insitu_stage,
                                  metadata_store_factory=partial(
                                      solr_factory,
                                      solr_host_and_port,
                                      zk_host_and_port,
                                      args.insitu_stage
                                  ))
        try:
            logger.info('Running health checks...')
            await run_health_checks([
                consumer,
                solr_factory(solr_host_and_port, zk_host_and_port, args.insitu_stage)
            ])
            async with consumer:
                logger.info("All external dependencies have passed the health checks. Now listening to message queue.")
                await consumer.start_consuming()
        except FailedHealthCheckError as e:
            logger.error(f"Quitting because not all dependencies passed the health checks: {e}")
        except LostConnectionError as e:
            logger.error(f"{e} Any messages that were being processed have been re-queued. Quitting.")
        except Exception as e:
            logger.exception(f"Shutting down because of an unrecoverable error:\n{e}")
        finally:
            sys.exit(1)
    elif insitu_mode == 'cluster':
        solr_stage = solr_factory(solr_host_and_port, zk_host_and_port, args.insitu_stage)
        await run_health_checks([solr_stage])

        cluster: ClusterSearch = clustering_methods[args.cluster_method]['cls'](args, solr_stage.get_solr())

        cluster.start_detecting()
    elif insitu_mode == 'tile':
        data_store_factory = partial(cassandra_factory,
                                     cassandra_contact_points,
                                     cassandra_port,
                                     cassandra_keyspace,
                                     cassandra_username,
                                     cassandra_password)

        consumer = InsituConsumer(message_type='tile',
                                  rabbitmq_host=args.rabbitmq_host,
                                  rabbitmq_username=args.rabbitmq_username,
                                  rabbitmq_password=args.rabbitmq_password,
                                  rabbitmq_queue=args.insitu_rmq,
                                  solr_url=args.solr_host_and_port,
                                  solr_collection={'stage': args.insitu_stage, 'store': args.insitu_solr},
                                  metadata_store_factory=partial(
                                      solr_factory,
                                      solr_host_and_port,
                                      zk_host_and_port,
                                      args.insitu_solr
                                  ),
                                  data_store_factory=data_store_factory,
                                  stage_factory=partial(
                                      solr_factory,
                                      solr_host_and_port,
                                      zk_host_and_port,
                                      args.insitu_stage
                                  ))
        try:
            logger.info('Running health checks...')
            await run_health_checks([
                consumer,
                solr_factory(solr_host_and_port, zk_host_and_port, args.insitu_stage),
                solr_factory(solr_host_and_port, zk_host_and_port, args.insitu_solr),
                data_store_factory()
            ])
            async with consumer:
                logger.info("All external dependencies have passed the health checks. Now listening to message queue.")
                await consumer.start_consuming()
        except FailedHealthCheckError as e:
            logger.error(f"Quitting because not all dependencies passed the health checks: {e}")
        except LostConnectionError as e:
            logger.error(f"{e} Any messages that were being processed have been re-queued. Quitting.")
        except Exception as e:
            logger.exception(f"Shutting down because of an unrecoverable error:\n{e}")
        finally:
            sys.exit(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
