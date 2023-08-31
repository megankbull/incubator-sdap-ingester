# SDAP Granule Ingester

The SDAP Granule Ingester is a service that reads from a RabbitMQ queue for
YAML-formated string messages produced by the Collection Manager (`/collection_manager` 
in this repo). For each message consumed, this service will read a granule file from
disk and ingest it into SDAP by processing the granule and writing the resulting
data to Cassandra and Solr.

# Local setup

## Prerequisites 
Python 3.8.17

## Create conda enviornment 
    ```
    conda create -n granule-ingester 'python=3.8.17'
    conda activate granule-ingester
    ```

## Building the service
From `incubator-sdap-ingester`, run:
    ```
    cd common && python setup.py install
    cd ../granule_ingester && python setup.py install
    ```

## Launching the Granule Ingester
From `incubator-sdap-ingester`, run:
    ```
    python granule_ingester/granule_ingester/main.py -h
    ```
      
## Building the Docker image
From `incubator-sdap-ingester`, run:
    ```
    docker build . -f granule_ingester/docker/Dockerfile -t nexusjpl/granule-ingester
    ```
    
<!-- Not sure if the following instructions are still relevant -->
<!-- ## Running the tests
From `incubator-sdap-ingester`, run:

    $ cd common && python setup.py install
    $ cd ../granule_ingester && python setup.py install
    $ pip install pytest && pytest -->
    