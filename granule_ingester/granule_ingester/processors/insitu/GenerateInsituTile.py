from nexusproto.DataTile_pb2 import InsituTile, InsituTileObservation, NexusTile
from typing import List
from granule_ingester.granule_loaders import InsituLoader


MAX_SOLR_FQ = 150


class GenerateInsituTile:
    def __init__(self, solr):
        self._solr = solr

    def generate_tile(self, uuids: List[str]) -> NexusTile:
        pass

        # Batched fetch of data by uuid (combine into one big list)
        # Group by file_url
        # For each file url get data and build InsituTileObservations
        # Add InsituTileObservations to InsituTile
        # Add InsituTile to NexusTile
        # Return NexusTile

