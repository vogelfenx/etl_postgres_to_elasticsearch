from dataclasses import asdict
from typing import List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from data.dataclasses import Movie
from loader.loader import Loader


class ElasticsearchLoader(Loader):
    """Loader implementation for Elasticsearch"""

    def __init__(self, host: dict, index_name: str, index_settings: dict):
        self.index_settings = index_settings
        self.index_name = index_name
        self.index_settings = index_settings

        es_client = Elasticsearch(host)

        super().__init__(connection=es_client)

    def load_data(self, documents: List[dict]):

        self._create_index()

        self._bulk_update_documents(documents=documents)

    def _create_index(self):
        es_client = self.connection

        if not es_client.indices.exists(index=self.index_name):
            es_client.indices.create(index=self.index_name, body=self.index_settings)

    def _bulk_update_documents(self, documents: List[Movie]):
        """
        Update multiple documents in an Elasticsearch index using a list of Python dictionaries.
        """

        # Define the update actions for each document
        actions = [
            {
                '_index': self.index_name,
                '_id': document.id,
                '_op_type': 'update',
                'doc_as_upsert': True,
                'doc': asdict(document),
            }
            for document in documents
        ]
        print(actions)
        # Perform the bulk update operation
        success_count, errors = bulk(self.connection, actions)

        # Check if there were any errors
        if errors:
            error_count = len(errors)
            raise ValueError(
                f'{error_count} errors occurred while updating documents in index {self.index_name}.')

        print(f'{success_count} documents in index {self.index_name} have been updated.')
