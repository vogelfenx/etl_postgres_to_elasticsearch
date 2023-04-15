from dataclasses import asdict
from typing import List, Set, Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from data.dataclasses import Movie
from loader.loader import Loader
import logging


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

        try:
            self._bulk_update_documents(documents=documents)
        except ValueError as error:
            logging.error('%s: %s', error.__class__.__name__, error)

    def delete_outdated_data(self, source_data_provider: Any) -> None:
        """The data source that contains the data to be compared and potentially deleted if outdated."""
        source_data_entity_ids = source_data_provider.select_all_entity_ids(entity='film_work')
        source_data_entity_ids = [doc.get('id') for doc in source_data_entity_ids]

        try:
            deleted_docs = self._delete_missing_docs_by_ids(source_data_entity_ids)
            if deleted_docs:
                logging.warning('The following obsolete documents were deleted: %s', deleted_docs)
        except ValueError as error:
            logging.error('%s: %s', error.__class__.__name__, error)

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
        # Perform the bulk update operation
        success_count, errors = bulk(self.connection, actions)

        # Check if there were any errors
        if errors:
            error_count = len(errors)
            raise ValueError(
                f'{error_count} errors occurred while updating documents in index {self.index_name}.'
            )

    def _delete_missing_docs_by_ids(self, docs_ids: List) -> List:
        query = {
            'query': {
                'bool': {
                    'must_not': [
                        {
                            'terms': {
                                '_id': docs_ids,
                            },
                        },
                    ],
                },
            },
        }

        response = self.connection.search(index=self.index_name, body=query)
        not_found_docs_hits = response['hits']['hits']

        not_found_docs_ids = [doc['_id'] for doc in not_found_docs_hits]

        actions = [
            {'_index': self.index_name, '_id': doc_id, '_op_type': 'delete'}
            for doc_id in not_found_docs_ids
        ]

        _, errors = bulk(self.connection, actions)

        if errors:
            error_count = len(errors)
            raise ValueError(
                f'{error_count} errors occurred while updating documents in index {self.index_name}.'
            )

        return not_found_docs_ids
