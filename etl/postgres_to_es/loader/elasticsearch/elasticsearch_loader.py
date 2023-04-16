import logging
from dataclasses import asdict
from typing import Any, List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from data.dataclasses import Movie
from loader.loader import Loader


class ElasticsearchLoader(Loader):
    """Loader implementation for Elasticsearch"""

    def __init__(self, host: dict, index_name: str, index_settings: dict):
        """
        Initialize an ElasticsearchLoader object.

        Args:
            host (dict): The Elasticsearch server connection parameters.
            index_name (str): The name of the Elasticsearch index to load data into.
            index_settings (dict): The settings for the Elasticsearch index.

        """
        self.index_settings = index_settings
        self.index_name = index_name
        self.index_settings = index_settings

        es_client = Elasticsearch(host)

        super().__init__(connection=es_client)

    def load_data(self, documents: List[dict]) -> None:
        """
        Load data to the target Elasticsearch index.

        Args:
            documents (List[dict]): A list of dictionaries containing the data to load.

        """
        self._create_index()

        try:
            self._bulk_update_documents(documents=documents)
        except ValueError as error:
            logging.error('%s: %s', error.__class__.__name__, error)

    def delete_outdated_data(self, source_data_provider: Any) -> None:
        """
        Delete outdated data from the Elasticsearch index.

        Args:
            source_data_provider (Any):
                The data source that contains the data to be compared and
                potentially deleted if outdated.
        """
        source_data_entity_ids = source_data_provider.select_all_entity_ids(entity='film_work')
        source_data_entity_ids = [doc.get('id') for doc in source_data_entity_ids]

        try:
            deleted_docs = self._delete_missing_docs_by_ids(source_data_entity_ids)
            if deleted_docs:
                logging.warning('The following obsolete documents were deleted: %s', deleted_docs)
        except ValueError as error:
            logging.error('%s: %s', error.__class__.__name__, error)

    def _create_index(self) -> None:
        """
        Create the Elasticsearch index if it doesn't exist.

        """
        es_client = self.connection

        if not es_client.indices.exists(index=self.index_name):
            es_client.indices.create(index=self.index_name, body=self.index_settings)

    def _bulk_update_documents(self, documents: List[Movie]) -> None:
        """
        Update multiple documents in an Elasticsearch index using a list of Python dictionaries.

        Args:
            documents (List[Movie]): A list of Movie objects to update in the Elasticsearch index.
        """
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

        success_count, errors = bulk(self.connection, actions)

        if errors:
            error_count = len(errors)
            raise ValueError(
                f'{error_count} errors occurred while updating documents in index {self.index_name}.'
            )

    def _delete_missing_docs_by_ids(self, docs_ids: List[str]) -> List[str]:
        """
        Delete documents from the Elasticsearch index that are missing from the source data.

        Args:
            docs_ids (List[str]): A list of IDs of documents to delete from the Elasticsearch index.

        Returns:
            List[str]: A list of IDs of documents that were deleted.
        """
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
