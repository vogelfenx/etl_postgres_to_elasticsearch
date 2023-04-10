import logging
from abc import ABC, abstractmethod
from datetime import datetime

from .components.producer import Producer


class BaseExtractor(ABC):
    """Abstract class for extractor process."""

    def __init__(self, db_connection) -> None:
        logging.debug("Initialize %s: \n\t%s", self.__class__.__name__, self.__doc__)
        self.db_connection = db_connection

    @abstractmethod
    def extract_data(self, entity, package_size) -> dict:
        """Extract all needed data for a given entity."""


class MultipleQueryExtractor(BaseExtractor):
    """Implementation of extractor process using multiple database query strategy."""

    def extract_data(self, *, entity: str, package_size: int = None) -> dict:
        """Extract data implementation."""
        logging.debug("Extract data for entity: %s, package_size: %s", entity, package_size)

        test_datetime = datetime(2023, 4, 10)  # TODO use state storage

        producer = Producer(db_connection=self.db_connection, entity=entity)
        modified_records_ids = producer.select_modified_entity_records_ids(
            modified_from=test_datetime)

        return {}
