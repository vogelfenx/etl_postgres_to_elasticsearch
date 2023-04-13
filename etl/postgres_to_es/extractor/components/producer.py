import logging
from abc import ABC, abstractmethod
from datetime import datetime


class BaseProducer(ABC):
    """Select all modified ids of an entity in a given period of time."""

    def __init__(self, *, db_connection) -> None:
        logging.debug("Initialize %s", type(self).__name__)

        self.db_connection = db_connection

    @abstractmethod
    def extract_last_modified_entity_ids(self, *, modified_from_timestamp: datetime) -> list:
        """Select all modified ids of an entity in a given period of time.

        Args:
            modified_from (datetime): Select condition of modified records by datetime

        Returns:
            list: ids of last modified entity records by the given time period.
        """


class PersonProducer(BaseProducer):

    def extract_last_modified_entity_ids(self, *, modified_from_timestamp: datetime) -> list:
        last_modified_entity_ids = self.db_connection.select_last_modified_entity_ids(
            entity='person',
            modified_timestamp=modified_from_timestamp,
        )
        return last_modified_entity_ids
