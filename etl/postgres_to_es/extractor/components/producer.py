import logging
from abc import ABC, abstractmethod
from datetime import datetime


class BaseProducer(ABC):
    """Select all modified ids of an entity in a given period of time."""

    def __init__(self, *, db_connection, entity) -> None:
        logging.debug("Initialize %s", type(self).__name__)

        self.entity = entity
        self.db_connection = db_connection

    @abstractmethod
    def select_modified_entity_records_ids(self, modified_from: datetime) -> list:
        """Select all modified ids of an entity in a given period of time.

        Args:
            modified_from (datetime): Select condition of modified records by datetime

        Returns:
            list: ids of last modified entity records by the given time period.
        """


class Producer(BaseProducer):
    """Select ids of all modified persons in a given period of time."""

    def select_modified_entity_records_ids(self, *, modified_from: datetime) -> list:
        """Select all modified person ids in a given period of time.

        Args:
            modified_from (datetime): Select condition of modified records by datetime

        Returns:
            list: ids of last modified entity records by the given time period.
        """
        logging.debug('perform select_modified_entity_records_ids():\n\t%s', self.__doc__)

        # SELECT last modified entity's IDS
        ids = self.db_connection.select_entity_fields(
            entity=self.entity, fields=['id'], condition=f'modified>\'{modified_from}\'')

        return ids
