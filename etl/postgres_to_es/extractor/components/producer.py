import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List


class BaseProducer(ABC):
    """Select all modified ids of an entity in a given period of time."""

    def __init__(self, db_connection, state) -> None:
        logging.debug("Initialize %s", type(self).__name__)
        self.db_connection = db_connection
        self.state = state

    @abstractmethod
    def extract_modified_entity_ids(self, *, modified_from_timestamp: datetime) -> list:
        """Select all modified ids of an entity in a given period of time.

        Args:
            modified_from (datetime): Select condition of modified records by datetime

        Returns:
            list: ids of last modified entity records by the given time period.
        """


class Producer(BaseProducer):

    def extract_modified_entity_ids(self, *, entity: str) -> List:

        producer_state_key = f'producer.{entity}'
        last_processed_modified_field = self.state.get_state(key=producer_state_key)

        if not last_processed_modified_field:
            last_processed_modified_field = datetime(1, 1, 1)

        last_modified_entity_ids = self.db_connection.select_last_modified_entity_ids(
            entity=entity,
            modified_timestamp=last_processed_modified_field,
        )

        is_last_data_chunk = next(last_modified_entity_ids).get('is_last_data_chunk')
        last_modified_entity_ids = list(last_modified_entity_ids)

        if last_modified_entity_ids:
            state_value = last_modified_entity_ids[-1].get('modified').strftime('%Y-%m-%d %H:%M:%S')
            self.state.set_state(producer_state_key, state_value)

        last_modified_entity_ids = [row['id'] for row in last_modified_entity_ids]
        return (is_last_data_chunk, last_modified_entity_ids)
