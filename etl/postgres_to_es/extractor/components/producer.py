import logging
from datetime import datetime
from typing import Any, List

from state.state_manager import State


class Producer:
    """Fetch modified entity ids for a given entity in a given period of time."""

    def __init__(self, db_connection: Any, state: State) -> None:
        """
        Initialize a Producer object.

        Args:
            db_connection (Any): The database connection object.
            state (State): The state object.
        """
        logging.debug("Initialize %s", type(self).__name__)
        self.db_connection = db_connection
        self.state = state

    def extract_modified_entity_ids(self, *, entity: str) -> tuple[bool, List[int]]:
        """
        Extract the modified records ids for a given entity.

        Args:
            entity (str): The name of the entity to extract.

        Returns:
            Tuple[bool, List[int]]: A tuple containing a boolean indicating whether this is the last data chunk, 
            and a list of the modified entity ids.
        """
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
