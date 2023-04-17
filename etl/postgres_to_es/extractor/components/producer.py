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

    def extract_modified_entity_ids(self, *, entity: str, modified_timestamp) -> tuple[bool, List[int]]:
        """
        Extract the modified records ids for a given entity.

        Args:
            entity (str): The name of the entity to extract.

        Returns:
            Tuple[bool, List[int]]: A tuple containing a boolean indicating whether this is the last data chunk, 
            and a list of the modified entity ids.
        """
        last_modified_entity_ids = self.db_connection.select_last_modified_entity_ids(
            entity=entity,
            modified_timestamp=modified_timestamp,
        )

        is_last_data_chunk = next(last_modified_entity_ids).get('is_last_data_chunk')
        last_modified_entity_ids = list(last_modified_entity_ids)

        if last_modified_entity_ids:
            last_modified_timestamp = last_modified_entity_ids[-1].get('modified')

        last_modified_entity_ids = [row['id'] for row in last_modified_entity_ids]
        return (last_modified_timestamp, last_modified_entity_ids)
