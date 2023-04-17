from typing import Dict, List

from util.configuration import LOGGER


class Enricher:

    def __init__(self, db_connection, state=None) -> None:
        LOGGER.debug("Initialize %s: \n\t%s", self.__class__.__name__, self.__doc__)
        self.db_connection = db_connection
        self.state = state

    def extract_child_entity_ids(
            self,
            *,
            parent_entity_ids: List[int],
            entity_parameters: Dict,
    ) -> List[int]:
        """
        Extract the child entity IDs based on the provided parent entity IDs and entity parameters.

        Args:
            parent_entity_ids: A list of integers representing parent entity IDs.
            entity_parameters: A dictionary containing parameters to filter the child entities.

        Returns:
            related child entity IDs.
        """

        child_entity_ids = self.db_connection.select_related_entity_ids(
            **entity_parameters,
            parent_entity_ids=parent_entity_ids,
        )

        child_entity_ids = [row['id'] for row in child_entity_ids]

        return child_entity_ids
