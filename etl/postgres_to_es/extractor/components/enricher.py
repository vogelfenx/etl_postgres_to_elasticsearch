import logging
from abc import ABC, abstractmethod
from typing import List


class BaseEnricher(ABC):
    """Select all ids of the related entity (producer)."""

    def __init__(self, db_connection, state=None) -> None:
        logging.debug("Initialize %s: \n\t%s", self.__class__.__name__, self.__doc__)
        self.db_connection = db_connection
        self.state = state

    @abstractmethod
    def extract_child_entity_ids(self, entity_ids) -> List:
        """Extract all related entity IDs that match the given m2m entity IDs."""


class Enricher(BaseEnricher):

    # За всеми фильмами и сериалами, в которых приняли участие выбранные люди.

    # SELECT fw.id, fw.modified
    # FROM content.film_work fw
    # LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    # WHERE pfw.person_id IN (<id_всех_людей>)
    # ORDER BY fw.modified
    # LIMIT 100;

    def extract_child_entity_ids(
            self,
            *,
            parent_entity_ids,
            entity_parameters,
    ) -> List:

        child_entity_ids = self.db_connection.select_related_entity_ids(
            **entity_parameters,
            parent_entity_ids=parent_entity_ids,
        )
        try:
            child_entity_ids = [row['id'] for row in child_entity_ids]
        except StopIteration:
            logging.debug('End of data reached, the last batch of data is collected')

        return child_entity_ids
