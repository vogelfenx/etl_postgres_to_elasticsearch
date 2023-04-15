import logging
from abc import ABC, abstractmethod
from typing import Generator


class BaseMerger(ABC):
    """Select all missing fields of the selected entities (producer & enricher)."""

    def __init__(self, db_connection) -> None:
        logging.debug("Initialize %s", type(self).__name__)
        self.db_connection = db_connection

    @abstractmethod
    def aggregate_film_work_related_fields(self, *, entity_ids: Generator) -> Generator:
        """_summary_.

        Args:
            entity_ids (list): _description_
        """


class MovieMerger(BaseMerger):

    def aggregate_film_work_related_fields(self, *, entity_ids: Generator) -> Generator:
        aggregated_movies = self.db_connection.select_film_work_related_fields(
            film_work_ids=entity_ids,
        )

        return aggregated_movies
