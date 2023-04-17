from typing import Generator, List

from util.configuration import LOGGER


class MovieMerger:
    """
    Class to select all missing fields of the selected film works.
    """

    def __init__(self, db_connection) -> None:
        LOGGER.debug("Initialize %s", type(self).__name__)
        self.db_connection = db_connection

    def aggregate_film_work_related_fields(self, *, entity_ids: List[str]) -> Generator:
        """Aggregate all the film work related fields.

        Args:
            entity_ids (Generator): A generator object of entity IDs.

        Returns:
            Generator: A generator object of aggregated movies.
        """
        aggregated_movies = self.db_connection.select_film_work_related_fields(
            film_work_ids=entity_ids,
        )

        return aggregated_movies
