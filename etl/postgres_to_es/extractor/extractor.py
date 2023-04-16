
import logging
from abc import abstractmethod
from typing import Generator, Any, List

from psycopg2.extras import DictRow

from data.dataclasses import Movie
from state.state_manager import State

from .components.enricher import Enricher
from .components.merger import MovieMerger
from .components.producer import Producer


class BaseExtractor:
    """
    Abstract class for extractor process.
    """

    def __init__(self, db_connection: Any) -> None:
        """
        Initializes the BaseExtractor class.

        Args:
            db_connection (Any): the database connection object
        """
        logging.debug("Initialize %s: \n\t%s", self.__class__.__name__, self.__doc__)
        self.db_connection = db_connection

    @abstractmethod
    def extract_data(self, entity: str, package_size: int) -> dict:
        """
        Extracts all needed data for a given entity.

        Args:
            entity (str): the entity for which to extract data
            package_size (int): the size of the data package to extract

        Returns:
            dict: the extracted data
        """

    def _group_data(self, film_works: Generator[DictRow, None, None]) -> List[Movie]:
        """
        Groups the extracted data into a list of movies.

        Args:
            film_works (Generator[DictRow, None, None]): the extracted data to group

        Returns:
            List[Movie]: a list of grouped movies
        """
        movies = []

        for film_work in film_works:

            film_work_id = film_work.get('fw_id')
            film_work_title = film_work.get('title')
            film_work_description = film_work.get('description')
            film_work_genre = film_work.get('genre')
            imdb_rating = film_work.get('rating')

            movie = next((movie for movie in movies if movie.id == film_work_id), None)

            if not movie:
                movie = Movie(
                    id=film_work_id,
                    imdb_rating=imdb_rating,
                    genre=[film_work_genre],
                    title=film_work_title,
                    description=film_work_description,
                )
                movies.append(movie)

            person_id = film_work.get('person_id')
            person_name = film_work.get('full_name')
            person_role = film_work.get('role')

            if person_role == 'actor':
                if person_name not in movie.actors_names:
                    movie.actors.append({'id': person_id, 'name': person_name})
                    movie.actors_names.append(person_name)
            elif person_role == 'writer':
                if person_name not in movie.writers_names:
                    movie.writers.append({'id': person_id, 'name': person_name})
                    movie.writers_names.append(person_name)
            elif person_role == 'director':
                movie.director = person_name

            if film_work_genre not in movie.genre:
                movie.genre.append(film_work_genre)
        return movies


class MultipleQueryExtractor(BaseExtractor):
    """
    Implementation of extractor process using multiple database query strategy.
    """

    def __init__(self, db_connection: Any, persistant_state_storage: dict, entities_update_schema: dict) -> None:
        """
        Initializes the MultipleQueryExtractor class.

        Args:
            db_connection (Any): the database connection object
            persistant_state_storage (dict): the persistent state storage object
            entities_update_schema (dict): the schema for updating entities
        """
        producer_state = State(storage=persistant_state_storage)

        self.producer = Producer(db_connection, producer_state)
        self.enricher = Enricher(db_connection)
        self.merger = MovieMerger(db_connection)

        self.entities_update_schema = entities_update_schema

        super().__init__(db_connection)

    def extract_data(self) -> tuple[bool, List[Movie]]:
        """
        Extracts data.

        Returns:
            A tuple with two elements:
            - A boolean indicating whether this is the last chunk of data to be extracted.
            - A list of Movie objects extracted from the database.
        """
        logging.debug('Extract data')

        grouped_films = []
        for _, entity_update_schema in self.entities_update_schema.items():
            producer_schema = entity_update_schema.get('producer')
            if producer_schema:
                is_last_data_chunk, entity_ids = self.producer.extract_modified_entity_ids(
                    entity=producer_schema['entity_name'],
                )

            if entity_ids:
                enricher_schema = entity_update_schema.get('enricher')
                if enricher_schema:
                    entity_ids = self.enricher.extract_child_entity_ids(
                        parent_entity_ids=entity_ids,
                        entity_parameters=enricher_schema,
                    )

                film_work_rows = self.merger.aggregate_film_work_related_fields(
                    entity_ids=entity_ids,
                )

                grouped_films.extend(self._group_data(film_works=film_work_rows))

        return (is_last_data_chunk, grouped_films)
