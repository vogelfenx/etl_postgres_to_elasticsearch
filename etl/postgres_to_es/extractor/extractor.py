
import logging
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, Generator, List

import pytz
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

    def _transform_film_works_to_dataclass(self, film_works: Generator[DictRow, None, None]) -> List[Movie]:
        """
        Groups data into a list of movies.

        Args:
            film_works (Generator[DictRow, None, None]): the extracted data

        Returns:
            List[Movie]: a list of grouped movies
        """
        movies = []
        for film_work in film_works:
            movie = Movie(
                id=film_work['fw_id'],
                imdb_rating=film_work['rating'],
                genre=film_work['genres'],
                title=film_work['title'],
                description=film_work['description'],
            )

            persons = film_work.get('persons')
            if persons:
                for person in persons:
                    person_role = person['person_role']
                    person_id = person['person_id']
                    person_name = person['full_name']

                    if person_role == 'actor':
                        movie.actors.append({'id': person_id, 'name': person_name})
                        movie.actors_names.append(person_name)
                    if person_role == 'writer':
                        movie.writers.append({'id': person_id, 'name': person_name})
                        movie.writers_names.append(person_name)
                    elif person_role == 'director':
                        movie.director = person_name

            movies.append(movie)

        return movies


class MultipleQueryExtractor(BaseExtractor):
    """
    Implementation of extractor process using multiple database query strategy.
    """
    last_state = defaultdict(lambda: datetime(1, 1, 1, tzinfo=pytz.timezone('UTC')))

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
            entity_name = producer_schema['entity_name']

            state_key = f'producer.{entity_name}'
            state_value = self.producer.state.get_state(key=state_key)

            if not state_value:
                state_value = datetime(1, 1, 1)

            if producer_schema:
                new_state_value, entity_ids = self.producer.extract_modified_entity_ids(
                    entity=entity_name,
                    modified_timestamp=state_value,
                )

            is_new_updated_entity = new_state_value > self.last_state[state_key]

            if is_new_updated_entity:
                LOGGER.debug("Next or new data found, continue extraction process")

                self.last_state[state_key] = new_state_value
                new_state_value = new_state_value.strftime('%Y-%m-%d %H:%M:%S')
                self.producer.state.set_state(state_key, new_state_value)

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

                movies = self._transform_film_works_to_dataclass(film_works=film_work_rows)
                grouped_films.extend(movies)

        return grouped_films
