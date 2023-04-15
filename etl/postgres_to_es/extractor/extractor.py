
import itertools
import logging
from abc import abstractmethod
from datetime import datetime
from typing import Generator

from psycopg2.extras import DictRow

from data.dataclasses import Movie

from .components.enricher import Enricher
from .components.merger import MovieMerger
from .components.producer import Producer


class BaseExtractor:
    """Abstract class for extractor process."""

    def __init__(self, db_connection) -> None:
        logging.debug("Initialize %s: \n\t%s", self.__class__.__name__, self.__doc__)
        self.db_connection = db_connection

    @abstractmethod
    def extract_data(self, entity, package_size) -> dict:
        """Extract all needed data for a given entity."""

    def _group_data(self, film_works: Generator[DictRow, None, None]) -> list:
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
    """Implementation of extractor process using multiple database query strategy."""

    def __init__(self, db_connection, entities_update_schema) -> None:
        self.producer = Producer(db_connection)
        self.enricher = Enricher(db_connection)
        self.merger = MovieMerger(db_connection)
        self.entities_update_schema = entities_update_schema
        super().__init__(db_connection)

    def extract_data(self) -> list:
        """Extract data implementation."""
        logging.debug("Extract data")

        test_datetime = datetime(1, 1, 1)  # TODO use state storage

        grouped_films = []
        for _, entity_update_schema in self.entities_update_schema.items():
            producer_schema = entity_update_schema.get('producer')
            if producer_schema:
                entity_ids = self.producer.extract_modified_entity_ids(
                    entity=producer_schema['entity_name'],
                    modified_from_timestamp=test_datetime,
                )
                try:
                    first_value = next(entity_ids)
                    entity_ids = itertools.chain([first_value], entity_ids)
                except StopIteration:
                    break
                entity_ids = (field.get('id') for field in entity_ids)

            enricher_schema = entity_update_schema.get('enricher')
            if enricher_schema:
                entity_ids = self.enricher.extract_child_entity_ids(
                    parent_entity_ids=entity_ids,
                    entity_parameters=enricher_schema,
                )
                entity_ids = (field.get('id') for field in entity_ids)

            film_work_rows = self.merger.aggregate_film_work_related_fields(
                entity_ids=entity_ids,
            )

            grouped_films.extend(self._group_data(film_works=film_work_rows))

        return grouped_films
