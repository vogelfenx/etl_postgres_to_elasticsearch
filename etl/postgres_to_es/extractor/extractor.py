
import logging
from abc import abstractmethod
from datetime import datetime
from typing import Generator

from psycopg2.extras import DictRow

from .components.enricher import PersonEnricher
from .components.merger import PersonMerger
from .components.producer import PersonProducer
from data.dataclasses import Movie


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
                    genres=[film_work_genre],
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

            if film_work_genre not in movie.genres:
                movie.genres.append(film_work_genre)
        return movies


class MultipleQueryExtractor(BaseExtractor):
    """Implementation of extractor process using multiple database query strategy."""

    def extract_data(self) -> list:
        """Extract data implementation."""
        logging.debug("Extract data")

        test_datetime = datetime(2023, 4, 10)  # TODO use state storage

        etl_proccesses = {
            'updatePerson': {
                'producer': PersonProducer,
                'enricher': PersonEnricher,
                'merger': PersonMerger,
            },
            # 'updateGenre': {},
            # 'updateMovie': {},
        }

        for _, etl_process in etl_proccesses.items():
            producer = etl_process.get('producer')(db_connection=self.db_connection)
            enricher = etl_process.get('enricher')(db_connection=self.db_connection)
            merger = etl_process.get('merger')(db_connection=self.db_connection)

            producer_entity_ids = producer.extract_last_modified_entity_ids(
                modified_from_timestamp=test_datetime,
            )

            producer_entity_ids = (field.get('id') for field in producer_entity_ids)

            enricher_entity_ids = enricher.extract_child_entity_ids(
                relation_entity_ids=producer_entity_ids,
            )

            enricher_entity_ids = (field.get('id') for field in enricher_entity_ids)

            film_work_rows = merger.aggregate_film_work_related_fields(
                entity_ids=enricher_entity_ids,
            )

            grouped_films = self._group_data(film_works=film_work_rows)

            # for i in merger_entity_ids:
            #     print(i)
            # break

        return grouped_films
