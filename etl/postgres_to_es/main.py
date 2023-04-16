import json
import logging
import os

import dotenv

from extractor import MultipleQueryExtractor
from extractor.source_database.postgres import PostgresConnection
from loader import ElasticsearchLoader
from state.persistent_state_manager import JsonFileStorage
from util.common.backoff import backoff
from util.configuration import setup


# @backoff(factor=2)
def run_etl_process():
    pg_conn = PostgresConnection(dsn=dsn_postgres, package_limit=1000)

    extractor = MultipleQueryExtractor(
        db_connection=pg_conn,
        entities_update_schema=entities_update_schema,
        persistant_state_storage=JsonFileStorage.create_storage(),
    )

    loader = ElasticsearchLoader(
        host=elasticsearch_host,
        index_name=elasticsearch_index_schema['index_name'],
        index_settings=elasticsearch_index_schema['index_settings'],
    )

    while True:
        try:
            is_last_data_chunk, collected_movies_data = extractor.extract_data()
        except Exception:
            raise

        loader.load_data(documents=collected_movies_data)

        loader.delete_outdated_data(source_data_provider=pg_conn)

        if is_last_data_chunk:
            break


if __name__ == '__main__':
    setup()
    dotenv.load_dotenv()

    logging.debug('%s', 'start etl process')

    dsn_postgres = {
        'dbname': os.getenv('PG_DB_NAME'),
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PASSWORD'),
        'host': os.getenv('PG_HOST'),
        'port': os.getenv('PG_PORT'),
        'options': '-c search_path=content',
    }

    elasticsearch_host = {
        'scheme': 'http',
        'host': os.getenv('ELASTICSEARCH_HOST'),
        'port': int(os.getenv('ELASTICSEARCH_PORT')),
    }

    with open('loader/elasticsearch/settings/movies_schema.json') as elasticsearch_index_schema:
        elasticsearch_index_schema = {
            'index_name': 'movies',
            'index_settings': json.load(elasticsearch_index_schema),
        }

    entities_update_schema = {
        'updateMovie': {
            'producer': {
                'entity_name': 'film_work',
            },
            'enricher': None,
        },
        'updatePerson': {
            'producer': {
                'entity_name': 'person',
            },
            'enricher': {
                'entity_name': 'film_work',
                'relation_table': 'person_film_work',
                'parent_key': 'film_work_id',
                'child_key': 'person_id',
            },
        },
        'updateGenre': {
            'producer': {
                'entity_name': 'genre',
            },
            'enricher': {
                'entity_name': 'film_work',
                'relation_table': 'genre_film_work',
                'parent_key': 'film_work_id',
                'child_key': 'genre_id',
            },
        },
    }

    run_etl_process()
