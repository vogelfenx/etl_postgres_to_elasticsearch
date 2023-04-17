import json
import os
from contextlib import closing
from time import sleep

from extractor import MultipleQueryExtractor
from extractor.source_database.postgres import PostgresConnection
from loader import ElasticsearchLoader
from state.persistent_state_manager import JsonFileStorage
from util.common.backoff import backoff
from util.configuration import LOGGER, read_app_config


@backoff(factor=2)
def run_etl_process():
    """
        Run an ETL process for moving data from a Postgres database to an Elasticsearch index.

        Uses a Multiple Query Data handling strategy to extract data from Postgres.
    """

    with closing(PostgresConnection(dsn=dsn_postgres)) as pg_conn:

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
            configurations = read_app_config()
            pg_conn.package_limit = configurations['PAGE_DATA_SIZE_LIMIT']
            process_sleep_time = configurations["PROCESS_SLEEP_TIME"]

            processed_data_count = 0
            try:
                collected_movies_data = extractor.extract_data()
            except Exception:
                raise

            if collected_movies_data:
                processed_data_count += len(collected_movies_data)
                LOGGER.info('Number of found data to be load: %s', processed_data_count)

                loader.load_data(documents=collected_movies_data)

                loader.delete_outdated_data(source_data_provider=pg_conn)

            LOGGER.info(
                f'ETL process finished.\n \
                  Number of data loaded: {processed_data_count}\n \
                  Next processing in {process_sleep_time} seconds.',
            )

            sleep(process_sleep_time)


if __name__ == '__main__':
    LOGGER.debug('%s', 'start etl process')

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
