import logging
import os

import dotenv
from extractor import MultipleQueryExtractor
from extractor.source_database.postgres import PostgresConnection
from util.configuration import setup

if __name__ == '__main__':
    setup()
    dotenv.load_dotenv()

    # test logging
    logging.debug('%s', 'start etl process')

    # test connection
    dsn_postgres = {
        'dbname': os.getenv('PG_DB_NAME'),
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PASSWORD'),
        'host': os.getenv('PG_HOST'),
        'port': os.getenv('PG_PORT'),
        'options': '-c search_path=content',
    }

    pg_conn = PostgresConnection(dsn=dsn_postgres, package_limit=1000)

    # Process when a person record has been updated

    extractor = MultipleQueryExtractor(
        db_connection=pg_conn,
    )

    collected_movies_data = extractor.extract_data()

    pass
