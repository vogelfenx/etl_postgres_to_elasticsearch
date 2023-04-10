import logging

import psycopg2
from psycopg2.extras import DictCursor


class PostgresConnection:
    """PostgreSQL database handler."""

    def __init__(self, dsn: dict, package_limit: int = None):
        """Postgres database handler.

        Args:
            dsn (dict): data source name for postgres connection
        """
        logging.debug('initialize PostgresConnection')

        self.connection = psycopg2.connect(**dsn, cursor_factory=DictCursor)

        self.cursor = self.connection.cursor()
        self.package_limit = 10
        self.offset = 0

    def close(self):
        """Close postgres connection."""
        self.connection.close()

    def select_entity_fields(self, *, entity: str, fields: list = None, condition: str):
        """Return specified fields."""
        logging.debug('select_entity_fields(%s, %s)', entity, fields)

        cursor = self.cursor

        fields = ','.join(fields)

        try:
            cursor.execute(f'SELECT {fields} FROM {entity} where {condition}')
        except psycopg2.Error as error:
            logging.error('%s: %s', error.__class__.__name__, error)
            raise error

        while True:
            rows = cursor.fetchmany(size=self.package_limit)
            if not rows:
                return
            yield from rows

    def _check_table_consistency(self, *, table_name: str):
        """Check if the given table exists.

        Args:
            table_name (str): name of the table to test

        Raises:
            OperationalError: Raise exception if table doesn't exist
        """
        sql_query = """
        SELECT EXISTS (
            SELECT FROM
            pg_tables
        WHERE
            tablename  = %s
        );
        """

        self.cursor.execute(sql_query, (table_name, ))
        is_table_exists = self.cursor.fetchone()[0]

        if not is_table_exists:
            raise psycopg2.OperationalError(f"table doesn't exist: {table_name}")
