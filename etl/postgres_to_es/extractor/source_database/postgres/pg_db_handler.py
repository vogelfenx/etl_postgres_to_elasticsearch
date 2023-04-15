import logging

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime


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

    def select_last_modified_entity_ids(self, *, entity: str, modified_timestamp: datetime):
        """Return last modified entity IDs."""

        cursor = self.cursor

        # TODO implement validation for entity & modified_timestamp
        try:
            cursor.execute(f"SELECT id FROM {entity} where modified > '{modified_timestamp}'")
        except psycopg2.Error as error:
            logging.error('%s: %s', error.__class__.__name__, error)
            raise error

        while True:
            rows = cursor.fetchmany(size=self.package_limit)
            if not rows:
                return
            yield from rows

    def select_related_entity_ids(
        self,
        *,
        entity_name,
        relation_table,
        parent_key,
        child_key,
        parent_entity_ids,
    ):
        # SELECT fw.id, fw.modified
        # FROM content.film_work fw
        # LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        # WHERE pfw.person_id IN (<id_всех_людей>)
        # ORDER BY fw.modified
        # LIMIT 100;

        parent_entity_ids = ','.join(f"'{field}'" for field in parent_entity_ids)

        cursor = self.cursor
        sql_query = f"""
        SELECT sel_table.id
            FROM {entity_name} sel_table
            LEFT JOIN {relation_table} rel_table ON rel_table.{parent_key} = sel_table.id
            WHERE rel_table.{child_key} IN ({parent_entity_ids})
            ORDER BY sel_table.modified;
        """
        try:
            cursor.execute(sql_query)
        except psycopg2.Error as error:
            logging.error('%s: %s', error.__class__.__name__, error)
            raise error

        while True:
            rows = cursor.fetchmany(size=self.package_limit)
            if not rows:
                return
            yield from rows

    def select_film_work_related_fields(
        self,
        film_work_ids,
    ):

        film_work_ids = ','.join(f"'{field}'" for field in film_work_ids)

        cursor = self.cursor
        sql_query = f"""
            SELECT
                fw.id as fw_id, 
                fw.title, 
                fw.description, 
                fw.rating, 
                pfw.role, 
                p.id as person_id, 
                p.full_name,
                g.name as genre
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({film_work_ids});
        """
        try:
            cursor.execute(sql_query)
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
