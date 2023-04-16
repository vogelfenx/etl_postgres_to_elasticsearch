import logging
from datetime import datetime
from typing import Generator, List, Any

import psycopg2
from psycopg2.extras import DictCursor


class PostgresConnection:
    """PostgreSQL database handler."""

    def __init__(self, dsn: dict, package_limit: int = 1000):
        """Postgres database handler.

        Args:
            dsn (dict): data source name for postgres connection
            package_limit (int, optional): limit of the rows to fetch at once. Defaults to 1000.
        """
        logging.debug('initialize PostgresConnection')

        self.connection = psycopg2.connect(**dsn, cursor_factory=DictCursor)

        self.cursor = self.connection.cursor()
        self.package_limit = package_limit
        self.offset = 0

    def close(self):
        """Close postgres connection."""
        self.connection.close()

    def select_all_entity_ids(self, entity: str) -> Generator:
        """Select all entity IDs.

        Args:
            entity (str): entity name

        Yields:
            str: entity IDs
        """
        cursor = self.cursor

        try:
            cursor.execute(f"SELECT id FROM {entity}")
        except psycopg2.Error as error:
            logging.error('%s: %s', error.__class__.__name__, error)
            raise error

        while True:
            rows = cursor.fetchmany(size=self.package_limit)
            if not rows:
                return
            yield from rows

    def select_last_modified_entity_ids(self, *, entity: str, modified_timestamp: datetime) -> Generator:
        """Select last modified entity IDs.

        Args:
            entity (str): entity name
            modified_timestamp (datetime): modified timestamp

        Yields:
            dict: A dictionary with `id` and `modified` keys.
                  The last record has an additional `is_last_data_chunk` key with a boolean value.
        """

        cursor = self.cursor

        sql_query = f"""
        SELECT id, modified
        FROM {entity}
        WHERE modified > '{modified_timestamp}'
        ORDER BY modified
        LIMIT {self.package_limit}
        """
        try:
            cursor.execute(sql_query)
        except psycopg2.Error as error:
            logging.error('%s: %s', error.__class__.__name__, error)
            raise error

        rows = cursor.fetchall()

        if self.package_limit > len(rows):
            rows.insert(0, {'is_last_data_chunk': True})
            yield from rows
            return
        rows.insert(0, {'is_last_data_chunk': False})

        yield from rows

    def select_related_entity_ids(
        self,
        *,
        entity_name: str,
        relation_table: str,
        parent_key: str,
        child_key: str,
        parent_entity_ids: List[str],
    ):
        """Select related entity IDs.

        Args:
            entity_name (str): entity name
            relation_table (str): relation table name
            parent_key (str): parent key name
            child_key (str): child key name
            parent_entity_ids (List[str]): parent entity IDs

        Yields:
            dict: A dictionary with `id` and `modified` keys.
        """
        parent_entity_ids = ','.join(f"'{field}'" for field in parent_entity_ids)

        cursor = self.cursor
        sql_query = f"""
        SELECT sel_table.id, sel_table.modified
            FROM {entity_name} sel_table
            LEFT JOIN {relation_table} rel_table ON rel_table.{parent_key} = sel_table.id
            WHERE rel_table.{child_key} IN ({parent_entity_ids})
            ORDER BY sel_table.modified
            LIMIT {self.package_limit};
        """
        try:
            cursor.execute(sql_query)
        except psycopg2.Error as error:
            logging.error('%s: %s', error.__class__.__name__, error)
            raise error

        rows = cursor.fetchall()

        yield from rows
        if self.package_limit > len(rows):
            return

    def select_film_work_related_fields(
        self,
        film_work_ids: List[str],
    ):
        """Return film work related fields for given film work ids.

        Args:
            film_work_ids (list[str]): a list of film work ids to fetch data for

        Yields:
            Dict[str, Any]: a dictionary containing film work related fields for a single film work id
        """
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
            yield from rows
            if not rows:
                return

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
