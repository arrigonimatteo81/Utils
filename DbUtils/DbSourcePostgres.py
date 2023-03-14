import logging

import psycopg2

from DbUtils.interfaces.IDbSource import IDbSource


class DbSourcePostgres(IDbSource):
    def returnCount(self, query_count) -> int:
        pass

    def __init__(self, config):
        self.db_source = psycopg2.connect(
            host=config['CONFIG']['db_host'],
            user=config['CONFIG']['db_user'],
            password=config['CONFIG']['db_password'],
            database=config['CONFIG']['db_name']
        )

    def returnQueryContent(self, query):
        query_cursor = self.db_source.cursor()
        try:
            query_cursor.execute(query)
            return query_cursor.fetchall()
        except TimeoutError as te:
            logging.error(f"DbConfPostgres - QueryExecutionException in returnQueryContent: {te}")
            raise te
        finally:
            query_cursor.close()

    def closeConnection(self):
        if self.db_source.closed == 0:
            self.db_source.close()