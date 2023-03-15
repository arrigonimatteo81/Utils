import logging

import psycopg2

from DbUtils.interfaces.IDbSource import IDbSource


class DbSourcePostgres(IDbSource):
    def returnCount(self, query_count) -> int:
        pass

    def __init__(self, config):
        self.config = config

    def returnQueryContent(self, query):
        with psycopg2.connect(
                host=self.config['CONFIG']['db_host'],
                user=self.config['CONFIG']['db_user'],
                password=self.config['CONFIG']['db_password'],
                database=self.config['CONFIG']['db_name']
        ) as db_source:
            query_cursor = db_source.cursor()
            try:
                query_cursor.execute(query)
                return query_cursor.fetchall()
            except TimeoutError as te:
                logging.error(f"DbConfPostgres - QueryExecutionException in returnQueryContent: {te}")
                raise te
            finally:
                query_cursor.close()

    #def closeConnection(self):
    #    if self.db_source.closed == 0:
    #        self.db_source.close()
