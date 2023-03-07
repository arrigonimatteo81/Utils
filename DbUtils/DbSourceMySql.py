import logging
from typing import List

import mysql.connector
from mysql.connector.types import RowType

from DbUtils.interfaces.IDbSource import IDbSource


class DbSourceMySql(IDbSource):

    def __init__(self, config):
        self.config = config
        self.db_source = mysql.connector.connect(
            host=self.config['db_host'],
            user=self.config['db_user'],
            password=self.config['db_password'],
            database=self.config['db_name']
        )

    def returnCount(self, query_count):
        try:
            count_cursor = self.db_source.cursor()
            count_cursor.execute(query_count)
            return int(count_cursor.fetchone()[0])
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in returnCount: {te}")
            raise te

    def returnQueryContent(self, query) -> List[RowType]:
        try:
            count_cursor = self.db_source.cursor()
            count_cursor.execute(query)
            return count_cursor.fetchall()
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in returnQueryContent: {te}")
            raise te