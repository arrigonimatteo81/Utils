import configparser
import json

import psycopg2
import logging

from DbUtils.interfaces.IDbConf import IDbConf


class DbConfPostgres(IDbConf):

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.db_config = psycopg2.connect(
            host=self.config['CONFIG']['db_host'],
            user=self.config['CONFIG']['db_user'],
            password=self.config['CONFIG']['db_password'],
            database=self.config['CONFIG']['db_name']
        )

    def getCountQuery(self, table):
        try:
            curs = self.db_config.cursor()
            curs.execute(f"SELECT Sql_String_Count from config_tables where Original_Table='{table}' "
                         "and Inquiry_Type='MENS'")
            return curs.fetchone()[0]
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in getCountQuery: {te}")
            raise te

    def getIngestionQuery(self, table):
        try:
            curs = self.db_config.cursor()
            curs.execute(
                f"SELECT Sql_String_Ingestion from config_tables where Original_Table='{table}' and Inquiry_Type='MENS'")
            return curs.fetchone()[0]
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in getIngestionQuery: {te}")
            raise te

    def getIngestionTable(self, table):
        try:
            curs = self.db_config.cursor()
            curs.execute(
                f"SELECT Output_Table from config_tables where Original_Table='{table}' and Inquiry_Type='MENS'")
            return curs.fetchone()[0]
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in getIngestionTable: {te}")
            raise te

    def getSparkParameters(self, table):
        try:
            curs = self.db_config.cursor()
            curs.execute(
                f"select Spark_Params from config_tables where Original_Table='{table}' and Inquiry_Type='MENS'")
            spark_confs = curs.fetchone()[0]
            spark_params = json.loads(spark_confs)
            return spark_params
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in getSparkParameters: {te}")
            raise te

    def getSourceParameters(self, table):
        try:
            curs = self.db_config.cursor()
            curs.execute(
                f"select Db_Source_Params from config_tables where Original_Table='{table}' and Inquiry_Type='MENS'")
            db_source_confs = curs.fetchone()[0]
            db_source_params = json.loads(db_source_confs)
            return db_source_params
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in getSourceParameters: {te}")
            raise te

    def getAdditionalWhere(self, table):
        try:
            curs = self.db_config.cursor()
            curs.execute(
                f"SELECT Additional_Where from config_tables where Original_Table='{table}' and Inquiry_Type='MENS'")
            add_where = curs.fetchone()[0]
            if add_where is not None:
                return f"and {add_where}"
            else:
                return ""
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in getAdditionalWhere: {te}")
            raise te

    def getNumPartitions(self, table):
        try:
            curs = self.db_config.cursor()
            curs.execute(
                f"SELECT Num_Partitions from config_tables where Original_Table='{table}' and Inquiry_Type='MENS'")
            num_partitions = curs.fetchone()[0]
            if num_partitions is not None:
                return int(num_partitions)
            else:
                return 100
        except TimeoutError as te:
            logging.error(f"QueryExecutionException in getNumPartitions: {te}")
            raise te
