from DbUtils.interfaces.IDbConf import IDbConf


class ConfFile(IDbConf):

    def closeConnection(self):
        pass

    def __init__(self, config):
        self.config = config

    def getCountQuery(self, table) -> str:
        return self.config[table]["Sql_String_Count"]

    def getIngestionQuery(self, table) -> str:
        return self.config[table]["Sql_String_Ingestion"]

    def getIngestionTable(self, table) -> str:
        return self.config[table]["Output_Table"]

    def getSparkParameters(self, table) -> dict:
        return self.config[table]["Spark_Params"] ##ATTENZIONE: JSON!

    def getAdditionalWhere(self, table) -> str:
        return self.config[table]["Additional_Where"]

    def getNumPartitions(self, table) -> int:
        return self.config[table]["Num_Partitions"]

    def getSourceParameters(self, table) -> dict:
        return self.config[table]["Db_Source_Params"] ##ATTENZIONE: JSON!