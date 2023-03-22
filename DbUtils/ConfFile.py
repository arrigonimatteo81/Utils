from DbUtils.interfaces.IDbConf import IDbConf


class ConfFile(IDbConf):

    def __init__(self, config):
        self.config = config

    def getCountQuery(self, table) -> str:
        try:
            return self.config["tables"][table]["Sql_String_Count"]
        except KeyError:
            return ""

    def getIngestionQuery(self, table) -> str:
        try:
            return self.config["tables"][table]["Sql_String_Ingestion"]
        except KeyError:
            return ""

    def getIngestionTable(self, table) -> str:
        try:
            return self.config["tables"][table]["Output_Table"]
        except KeyError:
            return ""

    def getSparkParameters(self, table) -> dict:
        try:
            return self.config["tables"][table]["Spark_Params"]
        except KeyError:
            return {}

    def getAdditionalWhere(self, table) -> str:
        try:
            return self.config["tables"][table]["Additional_Where"]
        except KeyError:
            return ""

    def getNumPartitions(self, table) -> int:
        try:
            return self.config["tables"][table]["Num_Partitions"]
        except KeyError:
            return 100

    def getSourceParameters(self, table) -> dict:
        try:
            return self.config["tables"][table]["Db_Source_Params"]
        except KeyError:
            return {}
