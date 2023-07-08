from pyspark.sql import SparkSession
import json

SPARK_JARS = "/Users/kavivasquez/spark_drivers/postgresql-42.6.0.jar"


class Session:

    def __init__(self, config_file, inferred=True, header=True):

        self.spark = self._create_session()
        self.schema = inferred
        self.header = header
        self.config_file = config_file
        self.connections = self.parse_file()
        self.db_name = self.connections['db_name']
        self.password = self.connections['password']
        self.name = self.connections['username']
        self.table_name = self.connections["tbl_name"]
        # self._create_session()

    @staticmethod
    def _create_session():
        spark = (
            SparkSession \
                .builder \
                .appName("test") \
                .config("spark.jars", SPARK_JARS) \
                .getOrCreate()
        )
        return spark

    def parse_file(self):
        with open(self.config_file) as test:
            data = json.load(test)
            return data

    def pull_data(self):
        df = self.spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://localhost:5432/{self.db_name}") \
            .option("dbtable", f'{self.table_name}') \
            .option("user", f"{self.name}") \
            .option("password", f"{self.password}") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
