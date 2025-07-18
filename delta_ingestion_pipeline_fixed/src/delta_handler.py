import os
os.environ["PYSPARK_PYTHON"] = os.path.abspath(os.path.join("..", "venv", "Scripts", "python.exe"))

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

class DeltaTableHandler:
    def __init__(self, path: str):
        self.path = path
        builder = SparkSession.builder \
            .appName("DeltaLakeIngestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.io.nativeio.enabled", "false")
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def append_data(self, df):
        spark_df = self.spark.createDataFrame(df)
        spark_df.write.format("delta").mode("append").save(self.path)

    def get_latest_version(self):
        delta_table = DeltaTable.forPath(self.spark, self.path)
        latest_df = self.spark.read.format("delta").load(self.path)
        return latest_df