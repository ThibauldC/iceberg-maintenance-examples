import os
from pathlib import Path

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from maintenance import rewrite_data_files, expire_snapshots, delete_orphan_files, rewrite_manifests

ROOT_PATH = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent.absolute()
CATALOG = "local"
TABLE_NAME = "nyc.taxis"

FULL_TABLE_IDENTIFIER = f"{CATALOG}.{TABLE_NAME}"


def read_taxi_files(spark: SparkSession) -> None:
    taxis = spark.read.parquet(f"{ROOT_PATH}/src/main/resources/data/yellow_tripdata_2022-01.parquet")
    taxis.show()

    taxis.writeTo(FULL_TABLE_IDENTIFIER) \
        .tableProperty("write.target-file-size-bytes", "10485760") \
        .createOrReplace()

    for i in range(2, 7):
        t = spark.read.parquet(f"{ROOT_PATH}/src/main/resources/data/yellow_tripdata_2022-0{i}.parquet")
        t.write\
            .format("iceberg")\
            .mode("append")\
            .save(FULL_TABLE_IDENTIFIER)


if __name__ == "__main__":
    conf = SparkConf()
    configs = [
        ("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0"),
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        ("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"),
        ("spark.sql.catalog.spark_catalog.type", "hive"),
        ("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.local.type", "hadoop"),
        ("spark.sql.catalog.local.warehouse", f"{ROOT_PATH}/python/resources/warehouse"),
        ("spark.sql.defaultCatalog", "local")
    ]
    conf.setAll(configs)

    spark = SparkSession.builder \
        .master("local[*]") \
        .config(conf=conf) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    read_taxi_files(spark)

    rewrite_data_files(spark, CATALOG, TABLE_NAME)
    expire_snapshots(spark, CATALOG, TABLE_NAME)
    delete_orphan_files(spark, CATALOG, TABLE_NAME)
    rewrite_manifests(spark, CATALOG, TABLE_NAME)
