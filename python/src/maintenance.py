from datetime import datetime, timedelta

from pyspark.sql import SparkSession


def rewrite_data_files(spark: SparkSession, catalog: str, table: str) -> None:
    spark.sql(
        f"""
        CALL {catalog}.system.rewrite_data_files(
            table => '{table}', 
            strategy => 'binpack', 
            options => map('target-file-size-bytes', {1024 * 1024 * 100})
        )
        """
    )


def expire_snapshots(spark: SparkSession, catalog: str, table: str) -> None:
    spark.sql(
        f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{table}', 
            older_than => TIMESTAMP '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 
            retain_last => 1
        )
        """
    )


def delete_orphan_files(spark: SparkSession, catalog: str, table: str) -> None:
    ts = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(
        f"""
        CALL {catalog}.system.remove_orphan_files(
            table => '{table}', 
            older_than => TIMESTAMP '{ts}'
        )
        """
    )


def rewrite_manifests(spark: SparkSession, catalog: str, table: str) -> None:
    spark.sql(
        f"""
        CALL {catalog}.system.rewrite_manifests(
            table => '{table}'
        )
        """
    )
