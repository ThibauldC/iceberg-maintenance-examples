package com.tcroonen

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.iceberg.spark.Spark3Util


object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hive")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set("spark.sql.catalog.local.warehouse", s"${System.getProperty("user.dir")}/src/main/resources/warehouse")
      .set("spark.sql.defaultCatalog", "local")

    val spark = SparkSession.builder()
      .config(conf)
      .appName("IcebergMaintenance")
      .master("local[*]")
      .getOrCreate

    val taxis = spark.read.parquet("src/main/resources/data/yellow_tripdata_2022-01.parquet")
    taxis.show(20)

    taxis.writeTo("local.nyc.taxis")
      .tableProperty("write.target-file-size-bytes", "10485760")
      .createOrReplace

    (2 to 6)
      .foreach { i =>
        val t = spark.read.parquet(s"src/main/resources/data/yellow_tripdata_2022-0$i.parquet")
        t.write.format("iceberg").mode("append").save("local.nyc.taxis")
      }

    val table = Spark3Util.loadIcebergTable(spark, "local.nyc.taxis")

    Maintenance.rewriteDataFiles(spark, table)
    Maintenance.expireSnapshots(spark, table)
    Maintenance.deleteOrphanFiles(spark, table)
    Maintenance.rewriteManifests(spark, table)
  }
}
