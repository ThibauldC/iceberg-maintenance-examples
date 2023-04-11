package com.tcroonen

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

//    val confSettings = Seq(
//      ("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.13:1.2.0"),
//      ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
//      ("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog"),
//      ("spark.sql.catalog.spark_catalog.type", "hive"),
//      ("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog"),
//      ("spark.sql.catalog.local.type", "hadoop"),
//      ("spark.sql.catalog.local.warehouse", s"${File(".").toAbsolute.path}/warehouse")
//
//    )
//
//    val conf = new SparkConf().setAll(confSettings)

    val conf = new SparkConf()
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hive")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set("spark.sql.catalog.local.warehouse", "src/main/resources/warehouse")
      .set("spark.sql.defaultCatalog", "local")

    val spark = SparkSession.builder()
      .config(conf)
      .appName("IcebergMaintenance")
      .master("local[*]")
      .getOrCreate

    //spark.sparkContext.setLogLevel("INFO")

    val taxis = spark.read.parquet("src/main/resources/data/yellow_tripdata_2022-01.parquet")
    taxis.show(20)

    //taxis.write.format("iceberg").saveAsTable("local.nyc.taxis")
  }
}
