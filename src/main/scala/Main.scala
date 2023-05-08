package com.tcroonen

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions


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

//    taxis.write.format("iceberg").saveAsTable("local.nyc.taxis")
//
    (1 to 3)
      .foreach { i =>
        val t = spark.read.parquet(s"src/main/resources/data/yellow_tripdata_2022-0$i.parquet")
        t.write.format("iceberg").mode("append").save("local.nyc.taxis")
      }


    //val maint = Maintenance
    //maint.deleteOrphanFiles(table)


//    val table = Spark3Util.loadIcebergTable(spark, "local.nyc.taxis")
//
//    SparkActions.get(spark)
//      .expireSnapshots(table)
//      .retainLast(3)
//      .expireFiles()

//    SparkActions.get(spark)
//      .deleteOrphanFiles(table)
//      .olderThan(System.currentTimeMillis() - 1000L*60*60*24*7)
//      .execute
  }
}
