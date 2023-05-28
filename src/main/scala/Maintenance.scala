package com.tcroonen

import org.apache.iceberg.Table
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession

object Maintenance {

  def expireSnapshots(spark: SparkSession,table: Table): Unit = {
    SparkActions.get(spark)
      .expireSnapshots(table)
      .expireOlderThan(System.currentTimeMillis())
      .retainLast(1)
      .execute
  }

  def rewriteDataFiles(spark: SparkSession, table: Table): Unit = {
    SparkActions.get(spark)
      .rewriteDataFiles(table)
      .option("target-file-size-bytes", (1024 * 1024 * 100L).toString)
      .binPack
      .execute
  }

  def deleteOrphanFiles(spark: SparkSession, table: Table): Unit = {
    SparkActions.get(spark)
      .deleteOrphanFiles(table)
      .olderThan(System.currentTimeMillis() - 1000L*60*60*24*7)
      .execute
  }

  def rewriteManifests(spark: SparkSession, table: Table): Unit = {
    SparkActions.get(spark)
      .rewriteManifests(table)
      .execute
  }
}
