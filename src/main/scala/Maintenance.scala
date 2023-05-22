package com.tcroonen

import org.apache.iceberg.Table
import org.apache.iceberg.spark.actions.SparkActions

object Maintenance {

  def deleteOrphanFiles(table: Table): Unit = {
    SparkActions.get
      .deleteOrphanFiles(table)
      .location(s"${table.location()}/data")
      .olderThan(System.currentTimeMillis()) // this somehow deletes all data and metadata?
      .execute
  }
}
