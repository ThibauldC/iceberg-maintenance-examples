ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "iceberg-maintenance-examples",
    idePackagePrefix := Some("com.tcroonen"),
    libraryDependencies ++= Seq(
        "org.apache.iceberg" % "iceberg-core" % "1.2.0",
        "org.apache.spark" %% "spark-sql" % "3.3.2",
        "org.apache.hive" % "hive-metastore" % "3.0.0",
        "org.apache.hadoop" % "hadoop-common" % "3.3.4",
        "org.apache.parquet" % "parquet-hadoop-bundle" % "1.12.2",
        "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.2.0",
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2"
    ),
    excludeDependencies ++= Seq (
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      ExclusionRule("log4j", "log4j"),
      ExclusionRule("org.slf4j", "slf4j-reload4j"),
      ExclusionRule("ch.qos.logback", "logback-classic"),
    )
  )
