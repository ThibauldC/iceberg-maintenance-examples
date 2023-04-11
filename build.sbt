ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "iceberg-maintenance-examples",
    idePackagePrefix := Some("com.tcroonen"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2",
    libraryDependencies += "org.apache.iceberg" % "iceberg-core" % "1.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2",
    libraryDependencies += "org.apache.hive" % "hive-metastore" % "3.0.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4",
    libraryDependencies += "org.apache.parquet" % "parquet-hadoop-bundle" % "1.12.2",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.2.0",
    libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2"

  )
