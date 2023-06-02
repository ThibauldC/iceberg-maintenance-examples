# Performing table maintenance on unpartitioned Apache Iceberg tables using Apache Spark (with Scala and Python) 
## Preface
Before everything here are some useful links on Apache Iceberg (apart from [the official docs](https://iceberg.apache.org/docs/latest/):
- [What is Apache Iceberg?](https://www.dremio.com/resources/guides/apache-iceberg/)
- [An architectural look under the covers](https://www.dremio.com/resources/guides/apache-iceberg-an-architectural-look-under-the-covers/#toc_item_The%20Iceberg%20Table%20Format)

In this walkthrough we will use [open taxi data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 
from New York.You can download the data using the following script, which downloads months 1-6 from 2022 at once:

```
#!/bin/bash

# Exit immediately if a  .
set -e

for i in {1..6}; do
  curl -o src/main/resources/data/yellow_tripdata_2022-0$i.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-0$i.parquet
done
```

## Maintenance
### Why perform maintenance on Iceberg tables?
Over time, data will accumulate in your Iceberg table, and the number of data and metadata files will grow significantly. 
Performing maintenance on your Iceberg tables can provide major benefits, such as improved query performance, 
reduced storage costs, and data integrity.

To keep your tables healthy and performant, it is essential to ensure that your metadata and data files do not grow 
too large in number. As data is added or deleted from an Apache Iceberg table, the table's metadata can become fragmented, 
impacting query performance as queries may need to open more files and scan more data than necessary. 
Iceberg provides several built-in actions for dealing with maintenance.

### Rewriting data files, aka file compaction
#### Potential issue
Data may arrive in smaller batches in Iceberg tables due to small writes or the ingestion of streaming data. 
Ingesting smaller files is faster for writing but not as fast for querying. Querying the data would be more efficient 
if there were fewer larger files with more data.

#### Benefit
Analyzing a query involves using each file's metadata to calculate how many splits are required and where to schedule 
each task to maximize data localization. The more files there are, the longer this part of query planning will take. 
Large files can also significantly decrease performance by limiting parallelism.

#### Code
To simulate the arrival of a number of small files into our Iceberg warehouse, we can set a table property called 
`write.target-file-size-bytes` to 10Mb.Adding a few snapshots shows the presence of small files in the Iceberg data files:

![Small files in the Iceberg data files](images/small_files.png)

There are two strategies available for rewriting data files: *binpack* and *sort*.
The *sort* strategy allows for sorting the data using the table sort order or a custom one, potentially using *z-order*.
However, we won't delve into that here.

A common option is to provide the target file size, which is the desired output size that Iceberg will attempt to reach 
when rewriting files. Here's an example using a target file size of 100MB in Scala:

```scala
val table = Spark3Util.loadIcebergTable(spark, "local.nyc.taxis")

SparkActions.get(spark)
  .rewriteDataFiles(table)
  .option("target-file-size-bytes", (1024 * 1024 * 100L).toString)
  .binPack
  .execute
```

Upon examining the summary of the latest snapshot, you'll see that 33 data files have been rewritten using the 
**replace** operation, resulting in 4 data files:

```json
{
  "operation" : "replace",
  "added-data-files" : "4",
  "deleted-data-files" : "33",
  "added-records" : "19817583",
  "deleted-records" : "19817583",
  "added-files-size" : "312579175",
  "removed-files-size" : "320381884",
  "changed-partition-count" : "1",
  "total-records" : "19817583",
  "total-files-size" : "312579175",
  "total-data-files" : "4",
  "total-delete-files" : "0",
  "total-position-deletes" : "0",
  "total-equality-deletes" : "0"
}
```

Looking into the data files directory, you'll see the rewritten data files, with only 3 files of approximately 100MB 
displayed, and one remaining file of 5MB:

![Rewritten data files](images/last_modified.png)

Achieving the same result in Python with a Spark procedure:
```python
spark.sql(
    f"""
    CALL local.system.rewrite_data_files(
        table => 'nyc.taxis', 
        strategy => 'binpack', 
        options => map('target-file-size-bytes', {1024 * 1024 * 100})
    )
    """
)
```

### Expiring snapshots
#### Potential problem
As mentioned earlier, data and snapshots can accumulate over time. Although Iceberg reuses unchanged data files from 
previous snapshots, new snapshots may require changing or removing data files. These files are typically kept for time 
travel or rollbacks, but it is recommended to set a data retention period and regularly expire unnecessary snapshots.

#### Benefit
Expiring snapshots on your Iceberg tables has cost benefits, as data that is no longer needed will automatically be 
deleted from your storage. This can be significant if you have many tables to maintain. It also improves performance by 
reducing the amount of data that needs to be scanned.

#### Code

To expire snapshots in your Iceberg table, you can use the following Scala code:

```scala
SparkActions.get(spark)
  .expireSnapshots(table)
  .expireOlderThan(System.currentTimeMillis())
  .retainLast(1)
  .execute
```

Equivalent Python code:
```python
spark.sql(
    f"""
    CALL local.system.expire_snapshots(
        table => 'nyc.taxis', 
        older_than => TIMESTAMP '{datetime.now().strftime("%Y-%m-%d %H:%m:%S")}', 
        retain_last => 1
    )
    """
)
```

This code expires snapshots older than the current time, retaining only the last snapshot. 
Ideally, you would want to keep more than one snapshot and snapshots older than the current time. 
However, this setup is used here to demonstrate the expiration and the fact that only the rewritten files are retained. 
You can observe that the data files related to the expired snapshots have been deleted:

![Expired snapshots](images/expired_snapshots.png)

While this action explicitly expires snapshots, the metadata files are still retained for historical purposes. 
You can also automatically clean metadata files by setting the following table properties:

- `write.metadata.delete-after-commit.enabled` to `true`
- `write.metadata.previous-versions-max` to the number of metadata files you want to keep (default 100)

```scala
taxis.writeTo("local.nyc.taxis")
  .tableProperty("write.metadata.delete-after-commit.enabled", "true")
  .tableProperty("write.metadata.previous-versions-max", "3")
  .create
```

### Deleting orphan files
#### Potential problem
Sometimes, Spark can create partially written files or files not associated with any snapshots. 
These files do not have a reference in the table metadata and are therefore not picked up by other cleanup actions.

#### Benefit
Regularly running this procedure prevents unused files from accumulating on your storage, 
keeping your folders clean and avoiding additional storage costs.

#### Code
To demonstrate the procedure of deleting orphan files, we will create an orphan file ourselves 
(since it will not be referenced by any of the metadata files):

```
TS=$(date -j -v-7d +"%Y-%m-%dT%H:%M:%S")
touch -d $TS src/main/resources/warehouse/nyc/taxis/data/partial-file
```
**!** This date command is for macOS and may be different on other systems.

You can observe that the file has been added:

![Partial file](images/with_partial.png)


Next, we will remove all orphan files that are older than 7 days (default is 3 days) using the following Scala code:

```scala
SparkActions.get(spark)
  .deleteOrphanFiles(table)
  .olderThan(System.currentTimeMillis() - 1000L*60*60*24*7)
  .execute
```

Equivalent Python code:
```python
ts = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%m:%S")

spark.sql(
    f"""
    CALL local.system.remove_orphan_files(
        table => 'nyc.taxis', 
        older_than => TIMESTAMP '{ts}'
    )
    """
)
```

Only the `partial-file` has been removed, and all other files remain untouched:

![Partial removed](images/partial_removed.png)

Even if you run this with the `olderThan` setting all data files are maintained, and only the created orphan file
has been removed. Note that this action does not create a snapshot.

### Rewriting manifest files
Rewriting manifest files is beneficial for better scan planning and achieving optimal sizes. 
However, I don't have much experience with this, so I will mention it for completeness.

Scala code:

```scala
SparkActions.get(spark)
  .rewriteManifests(table)
  .execute
```

Python code:
```python
spark.sql(
    f"""
    CALL local.system.rewrite_manifests(
        table => 'nyc.taxis'
    )
    """
)
```

The metadata of this snapshot contains the number of manifests rewritten:

```json
{
  "operation" : "replace",
  "manifests-created" : "1",
  "manifests-kept" : "0",
  "manifests-replaced" : "7",
  "entries-processed" : "0",
  "changed-partition-count" : "0",
  "total-records" : "19817583",
  "total-files-size" : "313217532",
  "total-data-files" : "4",
  "total-delete-files" : "0",
  "total-position-deletes" : "0",
  "total-equality-deletes" : "0"
}
```

## Might be helpful

### Scala

If you are running Spark 3.3.0 and up with Java 17, add the following JVM options:
```
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```

### Python
It might be helpful to create a virtual environment when trying the Python code:

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

