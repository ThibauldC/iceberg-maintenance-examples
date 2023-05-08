# Performing table maintenance on Apache Iceberg tables using Apache Spark (with Scala and Python) 
## Preface
For this walkthrough we will use [open taxi data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 
from New York.

It can be downloaded using this script (for downloading months 1-6 from 2022 at once):

```
#!/bin/bash

# Exit immediately if a  .
set -e

for i in {1..6}; do
  curl -o src/main/resources/data/yellow_tripdata_2022-0$i.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-0$i.parquet
done
```

## Maintenance
### Why maintenance?

### Expiring snapshots
#### Cause

#### Benefit
#### Code

### Rewriting data files
### Deleting orphan files
#### Cause
Sometimes Spark can create partially written files, or files not associated with any snapshots. These files do not have a
reference in the table metadata and are therefore not being picked up through other cleanup actions.

#### Benefit
Running this procedure regularly prevents unused files from accumulating on your storage, thus keeping your folders 
clean and preventing additional storage costs

#### Code
For demonstrating this procedure we will create an orphan file ourselves (since it will not be referenced by any of the 
metadata files):

```
TS=$(date -j -v-7d +"%Y-%m-%dT%H:%M:%S")
touch -d $TS src/main/resources/warehouse/nyc/taxis/data/partial-file
```
**!** This date command is for OS X, but will be different on other systems.

We will then remove all orphan files which are older than 7 days (default = 3 days).
```scala
SparkActions.get(spark)
  .deleteOrphanFiles(table)
  .olderThan(System.currentTimeMillis() - 1000L*60*60*24*7)
  .execute
```

Only the `partial-file` has been removed and all other files are left untouched.
Even if you run this with the `olderThan` setting all data files are maintained and only the created orphan file
has been removed.

spark 3.3.0 and up with Java 17 (add to VM options in Run/Debug configurations)
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