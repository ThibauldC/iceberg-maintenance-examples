#!/bin/bash

# Exit immediately if a  .
set -e

for i in {4..6}; do
  curl -o src/main/resources/data/yellow_tripdata_2022-0$i.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-0$i.parquet
done
