#!/bin/bash

# Run Spark submission with all necessary dependencies for Ozone/Iceberg
# Fixes included:
# 1. Using ozone-filesystem-hadoop3-client (Shaded) to avoid Protobuf conflicts
# 2. Setting replication to 1 (since we have 1 DataNode)
# 3. Removing userClassPathFirst to avoid Iceberg ClassCastException

docker exec madhuri-ozone-spark-iceberg-1 spark-submit \
  --master local[*] \
  --conf spark.hadoop.ozone.replication=1 \
  --conf spark.hadoop.ozone.replication.type=RATIS \
  --jars /home/iceberg/local/ozone-filesystem.jar \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  /home/iceberg/local/ingest_ozone.py /home/iceberg/local/sample.csv custom_table > ingest_log.txt 2>&1

echo "Ingestion job submitted. Logs redirected to 'ingest_log.txt'."
echo "To check logs run: cat ingest_log.txt"
