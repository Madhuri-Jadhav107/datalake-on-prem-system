#!/bin/bash

# Run Spark submission with all necessary dependencies for Ozone/Iceberg
# Fixes included:
# 1. Using ozone-filesystem-hadoop3 for Hadoop 3.x compatibility
# 2. Including protobuf-java for missing ServiceException class
# 3. Using local master
docker exec madhuri-ozone-spark-iceberg-1 spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.ozone:ozone-filesystem-hadoop3:1.4.0,com.google.protobuf:protobuf-java:2.5.0 \
  /home/iceberg/local/ingest_ozone.py /home/iceberg/local/sample.csv custom_table > ingest_log.txt 2>&1

echo "Ingestion job submitted. Logs redirected to 'ingest_log.txt'."
echo "To check logs run: cat ingest_log.txt"
