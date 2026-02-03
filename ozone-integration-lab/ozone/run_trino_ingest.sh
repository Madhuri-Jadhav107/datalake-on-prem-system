#!/bin/bash

# Run Spark submission for Trino-compatible ingestion (Hive Catalog + S3A)
# Dependencies:
# 1. Iceberg Spark Runtime
# 2. Hadoop AWS (for S3A file system)

echo "Submitting Trino-compatible ingestion job..."
docker exec madhuri-ozone-spark-iceberg-1 spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /home/iceberg/local/ingest_trino.py /home/iceberg/local/sample.csv trino_table > ingest_trino_log.txt 2>&1

echo "Job submitted. Check ingest_trino_log.txt for results."
echo "To check logs run: cat ingest_trino_log.txt"
