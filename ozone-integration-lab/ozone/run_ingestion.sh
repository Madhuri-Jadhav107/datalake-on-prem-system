#!/bin/bash

# Run Spark submission with all necessary dependencies for Ozone/Iceberg
# Fixes included:
# 1. Using ozone-filesystem-hadoop3-client (Shaded) to avoid Protobuf conflicts
# 2. Setting replication to 1 (since we have 1 DataNode)
# 3. Removing userClassPathFirst to avoid Iceberg ClassCastException

# Check for arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <local_csv_file_path> <table_name>"
    echo "Example: $0 ./data/users.csv users_table"
    exit 1
fi

INPUT_FILE=$1
TABLE_NAME=$2

# Create a container-accessible path for the input file
# We assume the user provides a path relative to the current directory or absolute.
# Since we mount $(pwd) to /home/iceberg/local, we need to handle the path mapping.
# For simplicity in this lab, we require the file to be in the current directory or a subdirectory.
# If it's a simple filename, we prepend /home/iceberg/local/

FILENAME=$(basename "$INPUT_FILE")
CONTAINER_PATH="/home/iceberg/local/$FILENAME"

echo "Submitting Spark job for file: $FILENAME -> Table: $TABLE_NAME"

docker exec madhuri-ozone-spark-iceberg-1 spark-submit \
  --master local[*] \
  --conf spark.hadoop.ozone.replication=1 \
  --conf spark.hadoop.ozone.replication.type=RATIS \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /home/iceberg/local/ingest_trino.py "$CONTAINER_PATH" "$TABLE_NAME" > ingest_trino_log.txt 2>&1

echo "Ingestion job submitted. Logs redirected to 'ingest_trino_log.txt'."
echo "To check logs run: cat ingest_trino_log.txt"
