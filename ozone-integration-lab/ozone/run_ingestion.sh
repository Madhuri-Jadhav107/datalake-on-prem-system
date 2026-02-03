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

# Strip host-side prefix if present
# We assume the user might provide paths starting with 'ozone-integration-lab/ozone/'
# or just relative to it.
MAPPED_PATH=$INPUT_FILE
# Strip leading ./ if present
MAPPED_PATH=${MAPPED_PATH#./}
# Strip host-side lab folder prefix if present
MAPPED_PATH=${MAPPED_PATH#ozone-integration-lab/ozone/}

# The container maps its /home/iceberg/local to our ozone-integration-lab/ozone folder
CONTAINER_PATH="file:///home/iceberg/local/$MAPPED_PATH"

echo "Submitting Spark job for file: $INPUT_FILE -> Table: $TABLE_NAME"

# Using spark-submit to ensure --packages are correctly loaded into the classpath
docker exec madhuri-ozone-spark-iceberg-1 spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.ozone.replication=1 \
  --conf spark.hadoop.ozone.replication.type=RATIS \
  /home/iceberg/local/ingest_trino.py "$CONTAINER_PATH" "$TABLE_NAME" > ingest_trino_log.txt 2>&1 &

echo "Ingestion job submitted in background. Logs redirected to 'ingest_trino_log.txt'."
echo "To check logs run: tail -f ingest_trino_log.txt"
