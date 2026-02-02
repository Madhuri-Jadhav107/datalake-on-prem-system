#!/bin/bash

echo "==================================================="
echo "     Smart Ingest: CSV to Iceberg in Ozone"
echo "==================================================="
echo ""
echo "Please provide the full path to your CSV file."
echo "Example: /home/ubuntu/data/sales_data.csv"
echo ""

read -p "Enter File Path: " filepath

if [ -z "$filepath" ]; then
    echo "No path provided. Exiting."
    exit 1
fi

filename=$(basename "$filepath")
name="${filename%.*}"

echo ""
echo "[1/3] Copying $filename to Docker container..."
docker cp "$filepath" madhuri-ozone-spark-iceberg-1:/home/iceberg/local/"$filename"
if [ $? -ne 0 ]; then
    echo "Error copying file. Please check the path and try again."
    exit 1
fi

echo "[2/3] Uploading raw file to Ozone (Optional Backup)..."
# Using a generic approach, errors suppressed to match .bat behavior
docker exec madhuri-ozone-om-1 ozone sh key put /vol1/bucket1/"$filename" /tmp/"$filename" >/dev/null 2>&1

echo "[3/3] Running Iceberg Ingestion Job..."
echo "Target Table: hive_prod.iceberg_db.$name"

# Copy Python script to container
docker cp ./ozone-integration-lab/ozone/ingest_to_iceberg.py madhuri-ozone-spark-iceberg-1:/home/iceberg/local/ingest_to_iceberg.py

# Ensure S3 bucket exists for warehouse (using correct replication for single-node)
docker exec madhuri-ozone-om-1 ozone sh volume create /s3v >/dev/null 2>&1 || true
docker exec madhuri-ozone-om-1 ozone sh bucket create /s3v/warehouse-v2 --replication=1 --type=RATIS >/dev/null 2>&1 || true

# Fetch S3 credentials dynamically
echo "Fetching S3 credentials..."
CREDS_OUTPUT=$(docker exec madhuri-ozone-om-1 ozone s3 getsecret -u hadoop 2>/dev/null)
if [ $? -eq 0 ] && [ ! -z "$CREDS_OUTPUT" ]; then
    ACCESS_KEY=$(echo "$CREDS_OUTPUT" | grep -o "awsAccessKey=[^ ]*" | cut -d= -f2 | tr -d '\r')
    SECRET_KEY=$(echo "$CREDS_OUTPUT" | grep -o "awsSecret=[^ ]*" | cut -d= -f2 | tr -d '\r')
    echo "Successfully retrieved S3 credentials for user 'hadoop'."
else
    echo "Warning: Could not retrieve S3 credentials. Using defaults."
    ACCESS_KEY="anyID"
    SECRET_KEY="anySecret"
fi

# Submit Spark job
# Note: Using s3a for warehouse to ensure compatibility with Trino
docker exec madhuri-ozone-spark-iceberg-1 /opt/spark/bin/spark-submit \
  --master local[*] \
  --deploy-mode client \
  --name "IcebergIngest" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive_prod.type=hive \
  --conf spark.sql.catalog.hive_prod.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.hive_prod.warehouse=s3a://warehouse-v2/ \
  --conf spark.sql.catalog.hive_prod.s3.endpoint=http://s3g:9878 \
  --conf spark.sql.defaultCatalog=hive_prod \
  --conf spark.hadoop.fs.s3a.access.key="$ACCESS_KEY" \
  --conf spark.hadoop.fs.s3a.secret.key="$SECRET_KEY" \
  --conf spark.hadoop.fs.s3a.endpoint=http://s3g:9878 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /home/iceberg/local/ingest_to_iceberg.py \
  /home/iceberg/local/"$filename" "$name"

echo ""
echo "==================================================="
echo "                JOB COMPLETE"
echo "==================================================="
