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
echo "Target Table: local.db.$name"

# Ensure S3 bucket exists for warehouse
docker exec madhuri-ozone-om-1 ozone sh volume create /s3v >/dev/null 2>&1 || true
docker exec madhuri-ozone-om-1 ozone sh bucket create /s3v/warehouse >/dev/null 2>&1 || true

docker exec madhuri-ozone-spark-iceberg-1 bash -c "spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4 --jars /home/iceberg/local/ozone-filesystem.jar --conf spark.driver.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.executor.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.hadoop.fs.s3a.endpoint=http://s3g:9878 --conf spark.hadoop.fs.s3a.access.key=anyID --conf spark.hadoop.fs.s3a.secret.key=anySecret --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.sql.defaultCatalog=local --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hive --conf spark.sql.catalog.local.uri=thrift://hive-metastore:9083 --conf spark.sql.catalog.local.warehouse=s3a://warehouse/ /home/iceberg/local/ingest_to_iceberg.py /home/iceberg/local/$filename $name"

echo ""
echo "==================================================="
echo "                JOB COMPLETE"
echo "==================================================="
