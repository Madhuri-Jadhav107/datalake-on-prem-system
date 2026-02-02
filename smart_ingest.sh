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
docker cp "$filepath" ozone-spark-iceberg-1:/home/iceberg/local/"$filename"
if [ $? -ne 0 ]; then
    echo "Error copying file. Please check the path and try again."
    exit 1
fi

echo "[2/3] Uploading raw file to Ozone (Optional Backup)..."
# Using a generic approach, errors suppressed to match .bat behavior
docker exec ozone-om-1 ozone sh key put /vol1/bucket1/"$filename" /tmp/"$filename" >/dev/null 2>&1

echo "[3/3] Running Iceberg Ingestion Job..."
echo "Target Table: local.db.$name"

docker exec ozone-spark-iceberg-1 bash -c "spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 --jars /home/iceberg/local/ozone-filesystem.jar --conf spark.driver.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.executor.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.sql.defaultCatalog=local --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=file:///home/iceberg/warehouse/local /home/iceberg/local/ingest_to_iceberg.py /home/iceberg/local/$filename $name"

echo ""
echo "==================================================="
echo "                JOB COMPLETE"
echo "==================================================="
