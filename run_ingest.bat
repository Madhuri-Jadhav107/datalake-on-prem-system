@echo off
echo Starting Data Ingestion to Iceberg...
docker exec spark-iceberg bash -c "spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 --jars /home/iceberg/local/ozone-filesystem.jar --conf spark.driver.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.executor.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.sql.defaultCatalog=local --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=file:///home/iceberg/warehouse/local /home/iceberg/local/ingest_to_iceberg.py"
echo Ingestion Complete.
pause
