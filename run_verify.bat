@echo off
echo Verifying Data in Iceberg Table...
docker exec spark-iceberg spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 --conf spark.sql.defaultCatalog=local --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=file:///home/iceberg/warehouse/local -e "SELECT count(*) as total_records, status FROM local.db.large_transactions GROUP BY status;"
pause
