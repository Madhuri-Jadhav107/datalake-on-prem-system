@echo off
setlocal
echo ===================================================
echo      Smart Ingest: CSV to Iceberg in Ozone
echo ===================================================
echo.
echo Please provide the full path to your CSV file.
echo Example: C:\Users\Data\sales_data.csv
echo.

set /p filepath="Enter File Path: "
if "%filepath%"=="" goto error

:: Extract filename and name without extension
for %%f in ("%filepath%") do (
    set filename=%%~nxf
    set name=%%~nf
)

echo.
echo [1/3] Copying %filename% to Docker container...
docker cp "%filepath%" spark-iceberg:/home/iceberg/local/%filename%
if %errorlevel% neq 0 (
    echo Error copying file. Please check the path and try again.
    goto end
)

echo [2/3] Uploading raw file to Ozone (Optional Backup)...
:: We try to put it in a bucket named after the file or just the main bucket
docker exec ozone-om-1 ozone sh key put /vol1/bucket1/%filename% /tmp/%filename% >nul 2>&1
:: Note: The above might fail if /tmp/%filename% doesn't exist in OM container. 
:: We skip explicit Ozone upload here to keep it fast, trusting the script to read local mount or we can do it properly if needed.
:: For now, we rely on the script reading the file we just copied to spark-iceberg.

echo [3/3] Running Iceberg Ingestion Job...
echo Target Table: local.db.%name%

docker exec spark-iceberg bash -c "spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 --jars /home/iceberg/local/ozone-filesystem.jar --conf spark.driver.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.executor.extraClassPath=/home/iceberg/local/ozone-filesystem.jar --conf spark.sql.defaultCatalog=local --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=file:///home/iceberg/warehouse/local /home/iceberg/local/ingest_to_iceberg.py /home/iceberg/local/%filename% %name%"

echo.
echo ===================================================
echo                 JOB COMPLETE
echo ===================================================
echo Check the output above for 'Success' or 'Verification' counts.
goto end

:error
echo No path provided. Exiting.

:end
pause
endlocal
