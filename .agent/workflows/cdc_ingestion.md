---
description: Enterprise CDC Ingestion Workflow (MySQL/PG -> Debezium -> Kafka -> Iceberg)
---

This workflow describes the automated Change Data Capture (CDC) pipeline for the scalable enterprise data lake.

## 1. Database Configuration
Enable Binlog (MySQL) or Logical Replication (PostgreSQL) on the source database.
// turbo
1. Create a Debezium user with replication permissions.
2. Configure the Debezium connector via REST API or Kafka Connect config.

## 2. Event Capture (Debezium)
Debezium continuously monitors the database transaction logs.
- Every **INSERT**, **UPDATE**, and **DELETE** is captured as a JSON event.
- Events include "before" and "after" images of the data for consistency.

## 3. Real-time Streaming (Kafka)
Events are streamed into dedicated Kafka topics.
- Example Topic: `source_db.inventory.customers`
- Kafka ensures data is stored durably even if downstream systems are slow.

## 4. Iceberg Upsert (Spark Streaming)
A Spark Structured Streaming job consumes events from Kafka.
// turbo
3. Submit the streaming job:
```bash
docker exec spark-iceberg spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /home/iceberg/local/cdc_merger.py
```
- Spark performs a `MERGE INTO iceberg_db.customers` operation.
- Updates are applied atomically without locking the table.

## 5. Metadata Update & Commit
Iceberg updates its metadata manifest and creates a new **Snapshot**.
- The change is now physically saved on **Apache Ozone**.
- Old versions are kept for **Time Travel** and audit.

## 6. Instant Consumption
- Users query the latest state via **Trino**.
- The **Admin Dashboard** history sidebar shows the new "Append" or "Overwrite" snapshot instantly.
