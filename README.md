# Ozone Unified Data Lake Project

An enterprise-grade Data Lake and CDC (Change Data Capture) pipeline built on **Apache Ozone**, **Apache Iceberg**, and **Kafka**, featuring real-time ingestion, field-level snapshot diffing (Time Travel), and ultra-precise Elasticsearch search.

## 🚀 Architecture Overview

This project implements a complete modern data stack:
*   **Sources**: MySQL and PostgreSQL databases.
*   **Capture**: **Debezium** captures row-level changes (CDC).
*   **Buffer**: **Kafka** stores streaming events safely.
*   **Processing**: **PySpark** merges streaming data into Iceberg tables.
*   **Storage**: **Apache Ozone** serves as the scalable object store (S3-compatible).
*   **Analytics**: **Trino** provides high-speed SQL access.
*   **Search**: **Elasticsearch** enables instant keyword search across billions of records.
*   **Dashboard**: A custom **FastAPI** Admin Portal for data management and time travel visualization.

---

## 🛠️ Setup Guide

### 1. Prerequisites
*   Docker and Docker Compose
*   Python 3.10+
*   Git

### 2. Clone and Install
```bash
git clone https://github.com/Madhuri-Jadhav107/datalake-on-prem-system.git
cd datalake-on-prem-system

# Create a Virtual Environment for the Dashboard
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn trino elasticsearch python-multipart
```

### 3. Start Infrastructure
Start the entire stack using Docker Compose:
```bash
cd ozone-integration-lab/ozone
docker compose -p madhuri-ozone up -d
```

### 4. Initialize Ozone Storage
Before ingesting data, you must create the required Volume and Bucket:
```bash
# Get OM container name
OM=$(docker ps --filter "label=com.docker.compose.service=om" --format "{{.Names}}")

# Create Volume and Bucket for Iceberg Warehouse
docker exec $OM ozone sh volume create /s3v
docker exec $OM ozone sh bucket create /s3v/warehouse-v2 --replication=1 --type=RATIS
```

---

## 📥 Data Ingestion Flows

### A. Smart CSV Ingestion (Batch)
Ingest any CSV file directly into an Iceberg table on Ozone:
```bash
bash smart_ingest.sh
# Follow the prompts to enter your CSV path (e.g., ./data/employees.csv)
```

### B. CDC Pipeline (Real-time)
1.  **Register Connector**: Tell Debezium to start watching your SQL databases:
    ```bash
    bash ozone-integration-lab/ozone/register_connector.sh
    ```
2.  **Start CDC Merger**: Ensure the PySpark merger is running to process Kafka events:
    ```bash
    docker exec madhuri-ozone-spark-iceberg-1 spark-submit \
      --master local[*] \
      /home/iceberg/local/ozone-integration-lab/ozone/cdc_merger.py
    ```

### C. Elasticsearch Search Sync
Synchronize your Iceberg data to Elasticsearch for high-speed searching:
```bash
docker exec madhuri-ozone-spark-iceberg-1 spark-submit \
  --master local[*] \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.17.10,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026 \
  /home/iceberg/local/ozone-integration-lab/ozone/sync_to_es.py cdc_customers
```

---

## 🖥️ Using the Admin Dashboard

The dashboard provides a unified view of your Data Lake with advanced search and snapshot comparison.

### Start the Dashboard
```bash
cd ~/datalake-on-prem-system
source venv/bin/activate
python3 -m uvicorn api_poc:app --host 0.0.0.0 --port 8000
```
Access via: `http://localhost:8000/view/cdc_customers`

### Key Features
*   **Time Travel**: Click any snapshot ID in the sidebar to "roll back" the view to that point in time.
*   **Field-Level Diffing**: Modified rows are highlighted in **Yellow**, and changed cells show `Old Value → New Value`.
*   **Enterprise Search**: Instant phrase matching across 160 Crore+ records using Elasticsearch.

---

## 🔧 Troubleshooting
*   **S3 Credentials**: If Spark fails to connect, fetch fresh credentials using `docker exec madhuri-ozone-om-1 ozone s3 getsecret -u hadoop`.
*   **Container Health**: Use `docker ps` to ensure `trino`, `om`, `s3g`, and `kafka` are all healthy.
*   **Logs**: Check Spark logs with `docker logs -f madhuri-ozone-spark-iceberg-1`.
