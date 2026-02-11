# Local Data Lakehouse (MinIO + Iceberg + DuckDB + FastAPI)

A lightweight, local Data Lakehouse implementation demonstrating ACID transactions, Time Travel, and Schema Evolution on a laptop.

## 🏗️ Architecture

| Component | Technology | Role |
|-----------|------------|------|
| **Storage** | **MinIO** | S3-compatible Object Storage (runs in Docker). |
| **Table Format** | **Apache Iceberg** | Provides ACID transactions, Schema Evolution, and Time Travel. |
| **Query Engine** | **DuckDB & Trino** | DuckDB for local analytics, Trino for distributed SQL. |
| **API Layer** | **FastAPI** | REST API for Ingestion and Querying. |

## 🚀 Prerequisites

1.  **Docker Desktop** (for MinIO)
2.  **Python 3.9+**
3.  **Git**

## 🛠️ Setup Guide

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/Saksheee1408/Data_Lake_System.git
cd Data_Lake_System
pip install -r requirements.txt
pip install -r api/requirements.txt
```

### 2. Start Infrastructure
Start the MinIO object storage:
```bash
docker-compose up -d
```
*   **Console**: [http://localhost:9001](http://localhost:9001) (User/Pass: `minioadmin`)
*   **API**: [http://localhost:9000](http://localhost:9000)

### 3. Initialize Bucket
Create the `warehouse` bucket:
```bash
python setup_infra.py
```

## 🏃 Usage: CLI Tool (`lake_cli.py`)

A unified CLI is provided to interact with the Lakehouse API.

| Command | Usage | Description |
|---------|-------|-------------|
| **Ingest** | `python lake_cli.py ingest data/leads-100.csv sales` | Uploads CSV to `sales` table. |
| **Read** | `python lake_cli.py read sales --limit 10` | Queries the table using the API. |
| **Delete** | `python lake_cli.py delete sales 1,2,3 id` | Deletes rows by ID. |

## 🌐 Usage: Backend API

Expose the Lakehouse via a REST API.

### 1. Start the Server
```bash
cd api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
*   **Swagger Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)

### 2. Test the API End-to-End
Open a new terminal (keep uvicorn running) and run the automated test:
```bash
python tests/test_api.py
```
This script will:
1.  Generate a test CSV.
2.  POST it to `/upload`.
3.  GET `/query` to verify data availability.

## 📂 Project Structure

```
├── api/
│   ├── main.py              # FastAPI Application
│   └── requirements.txt     # API Dependencies
├── docker-compose.yml       # MinIO & Hive Metastore & Trino Services
├── hive_trino_setup/        # Configuration for Hive/Trino
├── spark_jobs/              # Spark Ingestion Scripts
├── lake_cli.py              # CLI Tool for API
├── db_connection.py         # DuckDB Connection Helper
├── tests/                   # Verification Scripts (test_api.py, etc.)
├── requirements.txt         # Core Dependencies
└── archive/                 # Legacy Scripts (create_table.py, etc.)
```

## 📝 Design Notes
- **Catalog**: Uses **Hive Metastore** to track table state, enabling Time Travel and Schema Evolution.
- **Copy-on-Write**: Deletes and Updates rewrite data files to ensure atomicity (Iceberg V1/V2 spec).
- **Concurrency**: DuckDB is embedded; for high concurrency, consider running it in read-only mode or using a catalog service like Nessie.
