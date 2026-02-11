# Data Lakehouse API Usage

## 🚀 Unified Endpoints (New)

### 1. Dynamic Ingestion
Unified endpoint to ingest data into either **Iceberg** or **Hudi** tables.

- **URL**: `/ingest`
- **Method**: `POST`
- **Parameters**:
    - `table`: Target table name (e.g. `default.leads`).
    - `file`: CSV file to upload.
    - `format`: Table format (`iceberg` or `hudi`). Default: `iceberg`.
    - `mode`: Write mode (`append` or `overwrite`). Only for Iceberg. Default: `append`.
    - `pkey`: Primary specific key column. **Required for Hudi**.
    - `partition`: Partition column. Optional.
    - `precombine`: Precombine field (e.g. timestamp). Optional for Hudi.

**Example (Iceberg)**:
```bash
curl -X POST "http://localhost:8000/ingest" \
  -F "table=default.sales_iceberg" \
  -F "file=@sales.csv" \
  -F "format=iceberg"
```

**Example (Hudi)**:
```bash
curl -X POST "http://localhost:8000/ingest" \
  -F "table=sales_hudi" \
  -F "file=@sales.csv" \
  -F "format=hudi" \
  -F "pkey=id" \
  -F "partition=region"
```

### 2. Unified Querying
Execute SQL queries via Trino on any catalog.

- **URL**: `/query`
- **Method**: `POST`
- **Parameters**:
    - `query`: SQL query string.
    - `catalog`: Catalog name (`iceberg` or `hudi`). Default: `iceberg`.

**Example**:
```bash
curl -X POST "http://localhost:8000/query" \
  -d "query=SELECT * FROM default.sales_iceberg LIMIT 10" \
  -d "catalog=iceberg"
```

---
 Guide

This guide details how to perform CRUD (Create, Read, Update, Delete) operations on your Data Lakehouse using simple Python `requests`.

## 1. Create / Ingest (Upload a New File)
To ingest data, upload a CSV file.
- **Endpoint:** `POST /ingest/hudi`
- **Parameters:**
  - `table`: Name of the table (e.g., `employees`)
  - `pkey`: Primary Key column name (e.g., `id`)
  - `file`: The CSV file path
  - `partition`: (Optional) Column to partition by (e.g., `dept`)

```python
import requests

url = "http://localhost:8000/ingest/hudi"
files = {'file': open('my_data.csv', 'rb')}
data = {
    'table': 'my_table',
    'pkey': 'id',
    'partition': 'category' # Optional
}

response = requests.post(url, files=files, data=data)
print(response.json())
```

```

## 1.1 Ingest via Spark (Iceberg)
To ingest data into an Iceberg table using Spark explicitly:
- **Endpoint:** `POST /ingest/iceberg/spark`
- **Parameters:**
  - `table`: Name of the table (e.g., `default.sales`)
  - `file`: The CSV file path
  - `mode`: `append` (default) or `overwrite`

```python
import requests

url = "http://localhost:8000/ingest/iceberg/spark"
files = {'file': open('sales.csv', 'rb')}
data = {
    'table': 'default.sales',
    'mode': 'append'
}

response = requests.post(url, files=files, data=data)
print(response.json())
```

## 2. Update (Modify Data)
To update existing records, simply **ingest the same CSV** (or a new CSV containing just the rows to change) with the **same Primary Keys**. Hudi handles the "Upsert" automatically.

```python
# To update ID 101's salary to 90000:
# Create a small CSV with just that row
csv_update = "id,name,category,salary\n101,John Doe,Engineering,90000"

files = {'file': ('update.csv', csv_update, 'text/csv')}
data = {'table': 'my_table', 'pkey': 'id', 'partition': 'category'}

response = requests.post(url, files=files, data=data)
print(response.json())
```

## 3. Read (Query Data)
You can read data using simple parameters without writing SQL.
- **Endpoint:** `GET /hudi/{table}/read`

```python
# Read all data
url = "http://localhost:8000/hudi/my_table/read"
resp = requests.get(url)
print(resp.json())

# Read specific columns
resp = requests.get(url, params={"columns": "id,name,salary"})

# Read with filter
resp = requests.get(url, params={"filter_col": "category", "filter_val": "Engineering"})
```

## 4. Delete (Remove Records)
To delete specific records, provide their Primary Keys.
- **Endpoint:** `POST /delete/hudi`

```python
url = "http://localhost:8000/delete/hudi"
data = {
    'table': 'my_table',
    'pkey': 'id',
    'ids': '101,103'  # Comma-separated list of IDs to delete
}

response = requests.post(url, data=data)
print(response.json())
```
