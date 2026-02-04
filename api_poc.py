from fastapi import FastAPI, HTTPException, UploadFile, File
import trino
import os
import shutil
import subprocess
from pathlib import Path

app = FastAPI(title="Ozone Data Lake API (PoC)")

# --- Configuration ---
# TRINO_HOST: Use 'localhost' if running on host with port 18082 mapped
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "18082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
CATALOG = "iceberg"
SCHEMA = "trino_db"

# Path to the ingestion script RELATIVE TO THIS FILE
INGEST_SCRIPT = Path(__file__).parent / "ozone-integration-lab" / "ozone" / "run_ingestion.sh"
# Folder to store uploaded files (must be visible to Spark)
UPLOAD_DIR = Path(__file__).parent / "ozone-integration-lab" / "ozone"

def get_trino_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=CATALOG,
        schema=SCHEMA,
    )

@app.get("/")
async def welcome():
    """Health check and connection status."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        return {
            "message": "Welcome to the Ozone Data Lake API",
            "trino_version": version,
            "connection": "Active"
        }
    except Exception as e:
        return {
            "message": "API is running but Trino is unreachable",
            "error": str(e),
            "tip": f"Ensure Trino is running on {TRINO_HOST}:{TRINO_PORT}"
        }

@app.get("/tables")
async def list_tables():
    """Lists all Iceberg tables in the Ozone data lake."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]
        return {
            "catalog": CATALOG,
            "schema": SCHEMA,
            "tables": tables
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/{table_name}")
async def get_table_data(table_name: str, limit: int = 10):
    """Fetches records from a specific table as JSON."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        
        if not table_name.isidentifier():
             raise HTTPException(status_code=400, detail="Invalid table name format")

        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in cur.fetchall()]
            
        return {
            "table": table_name,
            "limit": limit,
            "count": len(data),
            "data": data
        }
    except Exception as e:
        if "does not exist" in str(e).lower():
             raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload/{table_name}")
async def upload_and_ingest(table_name: str, file: UploadFile = File(...)):
    """Uploads a CSV file and triggers background ingestion into the Data Lake."""
    try:
        # 1. Validation
        if not table_name.isidentifier():
            raise HTTPException(status_code=400, detail="Invalid table name format")
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")

        # 2. Save file to the lab directory (where Spark can see it)
        file_path = UPLOAD_DIR / file.filename
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 3. Trigger run_ingestion.sh as a background process
        # We use the relative path that run_ingestion.sh expects
        script_arg_path = f"ozone-integration-lab/ozone/{file.filename}"
        
        # We run it via 'bash' to ensure execution permissions
        cmd = ["bash", str(INGEST_SCRIPT), script_arg_path, table_name]
        
        # Subprocess runs in background
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        return {
            "message": "File uploaded successfully. Ingestion job triggered in background.",
            "file": file.filename,
            "target_table": table_name,
            "check_status": f"Check 'ingest_trino_log.txt' on server for progress"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
