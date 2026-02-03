from fastapi import FastAPI, HTTPException
import trino
import os

app = FastAPI(title="Ozone Data Lake API (PoC)")

# --- Configuration ---
# TRINO_HOST: Use 'localhost' if running on host with port 18082 mapped
# TRINO_PORT: Default mapped port for Trino in this lab is 18082
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "18082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
CATALOG = "iceberg"
SCHEMA = "trino_db"

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
        
        # Basic validation to prevent SQL injection in PoC
        if not table_name.isidentifier():
             raise HTTPException(status_code=400, detail="Invalid table name format")

        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        
        # Get column names from the description
        columns = [desc[0] for desc in cur.description]
        
        # Fetch data and convert rows to dictionaries
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
