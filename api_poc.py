from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import trino
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

app = FastAPI(title="Ozone Data Lake Admin Portal")

# --- Configuration ---
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "18082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
CATALOG = "iceberg"
SCHEMA = "trino_db"

INGEST_SCRIPT = Path(__file__).parent / "ozone-integration-lab" / "ozone" / "run_ingestion.sh"
UPLOAD_DIR = Path(__file__).parent / "ozone-integration-lab" / "ozone"

def get_trino_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=CATALOG,
        schema=SCHEMA,
    )

@app.get("/", response_class=HTMLResponse)
async def home_portal():
    """Portal Home Page with list of tables."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]
        
        table_links = "".join([f'<li><a href="/view/{t}" class="btn-table">{t}</a></li>' for t in tables])
        
        return f"""
        <html>
            <head>
                <title>Ozone Data Lake Portal</title>
                <style>
                    body {{ font-family: 'Inter', system-ui, sans-serif; margin: 0; background: #f0f2f5; color: #1c1e21; }}
                    .hero {{ background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); color: white; padding: 60px 20px; text-align: center; }}
                    .container {{ max-width: 900px; margin: -40px auto 40px; background: white; padding: 40px; border-radius: 12px; box-shadow: 0 8px 24px rgba(0,0,0,0.1); }}
                    h1 {{ margin: 0; font-size: 2.5em; }}
                    ul {{ list-style: none; padding: 0; display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 15px; margin-top: 30px; }}
                    .btn-table {{ display: block; padding: 20px; background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; text-decoration: none; color: #1a73e8; font-weight: 600; text-align: center; transition: 0.2s; }}
                    .btn-table:hover {{ background: #e8f0fe; border-color: #1a73e8; transform: translateY(-3px); }}
                </style>
            </head>
            <body>
                <div class="hero">
                    <h1>Ozone Data Lake Admin Portal</h1>
                    <p>Powered by Apache Iceberg & Trino</p>
                </div>
                <div class="container">
                    <h2>Managed Iceberg Tables</h2>
                    <ul>{table_links}</ul>
                    <hr style="margin: 40px 0; border: 0; border-top: 1px solid #eee;">
                    <h3>Quick Actions</h3>
                    <p>To ingest new data, use the <code>/upload</code> endpoint or the CLI helper.</p>
                </div>
            </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error reaching Trino</h1><p>{str(e)}</p>"

@app.get("/tables")
async def api_list_tables():
    """API Endpoint: Lists all Iceberg tables."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]
        return {"catalog": CATALOG, "schema": SCHEMA, "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/{table_name}")
async def get_table_data(table_name: str, limit: int = 10):
    """API Endpoint: Fetches records as JSON."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        if not table_name.isidentifier(): raise HTTPException(status_code=400, detail="Invalid table name")
        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"table": table_name, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload/{table_name}")
async def upload_and_ingest(table_name: str, file: UploadFile = File(...)):
    """API Endpoint: Automated Ingestion."""
    try:
        if not table_name.isidentifier(): raise HTTPException(status_code=400, detail="Invalid name")
        file_path = UPLOAD_DIR / file.filename
        with file_path.open("wb") as buffer: shutil.copyfileobj(file.file, buffer)
        script_arg_path = f"ozone-integration-lab/ozone/{file.filename}"
        cmd = ["bash", str(INGEST_SCRIPT), script_arg_path, table_name]
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return {"message": "Ingestion triggered", "table": table_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/view/{table_name}", response_class=HTMLResponse)
async def dashboard_view(table_name: str, search: Optional[str] = None):
    """The Main Admin Dashboard UI."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        
        # 1. Fetch Data (with search if provided)
        query = f"SELECT * FROM {table_name}"
        if search:
            # Basic search across all columns (conceptually)
            # For PoC, we search the first few common names or just filter by ID if it's a number
            if search.isdigit():
                query += f" WHERE id = {search}"
            else:
                # Try to find a 'name' or 'user_id' or 'category' column to search
                cur.execute(f"DESCRIBE {table_name}")
                cols = [row[0] for row in cur.fetchall()]
                search_targets = [c for c in cols if c in ['name', 'user_id', 'category', 'status', 'department']]
                if search_targets:
                    filters = " OR ".join([f"CAST({c} AS VARCHAR) LIKE '%{search}%'" for c in search_targets])
                    query += f" WHERE {filters}"
        
        query += " LIMIT 50"
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        # 2. Fetch Snapshots
        cur.execute(f'SELECT snapshot_id, committed_at, operation FROM "{table_name}$snapshots" ORDER BY committed_at DESC LIMIT 5')
        snapshots = cur.fetchall()

        # UI Components
        snapshot_list = "".join([f'<li><b>{s[2]}</b> at {s[1]} <br><small>{s[0]}</small></li>' for s in snapshots])
        
        table_headers = "".join([f"<th>{c}</th>" for c in columns])
        table_rows = ""
        for r in rows:
            row_cells = "".join([f"<td>{v}</td>" for v in r])
            # Add a delete button for each row (using first column as ID)
            delete_btn = f'<td><form action="/delete/{table_name}/{r[0]}" method="POST" style="margin:0"><button class="btn-delete">Delete</button></form></td>'
            table_rows += f"<tr>{row_cells}{delete_btn}</tr>"

        return f"""
        <html>
            <head>
                <title>Dashboard: {table_name}</title>
                <style>
                    body {{ font-family: 'Segoe UI', system-ui; margin: 20px; background: #f4f7f6; }}
                    .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
                    .grid {{ display: grid; grid-template-columns: 3fr 1fr; gap: 20px; }}
                    .card {{ background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }}
                    table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
                    th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
                    th {{ background: #f8f9fa; color: #555; text-transform: uppercase; font-size: 0.85em; }}
                    .btn-add {{ background: #27ae60; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; }}
                    .btn-delete {{ background: #e74c3c; color: white; border: none; padding: 5px 10px; border-radius: 4px; cursor: pointer; font-size: 0.8em; }}
                    .search-box {{ padding: 10px; width: 300px; border: 1px solid #ddd; border-radius: 6px; }}
                    .snapshot-card ul {{ padding: 0; list-style: none; font-size: 0.9em; }}
                    .snapshot-card li {{ padding: 10px 0; border-bottom: 1px solid #f0f0f0; }}
                    .snapshot-card li:last-child {{ border: 0; }}
                    .badge {{ background: #3498db; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.7em; vertical-align: middle; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <div>
                        <a href="/" style="text-decoration:none; color:#777">← Back to Portal</a>
                        <h1 style="margin:5px 0">{table_name} <span class="badge">ICEBERG</span></h1>
                    </div>
                    <form action="/view/{table_name}" method="GET">
                        <input type="text" name="search" class="search-box" placeholder="Search by ID or Name..." value="{search or ''}">
                        <button type="submit" style="padding:10px">Search</button>
                    </form>
                </div>

                <div class="grid">
                    <div class="card">
                        <div style="display:flex; justify-content:space-between">
                            <h3>Table Records</h3>
                            <button class="btn-add" onclick="document.getElementById('add-form').style.display='block'">+ Insert Record</button>
                        </div>
                        
                        <div id="add-form" style="display:none; background:#f9f9f9; padding:20px; margin:20px 0; border-radius:8px; border:1px solid #eee">
                            <h4>Insert New Record</h4>
                            <form action="/insert/{table_name}" method="POST">
                                {"".join([f'<input type="text" name="{c}" placeholder="{c}" style="margin:5px; padding:8px" required>' for c in columns])}
                                <button type="submit" class="btn-add">Save to Data Lake</button>
                                <button type="button" onclick="this.parentElement.parentElement.style.display='none'" style="background:none; border:none; color:red; cursor:pointer">Cancel</button>
                            </form>
                        </div>

                        <table>
                            <thead><tr>{table_headers}<th>Actions</th></tr></thead>
                            <tbody>{table_rows}</tbody>
                        </table>
                    </div>

                    <div class="card snapshot-card">
                        <h3>Snapshot History</h3>
                        <p style="font-size:0.8em; color:#666">Iceberg Time-Travel Log</p>
                        <ul>{snapshot_list}</ul>
                        <hr>
                        <p><small>Every change creates a new snapshot automatically.</small></p>
                    </div>
                </div>
            </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error loading dashboard</h1><p>{str(e)}</p>"

@app.post("/insert/{table_name}")
async def insert_record(table_name: str, request: Request):
    """Handles manual record insertion into Trino."""
    try:
        form_data = await request.form()
        cols = ", ".join(form_data.keys())
        vals = ", ".join([f"'{v}'" if not str(v).replace('.','',1).isdigit() else str(v) for v in form_data.values()])
        
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {table_name} ({cols}) VALUES ({vals})")
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/delete/{table_name}/{record_id}")
async def delete_record(table_name: str, record_id: str):
    """Handles record deletion by the first column (assumed ID)."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        # Assume first column name is 'id' or get it
        cur.execute(f"DESCRIBE {table_name}")
        id_col = cur.fetchone()[0]
        
        query = f"DELETE FROM {table_name} WHERE {id_col} = "
        query += f"{record_id}" if record_id.isdigit() else f"'{record_id}'"
        
        cur.execute(query)
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
