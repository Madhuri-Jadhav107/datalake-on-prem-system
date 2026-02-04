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
                    
                    <h3>🚀 Ingest New Data</h3>
                    <p>Upload a CSV file to automatically create/update a table in the Data Lake.</p>
                    <div style="background: #f8f9fa; padding: 25px; border-radius: 8px; border: 1px solid #dee2e6;">
                        <form action="/upload-ui" method="POST" enctype="multipart/form-data">
                            <div style="margin-bottom: 15px;">
                                <label style="display:block; margin-bottom: 5px; font-weight: 600;">Target Table Name:</label>
                                <input type="text" name="table_name" placeholder="e.g. sales_data" style="padding: 10px; width: 100%; border: 1px solid #ddd; border-radius: 4px;" required>
                            </div>
                            <div style="margin-bottom: 15px;">
                                <label style="display:block; margin-bottom: 5px; font-weight: 600;">Select CSV File:</label>
                                <input type="file" name="file" accept=".csv" style="padding: 10px; width: 100%; border: 1px solid #ddd; border-radius: 4px; background: white;" required>
                            </div>
                            <button type="submit" class="btn-table" style="width: 100%; background: #1a73e8; color: white; border: none; cursor: pointer;">Start Automated Ingestion</button>
                        </form>
                    </div>
                </div>
            </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error reaching Trino</h1><p>{str(e)}</p>"

@app.post("/upload-ui")
async def upload_ui(table_name: str = Form(...), file: UploadFile = File(...)):
    """Handles file upload from the Web UI and redirects to home."""
    try:
        await upload_and_ingest(table_name, file)
        return RedirectResponse(url="/?msg=Ingestion+Started", status_code=303)
    except Exception as e:
        return f"<h1>Upload Failed</h1><p>{str(e)}</p><a href='/'>Go Back</a>"

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
            # Add Edit and Delete buttons for each row (using first column as ID)
            actions = f"""
                <td style="display:flex; gap:5px">
                    <a href="/edit/{table_name}/{r[0]}" class="btn-edit" style="text-decoration:none">Edit</a>
                    <form action="/delete/{table_name}/{r[0]}" method="POST" style="margin:0">
                        <button class="btn-delete">Delete</button>
                    </form>
                </td>
            """
            table_rows += f"<tr>{row_cells}{actions}</tr>"

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
                    .btn-edit {{ background: #f39c12; color: white; border: none; padding: 5px 10px; border-radius: 4px; cursor: pointer; font-size: 0.8em; }}
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

@app.get("/edit/{table_name}/{record_id}", response_class=HTMLResponse)
async def edit_record_page(table_name: str, record_id: str):
    """Shows a form to edit an existing record."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        
        cur.execute(f"DESCRIBE {table_name}")
        schema = cur.fetchall()
        id_col = schema[0][0]
        
        # Fetch current values
        query = f"SELECT * FROM {table_name} WHERE {id_col} = "
        query += f"{record_id}" if record_id.isdigit() else f"'{record_id}'"
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        
        if not row:
            return f"<h1>Record not found</h1>"
            
        form_fields = ""
        for col, val in zip(columns, row):
            readonly = "readonly style='background:#eee'" if col == id_col else ""
            form_fields += f"""
            <div style="margin-bottom:15px">
                <label style="display:block; font-weight:600">{col}</label>
                <input type="text" name="{col}" value="{val}" {readonly} style="width:100%; padding:10px; border:1px solid #ddd; border-radius:4px">
            </div>
            """

        return f"""
        <html>
            <head>
                <title>Edit Record: {record_id}</title>
                <style>
                    body {{ font-family: 'Segoe UI', system-ui; margin: 40px; background: #f4f7f6; }}
                    .card {{ background: white; padding: 40px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); max-width: 500px; margin: auto; }}
                    .btn-save {{ background: #f39c12; color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; width: 100%; font-size: 1em; }}
                </style>
            </head>
            <body>
                <div class="card">
                    <h2>Edit Record in {table_name}</h2>
                    <form action="/update/{table_name}/{record_id}" method="POST">
                        {form_fields}
                        <button type="submit" class="btn-save">Update Data Lake</button>
                    </form>
                    <p style="text-align:center"><a href="/view/{table_name}" style="color:#777">Cancel and Go Back</a></p>
                </div>
            </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error loading edit page</h1><p>{str(e)}</p>"

async def get_cast_val(col_type: str, val: str):
    """Helper to cast string form values to Trino types."""
    col_type = col_type.lower()
    if "int" in col_type:
        return int(val) if val else 0
    elif "double" in col_type or "decimal" in col_type or "real" in col_type:
        return float(val) if val else 0.0
    return str(val)

@app.post("/update/{table_name}/{record_id}")
async def update_record(table_name: str, record_id: str, request: Request):
    """Handles manual record update in Trino with automatic type casting."""
    try:
        form_data = await request.form()
        conn = get_trino_conn()
        cur = conn.cursor()
        
        cur.execute(f"DESCRIBE {table_name}")
        schema_info = {row[0]: row[1] for row in cur.fetchall()}
        id_col = list(schema_info.keys())[0]
        
        update_parts = []
        vals = []
        
        for col, val in form_data.items():
            if col == id_col: continue # Don't update the ID
            update_parts.append(f"{col} = ?")
            vals.append(await get_cast_val(schema_info[col], val))

        # Add the ID for the WHERE clause
        vals.append(await get_cast_val(schema_info[id_col], record_id))
        
        query = f"UPDATE {table_name} SET {', '.join(update_parts)} WHERE {id_col} = ?"
        cur.execute(query, vals)
        
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

@app.post("/insert/{table_name}")
async def insert_record(table_name: str, request: Request):
    """Handles manual record insertion into Trino with automatic type casting."""
    try:
        form_data = await request.form()
        conn = get_trino_conn()
        cur = conn.cursor()
        
        # 1. Fetch column types from the table to handle strict Trino typing
        cur.execute(f"DESCRIBE {table_name}")
        schema_info = {row[0]: row[1] for row in cur.fetchall()} # {col_name: type_name}
        
        cols = []
        vals = []
        params = []
        
        for col, val in form_data.items():
            if col not in schema_info: continue
            cols.append(col)
            
            # 2. Cast string values from form to the correct Trino type
            vals.append(await get_cast_val(schema_info[col], val))
            params.append("?")

        cols_str = ", ".join(cols)
        params_str = ", ".join(params)
        
        # 3. Use parameterized query to prevent SQL injection and fix formatting
        query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({params_str})"
        cur.execute(query, vals)
        
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        # Provide better error feedback
        print(f"Insert Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Insert failed: {str(e)}")

@app.post("/delete/{table_name}/{record_id}")
async def delete_record(table_name: str, record_id: str):
    """Handles record deletion by the first column (assumed ID)."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        
        # Get the prime column (usually ID)
        cur.execute(f"DESCRIBE {table_name}")
        schema = cur.fetchall()
        id_col = schema[0][0]
        id_type = schema[0][1].lower()
        
        # Cast the ID correctly
        if "int" in id_type:
            val = int(record_id)
        elif "double" in id_type or "real" in id_type:
            val = float(record_id)
        else:
            val = record_id
            
        cur.execute(f"DELETE FROM {table_name} WHERE {id_col} = ?", (val,))
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
