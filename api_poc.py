from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import trino
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional
from datetime import datetime
from elasticsearch import Elasticsearch

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

ES_HOST = os.getenv("ES_HOST", "http://localhost:19200")

def get_es_client():
    try:
        return Elasticsearch([ES_HOST])
    except:
        return None

# --- Global Safety Helpers ---
def safe_execute(cursor, query_str, params=None):
    """Executes a query with detailed logging for debugging."""
    try:
        if params: cursor.execute(query_str, params)
        else: cursor.execute(query_str)
    except Exception as e:
        print(f"\n❌ TRINO ERROR: {str(e)}")
        print(f"FAILED QUERY: {query_str}")
        if params: print(f"PARAMS: {params}")
        raise

async def es_search(table_name: str, keyword: str, id_col: str):
    """Hits Elasticsearch for high-speed keyword search across all columns with precision."""
    try:
        es = get_es_client()
        if not es: return None
        
        index_name = table_name.lower()
        
        # High precision query: 
        # 1. 'must' ensures ALL words are present (operator: and)
        # 2. 'match_phrase' boosts the exact sequence for perfect matches
        res = es.search(index=index_name, body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": keyword,
                                "fields": ["*"],
                                "type": "cross_fields",
                                "operator": "and"
                            }
                        }
                    ],
                    "should": [
                        {
                            "multi_match": {
                                "query": keyword,
                                "fields": ["*"],
                                "type": "phrase",
                                "boost": 10.0
                            }
                        }
                    ]
                }
            },
            "size": 50
        })
        
        hits = res['hits']['hits']
        if not hits: return []
        
        results = []
        for hit in hits:
            source = hit['_source']
            found_id = None
            # Check for ID column case-insensitively
            for key, val in source.items():
                if key.lower() == id_col.lower():
                    found_id = val
                    break
            
            # Map back to string for the Trino IN clause
            results.append(str(found_id if found_id is not None else hit['_id']))
            
        return results
    except Exception as e:
        print(f"ES Search Error: {e}")
        return None

@app.get("/", response_class=HTMLResponse)
async def home_portal():
    """Portal Home Page with list of tables grouped by source."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]
        
        # Render all tables in a single list
        table_links = "".join([f'<li><a href="/view/{table}" class="btn-table">{table}</a></li>' for table in tables])
        
        return f"""
        <html>
            <head>
                <title>Ozone Data Lake Admin Portal</title>
                <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=Outfit:wght@700&display=swap" rel="stylesheet">
                <style>
                    body {{ font-family: 'Inter', sans-serif; margin: 0; background: #f8fafc; color: #0f172a; line-height: 1.5; }}
                    .hero {{ background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); color: white; padding: 60px 20px; text-align: center; border-bottom: 4px solid #3b82f6; }}
                    .hero h1 {{ font-family: 'Outfit', sans-serif; font-size: 3rem; margin-bottom: 5px; }}
                    .container {{ max-width: 1100px; margin: -40px auto 60px; background: white; padding: 40px; border-radius: 20px; box-shadow: 0 20px 25px -5px rgb(0 0 0 / 0.1); }}
                    .grid {{ display: grid; grid-template-columns: 1fr; gap: 30px; margin-top: 20px; }}
                    .card {{ background: #f1f5f9; padding: 25px; border-radius: 16px; border-top: 5px solid #cbd5e1; }}
                    .iceberg-card {{ border-color: #3b82f6; }}
                    ul {{ list-style: none; padding: 0; display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 15px; }}
                    .btn-table {{ display: block; padding: 15px; background: white; border-radius: 8px; text-decoration: none; font-weight: 600; transition: 0.2s; box-shadow: 0 1px 3px rgba(0,0,0,0.1); color: #0f172a; text-align: center; }}
                    .btn-table:hover {{ transform: scale(1.02); box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); color: #3b82f6; }}
                    .btn-sql {{ display: inline-block; padding: 12px 24px; background: #3b82f6; color: white; text-decoration: none; border-radius: 8px; font-weight: bold; }}
                </style>
            </head>
            <body>
                <div class="hero">
                    <h1>Ozone Unified Data Lake</h1>
                    <p>One Platform • Multiple Sources • Infinite Insights</p>
                </div>
                <div class="container">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px;">
                        <h2 style="font-family: 'Outfit'; margin: 0;">🗂️ Data Catalog</h2>
                        <a href="/sql-workspace" class="btn-sql">⚡ Open SQL Workspace</a>
                    </div>

                    <div class="grid">
                        <div class="card iceberg-card">
                            <h3>🧊 Data Lake Tables (Iceberg)</h3>
                            <ul>{table_links}</ul>
                        </div>
                    </div>

                    <div style="margin-top: 50px; background: #0f172a; color: white; padding: 40px; border-radius: 20px;">
                        <h3 style="color: #3b82f6; margin-top: 0;">🚀 Ingest New CSV to Source</h3>
                        <p style="opacity: 0.7;">Upload a data file and align it to a source system in the lake.</p>
                        <form action="/upload-ui" method="POST" enctype="multipart/form-data" style="margin-top: 25px;">
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                                <div>
                                    <label style="display:block; margin-bottom: 5px; font-weight: 600;">Table Name:</label>
                                    <input type="text" name="table_name" placeholder="e.g. sales_2024" style="padding: 10px; width: 100%; border-radius: 6px; border: 1px solid #334155; background: #1e293b; color: white;" required>
                                </div>
                                <div>
                                    <label style="display:block; margin-bottom: 5px; font-weight: 600;">CSV File:</label>
                                    <input type="file" name="file" accept=".csv" style="padding: 7px; width: 100%; border-radius: 6px; border: 1px solid #334155; background: #1e293b; color: white;" required>
                                </div>
                            </div>
                            <button type="submit" style="width: 100%; margin-top: 20px; padding: 15px; background: #3b82f6; color: white; border: none; border-radius: 10px; font-weight: bold; cursor: pointer;">Start Ingestion</button>
                        </form>
                    </div>
                </div>
            </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error reaching Trino</h1><p>{str(e)}</p>"

@app.get("/sql-workspace", response_class=HTMLResponse)
async def sql_workspace(query: Optional[str] = None):
    """SQL Query Workspace for direct Trino interaction."""
    results = []
    columns = []
    error = None
    if query:
        try:
            # SANITIZATION: Trino's Python driver often rejects trailing semicolons
            sanitized_query = query.strip().rstrip(";")
            
            conn = get_trino_conn()
            cur = conn.cursor()
            cur.execute(sanitized_query)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                results = cur.fetchall()
        except Exception as e:
            error = str(e)

    return f"""
    <html>
        <head>
            <title>Data Lake SQL Workspace</title>
            <style>
                body {{ font-family: 'Inter', sans-serif; margin: 40px; background: #f8fafc; }}
                .editor-card {{ background: white; padding: 30px; border-radius: 20px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }}
                textarea {{ width: 100%; height: 150px; padding: 15px; border: 1px solid #cbd5e1; border-radius: 8px; font-family: monospace; font-size: 1rem; margin-bottom: 15px; }}
                .btn-run {{ background: #3b82f6; color: white; padding: 12px 30px; border-radius: 8px; border: none; font-weight: bold; cursor: pointer; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 30px; background: white; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
                th {{ background: #f1f5f9; color: #475569; text-transform: uppercase; font-size: 0.8rem; font-weight: 700; }}
            </style>
        </head>
        <body>
            <a href="/" style="text-decoration:none; color:#64748b">← Back to Catalog</a>
            <h1 style="font-family: 'Outfit'; margin-top: 10px;">⚡ SQL Workspace</h1>
            
            <div class="editor-card">
                <form action="/sql-workspace" method="GET">
                    <label style="font-weight:600; display:block; margin-bottom:10px">Enter Your Trino SQL Query:</label>
                    <textarea name="query" placeholder="SELECT * FROM cdc_mysql_customers LIMIT 10">{query or ''}</textarea>
                    <div style="display:flex; gap:10px">
                        <button type="submit" class="btn-run">Execute Query</button>
                        <button type="button" onclick="document.getElementsByName('query')[0].value='SELECT * FROM cdc_mysql_customers LIMIT 10'" style="padding:10px; border-radius:8px; border:1px solid #ddd; background:none; cursor:pointer">Example: MySQL</button>
                        <button type="button" onclick="document.getElementsByName('query')[0].value='SELECT * FROM cdc_customers LIMIT 10'" style="padding:10px; border-radius:8px; border:1px solid #ddd; background:none; cursor:pointer">Example: Postgres</button>
                    </div>
                </form>

                {f'<div style="background:#fee2e2; color:#b91c1c; padding:15px; border-radius:8px; margin-top:20px"><b>SQL Error:</b> {error}</div>' if error else ''}

                {f'''<table><thead><tr>{" ".join([f"<th>{c}</th>" for c in columns])}</tr></thead><tbody>{" ".join([f"<tr>{' '.join([f'<td>{v}</td>' for v in r])}</tr>" for r in results])}</tbody></table>''' if results else ''}
            </div>
        </body>
    </html>
    """

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
async def dashboard_view(table_name: str, search: Optional[str] = None, snapshot: Optional[str] = None):
    """The Main Admin Dashboard UI with Time Travel & Enterprise Search support."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        
        # 1. Construct Query (Standard or Time Travel)
        # Use full catalog.schema.table for absolute safety
        full_table_path = f'"{CATALOG}"."{SCHEMA}"."{table_name}"'
        base_query = f"SELECT * FROM {full_table_path}"
        
        if snapshot:
             # Iceberg Time Travel Syntax: FOR VERSION AS OF <id>
             base_query += f" FOR VERSION AS OF {snapshot}"
             
        query = base_query
        
        # 1.5 Discovery: Identify ID column and Searchable columns early
        # This ensures id_col is available for both Search and Snapshot Diffing
        cur.execute(f"DESCRIBE {full_table_path}")
        cols_info = cur.fetchall()
        all_cols = [row[0] for row in cols_info]
        id_col = all_cols[0] if all_cols else "id"
        
        # Identify VARCHAR/STRING columns for standard search fallback
        string_cols = [row[0] for row in cols_info if 'varchar' in str(row[1]).lower() or 'string' in str(row[1]).lower()]
        search_targets = string_cols if string_cols else all_cols
        
        # 2. Handle Search (Enterprise ES Search vs. Trino Fallback)
        using_es = False
        where_clause = ""
        params = []
        if search:
            # TRY ELASTICSEARCH FIRST (for 1.6B record scale)
            ids = await es_search(table_name, search, id_col)
            
            if ids is not None and len(ids) > 0:
                # Use ES results to filter Trino
                id_list = ",".join([f"'{i}'" for i in ids])
                where_clause = f' WHERE CAST("{id_col}" AS VARCHAR) IN ({id_list})'
                using_es = True
            else:
                # FALLBACK to standard Trino search
                if search.isdigit():
                    # Direct ID match if input is numeric
                    where_clause = f' WHERE CAST("{id_col}" AS VARCHAR) = \'{search}\''
                else:
                    # Multi-column substring match
                    search_cols = list(set([id_col] + search_targets))
                    filters = " OR ".join([f'CAST("{c}" AS VARCHAR) LIKE ?' for c in search_cols])
                    where_clause = f" WHERE ({filters})"
                    params = [f"%{search}%"] * len(search_cols)
                using_es = False
        
        # Finalize the main query with WHERE, ORDER BY, and LIMIT
        query = f"{base_query} {where_clause} ORDER BY \"{id_col}\" DESC LIMIT 100"
        
        # 2.5 Manual Snapshot Diffing (State Comparison)
        changed_rows = {} # {id_val: "ADDED/MODIFIED"}
        if snapshot:
            try:
                # 1. Discover Parent Snapshot
                cur.execute(f'SELECT "parent_id" FROM "{CATALOG}"."{SCHEMA}"."{table_name}$snapshots" WHERE "snapshot_id" = {snapshot}')
                res = cur.fetchone()
                parent_id = res[0] if res else None
                
                if parent_id:
                    print(f"DEBUG: Comparing snapshot {snapshot} with parent {parent_id}")
                    # Fetch parent state for the same data 
                    # Use exactly the same filters, ordering, and limit
                    parent_full_query = query.replace(full_table_path, f"{full_table_path} FOR VERSION AS OF {parent_id}")
                    try:
                        if params:
                            safe_execute(cur, parent_full_query, params)
                        else:
                            safe_execute(cur, parent_full_query)
                        
                        parent_rows = cur.fetchall()
                        # Create a map of ID -> Full Row for comparison
                        parent_map = {str(r[0]): r for r in parent_rows}
                    except Exception as pe:
                        print(f"Parent Data Fetch Failed (Snapshot might be too old): {pe}")
                        parent_map = {}
                else:
                    parent_map = {}
            except Exception as e:
                print(f"Parent Discovery Failed: {e}")
                parent_map = {}

        # Execute main data query
        if params:
            safe_execute(cur, query, params)
        else:
            safe_execute(cur, query)
            
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        # 2.6 Perform Comparison
        if snapshot:
            for r in rows:
                rid = str(r[0])
                if rid not in parent_map:
                    # ONLY mark as NEW if parent_map actually has data (to avoid false positives if parent fetch failed)
                    if parent_map:
                        changed_rows[rid] = "NEW"
                else:
                    parent_row = parent_map[rid]
                    # Direct comparison is safer than hash()
                    if tuple(r) != tuple(parent_row):
                        changed_rows[rid] = "MODIFIED"

        # 3. Fetch Snapshot history for the sidebar (quoted metadata table)
        snapshot_query = f'SELECT "snapshot_id", "committed_at", "operation" FROM "{CATALOG}"."{SCHEMA}"."{table_name}$snapshots" ORDER BY "committed_at" DESC LIMIT 15'
        safe_execute(cur, snapshot_query)
        snapshots_data = cur.fetchall()

        # 4. Build UI components
        source_badge = ""
        if "mysql" in table_name.lower():
            source_badge = '<span class="badge" style="background:#f29111; margin-left:10px">MySQL Source</span>'
        elif "cdc" in table_name.lower() or "customer" in table_name.lower():
            source_badge = '<span class="badge" style="background:#336791; margin-left:10px">Postgres Source</span>'

        snapshot_list = ""
        for s in snapshots_data:
            # Important: Ensure string comparison for snapshot ID
            is_active = "border-left: 6px solid #f39c12; background: #fff8eb; font-weight: bold;" if str(s[0]) == str(snapshot) else ""
            search_param = f"&search={search}" if search else ""
            snapshot_list += f"""
                <li style='margin-bottom:12px; padding:12px; border-radius:8px; border:1px solid #e2e8f0; {is_active}; transition: 0.2s;'>
                    <a href='/view/{table_name}?snapshot={s[0]}{search_param}' style='text-decoration:none; color:inherit; display:block;'>
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:5px">
                            <span class="badge" style="background:#475569">{s[2]}</span>
                            <small style="color:#64748b; font-size:0.75rem">{str(s[1])[:19]}</small>
                        </div>
                        <small style="font-family:monospace; color:#94a3b8">ID: {str(s[0])[:12]}...</small>
                    </a>
                </li>
            """
        
        time_travel_banner = ""
        if snapshot:
            time_travel_banner = f"""
            <div style="background: #fff3cd; color: #856404; padding: 15px; border-radius: 8px; border: 1px solid #ffeeba; margin-bottom: 20px; display:flex; justify-content:space-between; align-items:center">
                <span>⚠️ <b>TIME TRAVEL ACTIVE:</b> Viewing data from snapshot <code>{snapshot}</code></span>
                <a href="/view/{table_name}{'?search='+search if search else ''}" style="background:#856404; color:white; padding:5px 12px; border-radius:4px; text-decoration:none; font-size:0.9em">Return to Latest</a>
            </div>
            """

        table_headers = "".join([f"<th>{c}</th>" for c in columns])
        if snapshot: table_headers += "<th>Status</th>" # Add status col only in snapshot view
        
        table_rows = ""
        for r in rows:
            row_id = str(r[0])
            row_style = ""
            status_cell = ""
            
            if snapshot and row_id in changed_rows:
                op = changed_rows[row_id]
                if op == "NEW":
                    row_style = "background-color: #dcfce7; color: #166534;" # Green for new rows
                else:
                    # Distinct highlight for modified rows
                    row_style = "background-color: #fffbeb; color: #856404;" # Light Yellow
                status_cell = f'<td><span class="badge bg-success">{op}</span></td>'
            elif snapshot:
                status_cell = '<td><small style="color:#999">-</small></td>'

            row_cells = ""
            for i, val in enumerate(r):
                cell_content = str(val)
                # If MODIFIED, check if THIS specific field changed
                if snapshot and row_id in changed_rows and changed_rows[row_id] == "MODIFIED":
                    if row_id in parent_map:
                        parent_val = parent_map[row_id][i]
                        if str(parent_val) != str(val):
                            # Show diff in the cell
                            cell_content = f'<span style="color:#94a3b8; text-decoration:line-through; font-size:0.8em; margin-right:5px">{parent_val}</span><b style="color:#166534">→ {val}</b>'

                row_cells += f"<td>{cell_content}</td>"
            actions = ""
            if not snapshot: # Only allow edits on the 'latest' view
                actions = f"""
                    <td style="display:flex; gap:5px">
                        <a href="/edit/{table_name}/{r[0]}" class="btn-edit" style="text-decoration:none">Edit</a>
                        <form action="/delete/{table_name}/{r[0]}" method="POST" style="margin:0">
                            <button class="btn-delete">Delete</button>
                        </form>
                    </td>
                """
            else:
                actions = "<td><small style='color:#999'>Read Only</small></td>"
            
            table_rows += f"<tr style='{row_style}'>{row_cells}{status_cell}{actions}</tr>"

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
                    .badge {{ background: #3498db; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.7em; vertical-align: middle; }}
                    .bg-success {{ background: #22c55e; }}
                    .bg-danger {{ background: #ef4444; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <div>
                        <a href="/" style="text-decoration:none; color:#777">← Back to Portal</a>
                        <h1 style="margin:5px 0">{table_name} {source_badge} <span class="badge" style="background:#555">ICEBERG</span></h1>
                    </div>
                    <form action="/view/{table_name}" method="GET">
                        <input type="hidden" name="snapshot" value="{snapshot or ''}">
                        <input type="text" name="search" class="search-box" placeholder="Search across columns..." value="{search or ''}">
                        <button type="submit" style="padding:10px">Search</button>
                    </form>
                </div>

                {time_travel_banner}

                <div class="grid">
                    <div class="card">
                        <div style="display:flex; justify-content:space-between">
                            <h3>{'Historical View' if snapshot else 'Latest Records'}</h3>
                            {'<button class="btn-add" onclick="document.getElementById(\'add-form\').style.display=\'block\'">+ Insert Record</button>' if not snapshot else ''}
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
                        <h3>Time Travel</h3>
                        <p style="font-size:0.8em; color:#666">Click a snapshot to travel back in time.</p>
                        <ul>{snapshot_list}</ul>
                        <hr>
                        <p><small>Iceberg keeps an immutable record of every change.</small></p>
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
    if val is None or val == "":
        return None
        
    col_type = col_type.lower()
    try:
        if "int" in col_type:
            return int(val)
        elif "double" in col_type or "decimal" in col_type or "real" in col_type:
            return float(val)
        elif "boolean" in col_type:
            return val.lower() in ("true", "1", "yes", "on")
        elif "timestamp" in col_type or "date" in col_type:
            # Try parsing common ISO formats
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except:
                # Fallback to string if parsing fails, but most modern inputs are ISO
                return val
    except:
        pass
    return str(val)

@app.post("/update/{table_name}/{record_id}")
async def update_record(table_name: str, record_id: str, request: Request):
    """Handles manual record update in Trino with automatic type casting and robust quoting."""
    try:
        form_data = await request.form()
        conn = get_trino_conn()
        cur = conn.cursor()
        
        # Use quoted identifiers for absolute safety
        full_table_path = f'"{CATALOG}"."{SCHEMA}"."{table_name}"'
        
        cur.execute(f"DESCRIBE {full_table_path}")
        schema_info = {row[0]: row[1] for row in cur.fetchall()}
        id_col = list(schema_info.keys())[0]
        
        update_parts = []
        vals = []
        
        for col, val in form_data.items():
            if col == id_col: continue # Don't update the ID
            # IMPORTANT: Quote column names
            update_parts.append(f'"{col}" = ?')
            vals.append(await get_cast_val(schema_info[col], val))

        # Add the ID for the WHERE clause
        vals.append(await get_cast_val(schema_info[id_col], record_id))
        
        query = f'UPDATE {full_table_path} SET {", ".join(update_parts)} WHERE "{id_col}" = ?'
        safe_execute(cur, query, vals)
        
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        print(f"Mutation Failed: {e}")
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

@app.post("/insert/{table_name}")
async def insert_record(table_name: str, request: Request):
    """Handles manual record insertion into Trino with automatic type casting."""
    try:
        form_data = await request.form()
        conn = get_trino_conn()
        cur = conn.cursor()
        
        full_table_path = f'"{CATALOG}"."{SCHEMA}"."{table_name}"'
        
        # 1. Fetch column types from the table to handle strict Trino typing
        cur.execute(f"DESCRIBE {full_table_path}")
        schema_info = {row[0]: row[1] for row in cur.fetchall()} # {col_name: type_name}
        
        cols = []
        vals = []
        params = []
        
        for col, val in form_data.items():
            if col not in schema_info: continue
            # Quote the column name to handle reserved words or special chars
            cols.append(f'"{col}"')
            
            # 2. Cast string values from form to the correct Trino type
            vals.append(await get_cast_val(schema_info[col], val))
            params.append("?")

        cols_str = ", ".join(cols)
        params_str = ", ".join(params)
        
        # 3. Use parameterized query to prevent SQL injection and fix formatting
        query = f"INSERT INTO {full_table_path} ({cols_str}) VALUES ({params_str})"
        cur.execute(query, vals)
        
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        # Provide better error feedback
        print(f"Insert Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Insert failed: {str(e)}")

@app.post("/delete/{table_name}/{record_id}")
async def delete_record(table_name: str, record_id: str):
    """Handles record deletion with robust identifier quoting."""
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        
        full_table_path = f'"{CATALOG}"."{SCHEMA}"."{table_name}"'
        
        # Get the prime column (usually ID)
        cur.execute(f"DESCRIBE {full_table_path}")
        schema = cur.fetchall()
        id_col = schema[0][0]
        id_type = schema[0][1].lower()
        
        # Cast the ID correctly
        val = await get_cast_val(id_type, record_id)
            
        query = f'DELETE FROM {full_table_path} WHERE "{id_col}" = ?'
        safe_execute(cur, query, (val,))
        return RedirectResponse(url=f"/view/{table_name}", status_code=303)
    except Exception as e:
        print(f"Deletion Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
