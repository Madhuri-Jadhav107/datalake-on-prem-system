import duckdb

def get_duckdb_connection():
    """
    Establishes a DuckDB connection and configures it for MinIO and Iceberg.
    """
    print("Connecting to DuckDB...")
    con = duckdb.connect(database=':memory:')
    
    # Install and load required extensions
    extensions = ['httpfs', 'iceberg']
    for ext in extensions:
        try:
            # print(f"Installing/Loading extension: {ext}")
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
        except Exception as e:
            print(f"[WARN] Failed to load extension '{ext}': {e}")
    
    # Configure S3/MinIO Settings
    con.execute("SET s3_endpoint='localhost:9000';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_region='us-east-1';")
    
    # Critical: Set URL style for MinIO
    con.execute("SET s3_url_style='path';")
    
    return con
