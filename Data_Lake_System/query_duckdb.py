import duckdb
import argparse
import sys

def query_duckdb(table_path):
    print(f"Connecting to MinIO via DuckDB to query: {table_path}")
    
    con = duckdb.connect()
    
    # Install and load required extensions
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL iceberg;")
    con.execute("LOAD iceberg;")
    
    # Configure MinIO credentials
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_endpoint='127.0.0.1:9000';")
    con.execute("SET s3_access_key_id='minioadmin';")
    con.execute("SET s3_secret_access_key='minioadmin';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    
    try:
        # Construct Iceberg scan query
        # DuckDB Iceberg extension usually scans the metadata file or the folder if supported
        # We assume table_path is like 's3://warehouse/default/leads'
        
        query = f"SELECT * FROM iceberg_scan('{table_path}') LIMIT 20;"
        print(f"Executing: {query}")
        
        result = con.execute(query).fetchdf()
        
        print("\nQuery Result:")
        print(result)
        print(f"\nTotal Rows: {len(result)}")

    except Exception as e:
        print(f"ERROR: Failed to query Iceberg table: {e}")
        # Fallback hint
        print("Hint: Ensure the path points to the folder containing the 'metadata' directory")
        print("Example: s3://warehouse/default/leads")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query Iceberg Table with DuckDB")
    parser.add_argument("--table", required=True, help="Full S3 path to Iceberg table (e.g. s3://warehouse/default/leads)")
    args = parser.parse_args()
    
    query_duckdb(args.table)
