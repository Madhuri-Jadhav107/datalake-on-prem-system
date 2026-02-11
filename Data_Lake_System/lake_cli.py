import argparse
import requests
import os
import json
import sys

API_URL = "http://localhost:8000"

def ingest(args):
    if not os.path.exists(args.file):
        print(f"❌ Error: File '{args.file}' not found.")
        return

    print(f"🚀 Ingesting '{args.file}' into table '{args.table}'...")
    
    files = {'file': open(args.file, 'rb')}
    data = {'table': args.table}
    
    
    endpoint = f"{API_URL}/ingest/hudi" if args.format == "hudi" else f"{API_URL}/ingest/iceberg/spark"
    
    try:
        if args.format == "hudi":
            # Hudi requires pkey logic
            # Client-side Auto-detection of Primary Key
            if args.pkey:
                data['pkey'] = args.pkey
            else:
                try:
                    # Read first line to get header
                    with open(args.file, 'r', encoding='utf-8', errors='ignore') as f:
                        header = f.readline().strip()
                        # Basic CSV split (handles simple cases)
                        cols = [c.strip().strip('"') for c in header.split(',')]
                        
                        # Heuristic: verify we have columns
                        if cols:
                            # Look for standard ID names
                            candidates = [c for c in cols if c.lower() in ['id', 'uuid', 'guid', 'key', 'pk', 'emp_id', 'user_id', 'transaction_id']]
                            # Check for _id suffix
                            if not candidates:
                                candidates = [c for c in cols if c.lower().endswith('_id')]
                            
                            # Selection
                            if candidates:
                                detected_pkey = candidates[0]
                            else:
                                detected_pkey = cols[0]
                                
                            print(f"ℹ️  Auto-detected Primary Key: '{detected_pkey}'")
                            data['pkey'] = detected_pkey
                except Exception as e:
                    print(f"⚠️  PKey Auto-detection warning: {e}")
        
        print(f"🚀 Sending request to {endpoint}...")
        resp = requests.post(endpoint, files=files, data=data)
        
        if resp.status_code == 200:
            print("✅ Success!")
            print(json.dumps(resp.json(), indent=2))
        else:
            print(f"❌ Failed: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def read(args):
    print(f"🔍 Reading from table '{args.table}'...")
    
    params = {}
    if args.columns:
        params['columns'] = args.columns
    if args.limit:
        params['limit'] = args.limit
        
    # Support "col=val" filtering
    if args.where:
        try:
            if '=' in args.where:
                col, val = args.where.split('=', 1)
                params['filter_col'] = col.strip()
                params['filter_val'] = val.strip()
            else:
                print("⚠️  Warning: --where argument must be in 'col=val' format. Ignoring.")
        except Exception as e:
            print(f"⚠️  Error parsing filter: {e}")
        
    
    # Pass format as catalog
    params['catalog'] = args.format

    try:
        # Use new generic endpoint
        resp = requests.get(f"{API_URL}/data/{args.table}/read", params=params)
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ Found {len(data)} rows:")
            print(json.dumps(data, indent=2))
        else:
            print(f"❌ Failed: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def delete(args):
    print(f"🗑 Deleting IDs [{args.ids}] from '{args.table}'...")
    
    data = {
        'table': args.table,
        'ids': args.ids,
        'pkey': args.pkey 
    }
    
    try:
        resp = requests.post(f"{API_URL}/delete/hudi", data=data)
        if resp.status_code == 200:
            print("✅ Success!")
            print(json.dumps(resp.json(), indent=2))
        else:
            print(f"❌ Failed: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Data Lake CLI Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # INGEST Command
    p_ingest = subparsers.add_parser("ingest", help="Upload CSV to Data Lake")
    p_ingest.add_argument("file", help="Path to CSV file")
    p_ingest.add_argument("table", help="Target Table Name")
    p_ingest.add_argument("--pkey", help="Primary Key (Optional, auto-detected if omitted)")
    p_ingest.add_argument("--format", default="hudi", choices=["hudi", "iceberg"], help="Table format (hudi or iceberg)")

    # READ Command
    p_read = subparsers.add_parser("read", help="Read data from Data Lake")
    p_read.add_argument("table", help="Table Name")
    p_read.add_argument("--columns", help="Columns to select (comma-separated)", default="*")
    p_read.add_argument("--limit", help="Max rows to return", default=100)
    p_read.add_argument("--where", help="Filter condition (e.g. 'col=val')")
    p_read.add_argument("--format", default="hudi", choices=["hudi", "iceberg"], help="Table format (hudi or iceberg)")

    # DELETE Command
    p_delete = subparsers.add_parser("delete", help="Delete records")
    p_delete.add_argument("table", help="Table Name")
    p_delete.add_argument("ids", help="Comma-separated IDs to delete")
    p_delete.add_argument("pkey", help="Primary Key column name")

    args = parser.parse_args()

    if args.command == "ingest":
        ingest(args)
    elif args.command == "read":
        read(args)
    elif args.command == "delete":
        delete(args)

if __name__ == "__main__":
    main()
