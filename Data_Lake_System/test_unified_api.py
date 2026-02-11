import requests
import argparse
import sys
import os
import io

def test_ingest_iceberg(api_url, csv_path):
    print(f"\n--- Testing Iceberg Ingestion for {csv_path} ---")
    file_name = os.path.basename(csv_path)
    table_name = "default.test_iceberg_unified"
    
    with open(csv_path, 'rb') as f:
        files = {'file': (file_name, f, 'text/csv')}
        data = {
            'table': table_name,
            'format': 'iceberg',
            'mode': 'append'
        }
        try:
            response = requests.post(f"{api_url}/ingest", files=files, data=data)
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error: {e}")

def test_ingest_hudi(api_url, csv_path):
    print(f"\n--- Testing Hudi Ingestion for {csv_path} ---")
    file_name = os.path.basename(csv_path)
    table_name = "test_hudi_unified" # Hudi usually synced to 'default' db but table name used directly in args often
    
    with open(csv_path, 'rb') as f:
        files = {'file': (file_name, f, 'text/csv')}
        data = {
            'table': table_name,
            'format': 'hudi',
            'pkey': 'id', # Assuming sample data has 'id'
            'partition': 'department' # Assuming sample data has 'department'
        }
        try:
            response = requests.post(f"{api_url}/ingest", files=files, data=data)
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error: {e}")

def test_query(api_url, sql, catalog='iceberg'):
    print(f"\n--- Testing Query [{catalog}] ---")
    print(f"SQL: {sql}")
    data = {'query': sql, 'catalog': catalog}
    try:
        response = requests.post(f"{api_url}/query", data=data)
        print(f"Status: {response.status_code}")
        try:
            print(f"Rows returned: {len(response.json())}")
            print(f"Sample: {response.json()[:1]}")
        except:
             print(f"Response Text: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Unified Ingest API")
    parser.add_argument("--url", default="http://localhost:8000", help="API Base URL")
    parser.add_argument("--file", help="Path to CSV file to test with")
    args = parser.parse_args()
    
    # Create dummy csv if not provided
    if not args.file:
        dummy_csv = "test_data.csv"
        with open(dummy_csv, "w") as f:
            f.write("id,name,department,salary\n")
            f.write("1,John Doe,Engineering,1000\n")
            f.write("2,Jane Smith,HR,1200\n")
            f.write("3,Bob Jones,Engineering,1100\n")
        args.file = dummy_csv
        print(f"Created temporary file: {dummy_csv}")

    test_ingest_iceberg(args.url, args.file)
    test_ingest_hudi(args.url, args.file)
    
    # Wait a bit or Just query? Ingestion is async/subprocess, might take a moment.
    # But scripts wait for subprocess.
    
    test_query(args.url, "SELECT * FROM default.test_iceberg_unified", "iceberg")
    test_query(args.url, "SELECT * FROM default.test_hudi_unified", "hudi")
