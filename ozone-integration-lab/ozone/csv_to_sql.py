import pandas as pd
import sqlalchemy
import sys
import os

def csv_to_sql(csv_path, db_type="mysql", table_name="customers"):
    """
    Ingests a CSV file into a SQL database (MySQL or Postgres)
    which then triggers CDC to the Data Lake.
    """
    print(f"🚀 Starting SQL-based ingestion for {csv_path}...")
    
    if not os.path.exists(csv_path):
        print(f"❌ Error: File {csv_path} not found.")
        return

    # Database connection parameters (standard for this setup)
    if db_type == "mysql":
        db_url = "mysql+pymysql://user:password@localhost:15431/inventory"
    elif db_type == "postgres":
        db_url = "postgresql://user:password@localhost:15432/inventory"
    else:
        print("❌ Error: Unsupported db_type. Use 'mysql' or 'postgres'.")
        return

    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"📊 Read {len(df)} rows from CSV.")

        # Connect and Insert
        engine = sqlalchemy.create_engine(db_url)
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        
        print(f"✅ Successfully inserted data into {db_type} table '{table_name}'.")
        print(f"✨ CDC (Debezium) will now pick up these changes and move them to Kafka/Ozone.")

    except Exception as e:
        print(f"❌ Error during ingestion: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python csv_to_sql.py <csv_path> [mysql|postgres] [table_name]")
    else:
        path = sys.argv[1]
        db = sys.argv[2] if len(sys.argv) > 2 else "mysql"
        table = sys.argv[3] if len(sys.argv) > 3 else "customers"
        csv_to_sql(path, db, table)
