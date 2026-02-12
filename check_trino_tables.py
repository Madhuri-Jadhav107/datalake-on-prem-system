import trino
import os

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "18082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")

def get_trino_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="iceberg",
        schema="trino_db",
    )

def check_tables():
    try:
        conn = get_trino_conn()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        print("--- Existing Tables in iceberg.trino_db ---")
        for t in tables:
            print(t[0])
    except Exception as e:
        print(f"Error connecting to Trino: {e}")

if __name__ == "__main__":
    check_tables()
