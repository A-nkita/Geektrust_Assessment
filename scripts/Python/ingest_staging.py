import psycopg2
import logging
import os

# ---------- log folder ----------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- db config ----------
DB_CONFIG = {
    "host": "localhost",
    "dbname": "geektrust_db",
    "user": "postgres",
    "password": "root",
    "port": 5432
}

FILES = {
    "staging.users": "C:/Users/Admin/OneDrive/Desktop/Geektrust/Data/users.csv",
    "staging.onboarding": "C:/Users/Admin/OneDrive/Desktop/Geektrust/Data/onboarding.csv",
    "staging.transactions": "C:/Users/Admin/OneDrive/Desktop/Geektrust/Data/transactions.csv"
}

def ingest(table, file_path, conn):
    with open(file_path, "r", encoding="utf-8") as f:
        cur = conn.cursor()
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)
        conn.commit()
        cur.close()
        logging.info(f"Ingested {table}")

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        for table, path in FILES.items():
            ingest(table, path, conn)
        conn.close()
        logging.info("Daily ingestion completed")
    except Exception as e:
        logging.error(f"Ingestion failed: {e}")

if __name__ == "__main__":
    main()
