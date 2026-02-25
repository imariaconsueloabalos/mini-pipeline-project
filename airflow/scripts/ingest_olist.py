import os
import pandas as pd
from sqlalchemy import create_engine, text

# Config from environment
POSTGRES_USER = os.environ.get("POSTGRES_USER", "olist")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "olist")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "olist_postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "olist_dw")
RAW_SCHEMA = "raw"

DATA_DIR = "/opt/airflow/datasets/raw"  # <-- your raw folder inside container

DB_CONN = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Ensure raw schema exists
with DB_CONN.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))
    print(f"Schema '{RAW_SCHEMA}' ensured in Postgres.")

# Load CSVs
csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

for csv_file in csv_files:
    table_name = csv_file.replace(".csv", "").replace("olist_", "").lower()
    file_path = os.path.join(DATA_DIR, csv_file)

    print(f"Loading {file_path} → {RAW_SCHEMA}.{table_name} ...")
    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, DB_CONN, schema=RAW_SCHEMA, if_exists="replace", index=False)
        print(f"{table_name} loaded successfully! Rows: {len(df)}")
    except Exception as e:
        print(f"Failed to load {table_name}: {e}")

print("All CSVs loaded into PostgreSQL raw schema.")