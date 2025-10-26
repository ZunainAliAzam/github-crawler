import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS repositories (
    repo_id BIGINT PRIMARY KEY,
    name TEXT,
    owner TEXT,
    stars_count INT,
    last_updated TIMESTAMP DEFAULT NOW()
);
""")
conn.commit()
cur.close()
conn.close()
print("✅ Database setup complete.")
