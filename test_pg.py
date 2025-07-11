import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
dsn = os.getenv("PG_DSN")
print("Loaded DSN =", dsn)
psycopg2.connect(dsn).close()
print("✅ Connection works")

