"""
workers/pg_writer.py
──────────────────────────────────────────────────────────
Consumes messages from Kafka topic `social_raw`
and inserts them into Postgres table `social_raw`.
Works even if you launch the script from any folder.
"""

import json, os, psycopg2, logging
from pathlib import Path
from kafka import KafkaConsumer
from dotenv import load_dotenv

# ── 0. Load .env from project root ───────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent   # ~/iocl-intelligence
load_dotenv(BASE_DIR / ".env")                      # explicit path

DSN = os.getenv("PG_DSN")
if not DSN:
    raise RuntimeError("PG_DSN missing in .env")

# ── 1. Logging setup ─────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

# ── 2. Kafka consumer ────────────────────────────────────
consumer = KafkaConsumer(
    "social_raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda b: json.loads(b.decode()),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# ── 3. Postgres connection ───────────────────────────────
pg = psycopg2.connect(DSN)
pg.autocommit = False
cur = pg.cursor()

INSERT_SQL = """
INSERT INTO social_raw (id, platform, text, author, url, created_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING
"""

logging.info("🚚 pg_writer started; waiting for messages…")

try:
    batch = 0
    for msg in consumer:
      t = msg.value                 # ← the Kafka message (dict)

    # ▶ NEW — skip any malformed test messages
      if not all(k in t for k in ("id", "platform", "text",
                                "author", "url", "created_at")):
        print("⚠️  Skipping malformed message:", t)
        continue
    # ▲ NEW

      cur.execute("""
        INSERT INTO social_raw (id, platform, text, author, url, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (t["id"], t["platform"], t["text"],
          t["author"], t["url"], t["created_at"]))
      pg.commit()
      batch += 1
      if batch >= 1:                 # commit every 100 rows
            pg.commit()
            logging.info("✅ committed every 1 rows")
            batch = 0

except KeyboardInterrupt:
    logging.info("⏹  CTRL-C received; finishing up…")

finally:
    pg.commit()
    cur.close()
    pg.close()
    consumer.close()
    logging.info("pg_writer shut down cleanly")

