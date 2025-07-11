
import os, json, logging, time
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg2
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")             
DSN = os.getenv("PG_DSN")                   

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)


MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment-latest"
logging.info("📦 Loading model %s … (one‑time download ~180 MB)", MODEL_ID)
tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)
model.eval()                 # inference mode (no gradients)
LABELS = ["negative", "neutral", "positive"]


consumer = KafkaConsumer(
    "social_raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda b: json.loads(b.decode()),
    group_id="nlp_worker",
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

pg  = psycopg2.connect(DSN)
cur = pg.cursor()
INSERT_SQL = """
INSERT INTO social_nlp (id, sentiment, sent_score)
VALUES (%s, %s, %s)
ON CONFLICT (id) DO UPDATE
SET  sentiment   = EXCLUDED.sentiment,
     sent_score  = EXCLUDED.sent_score,
     processed_at= now();
"""

def infer(text: str) -> tuple[str, float]:
    """Return (label, confidence) for tweet text."""
    inputs = tokenizer(text, truncation=True, return_tensors="pt")
    with torch.no_grad():
        logits = model(**inputs).logits
        probs  = torch.softmax(logits, dim=1)[0]
    lid  = int(torch.argmax(probs))
    conf = float(probs[lid])
    return LABELS[lid], conf


logging.info("🧠 nlp_worker started (RoBERTa sentiment)…")
while True:
    for msg in consumer:
        t = msg.value
        if not all(k in t for k in ("id", "text")):              # guard malformed
            logging.warning("Skipping bad message: %s", t)
            continue
        try:
            label, score = infer(t["text"])
            cur.execute(INSERT_SQL, (t["id"], label, score))
            pg.commit()
            logging.info("✓ %s → %-8s %.2f", t["id"], label, score)
        except Exception as e:
            logging.error("❌ NLP failed on %s: %s", t["id"], e)

    time.sleep(1)   

