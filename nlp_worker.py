from kafka import KafkaConsumer
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import json
import os
import psycopg2

# Load the sentiment model
model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Setup Kafka consumer
consumer = KafkaConsumer(
    'sentiment-input',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='sentiment-group'
)

# Setup PostgreSQL connection
pg_dsn = os.environ.get("PG_DSN")
conn = psycopg2.connect(pg_dsn)
cursor = conn.cursor()

print("🔁 Started NLP Worker...")

for message in consumer:
    data = message.value
    text = data.get("text", "")

    print(f"📝 Received: {text}")

    # Sentiment prediction
    inputs = tokenizer(text, return_tensors="pt", truncation=True)
    outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=1)
    sentiment = torch.argmax(probs).item()

    # Save to DB or print
    print(f"✅ Sentiment = {sentiment}, probs = {probs.tolist()}")

    # Optionally save to DB (if you have a table ready)
    # cursor.execute("INSERT INTO sentiments (text, sentiment) VALUES (%s, %s)", (text, sentiment))
    # conn.commit()
