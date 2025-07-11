# workers/dummy_ingest.py
import json, uuid, datetime as dt, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda m: json.dumps(m).encode()
)

DUMMY_TWEETS = [
    "IndianOil launches new green initiative!",
    "Visited IOCL pump, great experience!",
    "Facing issues at Indane LPG connection.",
    "Loving the performance of Servo lubricants.",
    "IOCL customer care helped me today."
]

for _ in range(5):
    tweet = {
        "id": str(uuid.uuid4()),
        "platform": "twitter",
        "text": random.choice(DUMMY_TWEETS),
        "author": "dummy_user",
        "url": "https://x.com/i/web/status/" + str(uuid.uuid4())[:8],
        "created_at": dt.datetime.utcnow().isoformat()
    }
    producer.send("social_raw", tweet)
    print("✅ Sent:", tweet["text"])

producer.flush()
print("✅ All dummy tweets sent.")
