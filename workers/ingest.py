import os,time,json,logging
from dotenv import load_dotenv
from kafka import KafkaProducer
import tweepy

load_dotenv()

producer = KafkaProducer(
		bootstrap_servers="localhost:9092",
		value_serializer=lambda m: json.dumps(m).encode()
)
client = tweepy.Client(bearer_token=os.getenv("X_BEARER"))

QUERY = (
    '( "IndianOil" OR "Indian Oil" OR IOCL OR @IndianOilcl '
    'OR Indane OR Servo OR XTRAPREMIUM '
    'OR "#IndianOil" OR "#IOCL" OR "#FuelingNation" OR "#GoGreenWithIOC" ) '
    'lang:en -is:retweet'
)
def fetch_and_publish(keyword: str, since_id: str | None) -> str | None:
    """
    Pull up to 100 tweets newer than since_id and push to Kafka.
    Returns newest_id so the caller can keep state.
    """
    resp = client.search_recent_tweets(
        query=keyword,
        since_id=since_id,
        tweet_fields=["created_at", "author_id", "lang"],
        max_results=100,
    )

    new_id = since_id
    for tw in resp.data or []:
        tweet_doc = {
            "id": str(tw.id),
            "platform": "twitter",
            "text": tw.text,
            "author": str(tw.author_id),
            "url": f"https://x.com/i/web/status/{tw.id}",
            "created_at": tw.created_at.isoformat(),
        }
        producer.send("social_raw", tweet_doc)
        new_id = str(tw.id)          
    producer.flush()

    if tws := len(resp.data or []):
        logging.info(f"Published {tws} tweets (since_id={since_id})")

    return new_id


if __name__ == "__main__":
    logging.info("🔄 IOCL ingest worker started …")
    last_seen: str | None = None        

    try:
        while True:
            last_seen = fetch_and_publish(QUERY, last_seen)
            time.sleep(1800)              
    except KeyboardInterrupt:
        logging.info("Graceful shutdown requested …")
    finally:
        producer.flush()
        producer.close()
