
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import psycopg2
from textblob import TextBlob
import tweepy
import os
from datetime import datetime

app = FastAPI()

# Load Twitter API credentials from environment variables (Render)
TWITTER_BEARER_TOKEN = os.environ.get("X_BEARER")

# Set up Twitter client
client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)

# Connect to Render PostgreSQL
conn = psycopg2.connect(
    dbname="iocl_db",
    user="iocl_db_user",
    password="Jvjs0ygcosfDOnmOLdIM6Ow1kfHBVWuD",  # NOTE: Move this to an env var in real use
    host="dpg-d1qlbafdiees73f3iq8g-a.oregon-postgres.render.com",
    port="5432",
    sslmode="require"
)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS tweet_sentiments (
        id SERIAL PRIMARY KEY,
        tweet TEXT,
        sentiment TEXT,
        polarity FLOAT,
        created_at TIMESTAMP
    )
""")
conn.commit()


@app.get("/", response_class=HTMLResponse)
def read_root():
    return """
    <html>
        <head><title>IOCL Twitter Sentiment</title></head>
        <body style="font-family: sans-serif;">
            <h2>🛢️ IOCL Twitter Sentiment Dashboard</h2>
            <p>This is a live app that fetches tweets related to Indian Oil (IOCL), analyzes the sentiment using AI, and stores it in a database.</p>
            <h4>Try These:</h4>
            <ul>
                <li><a href="/tweetsentiment?query=IOCL&max_results=5">Analyze 5 tweets about IOCL</a></li>
                <li><a href="/all_sentiments_html">See all saved tweet sentiments</a></li>
            </ul>
            <p>Built using FastAPI, PostgreSQL, and Render 🚀</p>
        </body>
    </html>
    """


@app.get("/tweetsentiment")
def get_tweets_sentiment(query: str = Query(...), max_results: int = 10):
    try:
        tweets = client.search_recent_tweets(query=query, max_results=max_results, tweet_fields=["text"])
    except Exception as e:
        return {"error": str(e)}

    if not tweets.data:
        return {"message": "No tweets found for this query. Try another keyword."}

    results = []

    for tweet in tweets.data:
        text = tweet.text
        polarity = TextBlob(text).sentiment.polarity
        sentiment = "Positive" if polarity > 0 else "Negative" if polarity < 0 else "Neutral"

        cursor.execute(
            "INSERT INTO tweet_sentiments (tweet, sentiment, polarity, created_at) VALUES (%s, %s, %s, %s)",
            (text, sentiment, polarity, datetime.utcnow())
        )
        conn.commit()

        results.append({
            "tweet": text,
            "sentiment": sentiment,
            "polarity": polarity
        })

    return results


@app.get("/all_sentiments_html", response_class=HTMLResponse)
def view_sentiments():
    cursor.execute("SELECT tweet, sentiment, polarity, created_at FROM tweet_sentiments ORDER BY created_at DESC LIMIT 50")
    rows = cursor.fetchall()

    if not rows:
        return "<h3>No data available yet. Please analyze some tweets first.</h3>"

    table_rows = ""
    for tweet, sentiment, polarity, created_at in rows:
        table_rows += f"<tr><td>{tweet}</td><td>{sentiment}</td><td>{polarity:.2f}</td><td>{created_at}</td></tr>"

    return f"""
    <html>
        <head><title>Sentiments</title></head>
        <body style="font-family:sans-serif;">
            <h2>🧠 Stored Tweet Sentiments</h2>
            <table border="1" cellpadding="5">
                <tr><th>Tweet</th><th>Sentiment</th><th>Polarity</th><th>Time</th></tr>
                {table_rows}
            </table>
            <br><a href="/">← Back to home</a>
        </body>
    </html>
    """

