from fastapi import FastAPI
import psycopg2
from typing import List
from pydantic import BaseModel
import os

app = FastAPI()

DATABASE_URL = "postgresql://iocl_db_user:Jvjs0ygcosfDOnmOLdIM6Ow1kfHBVWuD@dpg-d1qlbafdiees73f3iq8g-a.oregon-postgres.render.com/iocl_db?sslmode=require"
conn = psycopg2.connect(dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    sslmode="require"))

class TweetData(BaseModel):
    id: int
    content: str
    sentiment: str
@app.get("/tweets", response_model=List[TweetData])
def get_tweets():
    cur = conn.cursor()
    cur.execute("SELECT id, content, sentiment FROM social_nlp LIMIT 10;")
    rows = cur.fetchall()
    cur.close()
    return [{"id": r[0], "content": r[1], "sentiment": r[2]} for r in rows]
