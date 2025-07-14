from fastapi import FastAPI
import psycopg2
from typing import List
from pydantic import BaseModel
import os

app = FastAPI()


conn = psycopg2.connect(os.getenv("postgresql://iocl_db_user:Jvjs0ygcosfDOnmOLdIM6Ow1kfHBVWuD@dpg-d1qlbafdiees73f3iq8g-a.oregon-postgres.render.com/iocl_db"))

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
