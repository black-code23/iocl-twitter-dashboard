from fastapi import FastAPI
import psycopg2
from typing import List
from pydantic import BaseModel

# Step 1: Create the FastAPI app
app = FastAPI()

# Step 2: DB connection (update these values)
conn = psycopg2.connect(
    host="localhost",
    database="iocl",
    user="postgres",
    password="postgres"
)

# Step 3: Define the response model
class TweetData(BaseModel):
    id: int
    content: str
    sentiment: str

# Step 4: API endpoint to get tweets
@app.get("/tweets", response_model=List[TweetData])
def get_tweets():
    cur = conn.cursor()
    cur.execute("SELECT id, content, sentiment FROM social_nlp LIMIT 10;")
    rows = cur.fetchall()
    cur.close()
    return [{"id": r[0], "content": r[1], "sentiment": r[2]} for r in rows]
