CREATE TABLE IF NOT EXISTS social_raw (
 id               TEXT PRIMARY KEY,
 platform         TEXT,
 text             TEXT,
 author           TEXT,
 url              TEXT,
 created_at       TIMESTAMPTZ,
 inserted_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS social_nlp (
 id               TEXT PRIMARY KEY
                  REFERENCES social_raw(id)
                  ON DELETE CASCADE,
 sentiment        TEXT,
 sent_score       REAL,
 processed_at     TIMESTAMPTZ DEFAULT now()
);
