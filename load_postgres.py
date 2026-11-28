
import os
import json
import psycopg
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA = Path("data/processed/1000")

with psycopg.connect(
    host="127.0.0.1",
    port=5433,
    dbname="multimodel",
    user="benchmark",
    password=os.environ["POSTGRES_PASSWORD"],
) as conn:

    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS gdelt_1000")
    cur.execute("SET search_path TO gdelt_1000")

    # Create events table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id BIGINT PRIMARY KEY,
            event_date DATE,
            date_added TIMESTAMPTZ,
            actor1_name TEXT,
            actor2_name TEXT,
            event_code TEXT,
            event_root_code TEXT,
            country_code TEXT,
            action_location TEXT,
            review_status TEXT
        )
    """)

    # Create articles table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            article_id TEXT PRIMARY KEY,
            url TEXT,
            mention_type INTEGER
        )
    """)

    # Create mentions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mentions (
            mention_id TEXT PRIMARY KEY,
            article_id TEXT REFERENCES articles(article_id),
            event_id BIGINT REFERENCES events(event_id),
            source TEXT,
            event_first_seen TIMESTAMPTZ,
            mentioned_at TIMESTAMPTZ,
            confidence INTEGER
        )
    """)

    def load_table(name):
        with open(DATA / f"{name}.jsonl", encoding="utf-8") as file:
            records = [json.loads(line) for line in file]

        columns = list(records[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))

        query = f"""
            INSERT INTO {name} ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """

        values = [
            tuple(record[column] for column in columns)
            for record in records
        ]

        cur.executemany(query, values)

        cur.execute(f"SELECT COUNT(*) FROM {name}")
        count = cur.fetchone()[0]

        assert count == len(records), f"Count mismatch: {name}"

        print(f"{name}: {count} records imported")

    load_table("events")
    load_table("articles")
    load_table("mentions")

print("PostgreSQL import completed!")
