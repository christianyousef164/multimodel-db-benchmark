import json
import os
from pathlib import Path

from dotenv import load_dotenv
from surrealdb import RecordID, Surreal

load_dotenv()

DATA = Path("data/processed/1000")

with Surreal("ws://127.0.0.1:8001") as db:
    # Connect
    db.signin(
        {
            "username": os.environ["SURREAL_USER"],
            "password": os.environ["SURREAL_PASS"],
        }
    )

    # Create and select the database
    db.query("DEFINE NAMESPACE IF NOT EXISTS gdelt;").first()
    db.use("gdelt", "gdelt_1000")
    db.query("DEFINE DATABASE IF NOT EXISTS gdelt_1000;").first()

    # Define tables
    db.query("DEFINE TABLE IF NOT EXISTS events;").first()
    db.query("DEFINE TABLE IF NOT EXISTS articles;").first()

    db.query("""
        DEFINE TABLE IF NOT EXISTS mentions
        TYPE RELATION IN articles OUT events ENFORCED;
    """).first()

    def load_table(name, key_field):
        records = []

        with open(DATA / f"{name}.jsonl", encoding="utf-8") as file:
            for line in file:
                record = json.loads(line)

                record["id"] = RecordID(name, record[key_field])

                if name == "mentions":
                    record["in"] = RecordID("articles", record["article_id"])
                    record["out"] = RecordID("events", record["event_id"])

                records.append(record)

        if name == "mentions":
            query = f"INSERT RELATION IGNORE INTO {name} $batch RETURN NONE;"
        else:
            query = f"INSERT IGNORE INTO {name} $batch RETURN NONE;"

        # Insert in batches
        for start in range(0, len(records), 1000):
            batch = records[start : start + 1000]
            db.query(query, {"batch": batch}).first()

        # Verify the number of records
        result = db.query(f"SELECT count() AS total FROM {name} GROUP ALL;").first()

        count = result[0]["total"]

        assert count == len(records), f"Count mismatch: {name}"

        print(f"{name}: {count} records imported")

    load_table("events", "event_id")
    load_table("articles", "article_id")
    load_table("mentions", "mention_id")

print("SurrealDB import completed!")
