
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from arango import ArangoClient

load_dotenv()

DATA = Path("data/processed/1000")
PASSWORD = os.environ["ARANGO_ROOT_PASSWORD"]

# Connect to ArangoDB
client = ArangoClient(hosts="http://127.0.0.1:8529")

system = client.db(
    "_system", username="root", password=PASSWORD
)

# Create a database for the 1K sample
if not system.has_database("gdelt_1000"):
    system.create_database("gdelt_1000")

db = client.db(
    "gdelt_1000", username="root", password=PASSWORD
)

# Create collections
for name in ["events", "articles", "mentions"]:
    if not db.has_collection(name):
        db.create_collection(
            name, edge=(name == "mentions")
        )


def load_collection(name, key_field):
    records = []

    with open(DATA / f"{name}.jsonl", encoding="utf-8") as file:
        for line in file:
            record = json.loads(line)
            record["_key"] = str(record[key_field])

            # Convert mentions into graph edges
            if name == "mentions":
                record["_from"] = f"articles/{record['article_id']}"
                record["_to"] = f"events/{record['event_id']}"

            records.append(record)

    collection = db.collection(name)

    # Import in batches of 1,000
    for start in range(0, len(records), 1000):
        batch = records[start:start + 1000]

        result = collection.import_bulk(
            batch, on_duplicate="replace"
        )

        if result["errors"] != 0:
            raise RuntimeError(result)

    count = collection.count()

    assert count == len(records), f"Count mismatch: {name}"
    print(f"{name}: {count} records imported")


load_collection("events", "event_id")
load_collection("articles", "article_id")
load_collection("mentions", "mention_id")

client.close()

print("ArangoDB import completed!")
