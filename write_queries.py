import json
import os
import zipfile
from collections import deque
from datetime import datetime
from pathlib import Path

import psycopg
from arango import ArangoClient
from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from surrealdb import RecordID, Surreal

load_dotenv()

# 1. Select 100 existing events for W2
with open("data/processed/1000/events.jsonl") as file:
    existing = [json.loads(line) for line in file][:100]

update_ids = [e["event_id"] for e in existing]

# 2. Select 100 unused real events for W1
last_rows = deque(maxlen=100)
total = 0

for path in sorted(Path("data/raw/gdelt2").glob("*.export.CSV.zip")):
    with zipfile.ZipFile(path) as archive:
        name = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
        with archive.open(name) as file:
            for line in file:
                last_rows.append(line)
                total += 1

assert total >= 1000100

new_events = []

for line in last_rows:
    r = line.decode("utf-8").rstrip("\r\n").split("\t")

    new_events.append(
        {
            "event_id": int(r[0]),
            "event_date": datetime.strptime(r[1], "%Y%m%d").date().isoformat(),
            "date_added": datetime.strptime(r[59], "%Y%m%d%H%M%S").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "actor1_name": r[6] or None,
            "actor2_name": r[16] or None,
            "event_code": r[26],
            "event_root_code": r[28],
            "country_code": r[53] or None,
            "action_location": r[52] or None,
            "review_status": "unreviewed",
        }
    )

new_ids = [e["event_id"] for e in new_events]

assert len(set(new_ids)) == 100
assert len(set(update_ids)) == 100


# 3. Common verification
def states(rows):
    return {r["event_id"]: r["review_status"] for r in rows}


def test(name, read, insert, update, delete):
    print(f"\n===== {name} =====")

    original = {i: "unreviewed" for i in update_ids}

    assert states(read(new_ids)) == {}
    assert states(read(update_ids)) == original

    # W1: Insert, verify and remove new records
    try:
        insert()
        assert states(read(new_ids)) == {i: "unreviewed" for i in new_ids}
        print("W1 passed: 100 events inserted")
    finally:
        delete()

    assert states(read(new_ids)) == {}

    # W2: Update and restore original records
    try:
        update("reviewed")
        assert states(read(update_ids)) == {i: "reviewed" for i in update_ids}
        print("W2 passed: 100 events updated")
    finally:
        update("unreviewed")

    assert states(read(update_ids)) == original
    print("Original state restored")


# 4. ArangoDB
client = ArangoClient(hosts="http://127.0.0.1:8529")
db = client.db(
    "gdelt_1000", username="root", password=os.environ["ARANGO_ROOT_PASSWORD"]
)


def aql(query, **params):
    return list(db.aql.execute(query, bind_vars=params))


test(
    "ArangoDB",
    lambda ids: aql(
        """
        FOR k IN @keys
            LET e = DOCUMENT(CONCAT("events/", k))
            FILTER e != null
            RETURN {
                event_id: e.event_id,
                review_status: e.review_status
            }
    """,
        keys=[str(i) for i in ids],
    ),
    lambda: aql(
        """
        FOR e IN @rows
            INSERT e INTO events
    """,
        rows=[dict(e, _key=str(e["event_id"])) for e in new_events],
    ),
    lambda status: aql(
        """
        FOR k IN @keys
            UPDATE k WITH {review_status: @status}
            IN events
    """,
        keys=[str(i) for i in update_ids],
        status=status,
    ),
    lambda: aql(
        """
        FOR k IN @keys
            REMOVE k IN events
            OPTIONS {ignoreErrors: true}
    """,
        keys=[str(i) for i in new_ids],
    ),
)

client.close()


# 5. SurrealDB
with Surreal("ws://127.0.0.1:8001") as db:
    db.signin(
        {"username": os.environ["SURREAL_USER"], "password": os.environ["SURREAL_PASS"]}
    )

    db.use("gdelt", "gdelt_1000")

    def surreal(query, **params):
        return db.query(query, params).first()

    def record_ids(ids):
        return [RecordID("events", i) for i in ids]

    test(
        "SurrealDB",
        lambda ids: surreal(
            """
            SELECT event_id, review_status FROM events
            WHERE id IN $ids;
        """,
            ids=record_ids(ids),
        ),
        lambda: surreal(
            """
            INSERT INTO events $rows RETURN NONE;
        """,
            rows=[dict(e, id=RecordID("events", e["event_id"])) for e in new_events],
        ),
        lambda status: surreal(
            """
            UPDATE events SET review_status = $status
            WHERE id IN $ids RETURN NONE;
        """,
            status=status,
            ids=record_ids(update_ids),
        ),
        lambda: surreal(
            """
            DELETE events WHERE id IN $ids RETURN NONE;
        """,
            ids=record_ids(new_ids),
        ),
    )


# 6. PostgreSQL
with psycopg.connect(
    host="127.0.0.1",
    port=5433,
    dbname="multimodel",
    user="benchmark",
    password=os.environ["POSTGRES_PASSWORD"],
    row_factory=dict_row,
    autocommit=True,
) as conn:
    conn.execute("SET search_path TO gdelt_1000")

    test(
        "PostgreSQL",
        lambda ids: conn.execute(
            """
            SELECT event_id, review_status FROM events
            WHERE event_id = ANY(%s)
        """,
            (ids,),
        ).fetchall(),
        lambda: conn.execute(
            """
            INSERT INTO events
            SELECT * FROM
            jsonb_populate_recordset(NULL::events, %s)
        """,
            (Jsonb(new_events),),
        ),
        lambda status: conn.execute(
            """
            UPDATE events SET review_status = %s
            WHERE event_id = ANY(%s)
        """,
            (status, update_ids),
        ),
        lambda: conn.execute(
            """
            DELETE FROM events
            WHERE event_id = ANY(%s)
        """,
            (new_ids,),
        ),
    )

print("\nAll write tests passed!")
