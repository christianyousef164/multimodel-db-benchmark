
import os
import json
import psycopg
from collections import Counter
from dotenv import load_dotenv
from arango import ArangoClient
from surrealdb import Surreal
from psycopg.rows import dict_row

load_dotenv()

# Read the original prepared data
with open("data/processed/1000/events.jsonl", encoding="utf-8") as file:
    events = [json.loads(line) for line in file]

assert len(events) == 1000

# Calculate expected answers independently
expected = [
    [
        {
            "event_id": e["event_id"],
            "event_code": e["event_code"]
        }
        for e in events
        if e["event_id"] == 1332291273
    ],
    sorted(
        [
            {"event_id": e["event_id"]}
            for e in events
            if e["country_code"] == "US"
            and e["event_root_code"] == "04"
            and "2026-01-01" <= e["event_date"] < "2027-01-01"
        ],
        key=lambda e: e["event_id"]
    ),
    [
        {"event_root_code": category, "total": count}
        for category, count in sorted(
            Counter(e["event_root_code"] for e in events).items()
        )
    ]
]

# ArangoDB queries
aql = [
    """
    FOR e IN events
        FILTER e._key == "1332291273"
        RETURN {event_id: e.event_id, event_code: e.event_code}
    """,
    """
    FOR e IN events
        FILTER e.country_code == "US"
        FILTER e.event_root_code == "04"
        FILTER e.event_date >= "2026-01-01"
        FILTER e.event_date < "2027-01-01"
        SORT e.event_id
        RETURN {event_id: e.event_id}
    """,
    """
    FOR e IN events
        COLLECT category = e.event_root_code WITH COUNT INTO total
        SORT category
        RETURN {event_root_code: category, total: total}
    """
]

# SurrealDB queries
surrealql = [
    """
    SELECT event_id, event_code FROM events:1332291273;
    """,
    """
    SELECT event_id FROM events
    WHERE country_code = 'US'
      AND event_root_code = '04'
      AND event_date >= '2026-01-01'
      AND event_date < '2027-01-01'
    ORDER BY event_id;
    """,
    """
    SELECT event_root_code, count() AS total
    FROM events
    GROUP BY event_root_code
    ORDER BY event_root_code;
    """
]

# PostgreSQL queries
sql = [
    """
    SELECT event_id, event_code FROM events
    WHERE event_id = 1332291273;
    """,
    """
    SELECT event_id FROM events
    WHERE country_code = 'US'
      AND event_root_code = '04'
      AND event_date >= '2026-01-01'
      AND event_date < '2027-01-01'
    ORDER BY event_id;
    """,
    """
    SELECT event_root_code, COUNT(*) AS total
    FROM events
    GROUP BY event_root_code
    ORDER BY event_root_code;
    """
]


def verify(name, queries, execute):
    print(f"\n===== {name} =====")

    for i, query in enumerate(queries):
        result = execute(query)

        assert result == expected[i], (
            f"{name}: Q{i + 1} failed."
        )

        print(f"Q{i + 1} passed: {len(result)} results")
        print(result[:3])


# Test ArangoDB
client = ArangoClient(hosts="http://127.0.0.1:8529")

db = client.db(
    "gdelt_1000",
    username="root",
    password=os.environ["ARANGO_ROOT_PASSWORD"]
)

verify("ArangoDB", aql, lambda q: list(db.aql.execute(q)))
client.close()


# Test SurrealDB
with Surreal("ws://127.0.0.1:8001") as db:
    db.signin({
        "username": os.environ["SURREAL_USER"],
        "password": os.environ["SURREAL_PASS"]
    })

    db.use("gdelt", "gdelt_1000")

    verify("SurrealDB", surrealql, lambda q: db.query(q).first())


# Test PostgreSQL
with psycopg.connect(
    host="127.0.0.1",
    port=5433,
    dbname="multimodel",
    user="benchmark",
    password=os.environ["POSTGRES_PASSWORD"],
    row_factory=dict_row
) as conn:

    conn.execute("SET search_path TO gdelt_1000")

    verify("PostgreSQL", sql, lambda q: conn.execute(q).fetchall())

print("\nAll fundamental queries passed!")

if not expected[1]:
    print("Note: Q2 has no matches. Choose another filter for benchmarking.")
