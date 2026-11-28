
import os
import json
import psycopg
from collections import defaultdict
from dotenv import load_dotenv
from arango import ArangoClient
from surrealdb import Surreal, RecordID
from psycopg.rows import dict_row

load_dotenv()

DATA = "data/processed/1000"
COUNTRY = "US"


def read_data(name):
    with open(f"{DATA}/{name}.jsonl", encoding="utf-8") as file:
        return [json.loads(line) for line in file]


# 1. Find a suitable event using the original data

countries = {
    e["event_id"]: e["country_code"]
    for e in read_data("events")
}

event_articles = defaultdict(set)
article_events = defaultdict(set)
coverage = defaultdict(lambda: defaultdict(set))

for m in read_data("mentions"):
    e = m["event_id"]
    a = m["article_id"]

    event_articles[e].add(a)
    article_events[a].add(e)
    coverage[e][m["source"]].add(a)


def related_events(event_id):
    return {
        other
        for article in event_articles[event_id]
        for other in article_events[article]
        if other != event_id
        and countries[other] == COUNTRY
    }


# Prefer an event with several sources and related US events
candidates = [
    e for e in event_articles if related_events(e)
]

assert candidates, "No suitable event found"

EVENT = min(
    candidates,
    key=lambda e: (-len(coverage[e]), e)
)

expected_q5 = sorted(related_events(EVENT))

expected_q6 = [
    {"source": source, "total": len(articles)}
    for source, articles in coverage[EVENT].items()
]


def rank(rows):
    return sorted(
        rows, key=lambda r: (-r["total"], r["source"])
    )


def verify(name, q5, q6):
    assert sorted(q5) == expected_q5, f"{name}: Q5 failed"
    assert rank(q6) == rank(expected_q6), f"{name}: Q6 failed"

    print(f"\n===== {name} =====")
    print("Q5 passed:", len(q5), "related events")
    print("Q6 passed:", len(q6), "sources")
    print("Top 3 sources:", rank(q6)[:3])


print("Selected event:", EVENT)
print("Connected articles:", len(event_articles[EVENT]))


# 2. ArangoDB

client = ArangoClient(hosts="http://127.0.0.1:8529")

db = client.db(
    "gdelt_1000",
    username="root",
    password=os.environ["ARANGO_ROOT_PASSWORD"]
)

q5 = list(db.aql.execute("""
    FOR a IN 1..1 INBOUND @start mentions
        FOR e IN 1..1 OUTBOUND a._id mentions
            FILTER e.event_id != @event
            FILTER e.country_code == @country
            RETURN DISTINCT e.event_id
""", bind_vars={
    "start": f"events/{EVENT}",
    "event": EVENT,
    "country": COUNTRY
}))

q6 = list(db.aql.execute("""
    FOR m IN mentions
        FILTER m._to == @start
        COLLECT source = m.source INTO ids = m.article_id
        LET total = LENGTH(UNIQUE(ids))
        SORT total DESC, source ASC
        RETURN {source: source, total: total}
""", bind_vars={"start": f"events/{EVENT}"}))

verify("ArangoDB", q5, q6)
client.close()


# 3. SurrealDB

with Surreal("ws://127.0.0.1:8001") as db:
    db.signin({
        "username": os.environ["SURREAL_USER"],
        "password": os.environ["SURREAL_PASS"]
    })

    db.use("gdelt", "gdelt_1000")

    q5 = db.query("""
        SELECT VALUE event_id
        FROM array::group(
            $start<-mentions<-articles->mentions->events
        )
        WHERE country_code = $country
          AND event_id != $event;
    """, {
        "start": RecordID("events", EVENT),
        "country": COUNTRY,
        "event": EVENT
    }).first()

    q6 = db.query("""
        SELECT source, count() AS total
        FROM (
            SELECT source, article_id
            FROM mentions
            WHERE out = $start
            GROUP BY source, article_id
        )
        GROUP BY source
        ORDER BY total DESC, source ASC;
    """, {
        "start": RecordID("events", EVENT)
    }).first()

    verify("SurrealDB", q5, q6)


# 4. PostgreSQL

with psycopg.connect(
    host="127.0.0.1",
    port=5433,
    dbname="multimodel",
    user="benchmark",
    password=os.environ["POSTGRES_PASSWORD"],
    row_factory=dict_row
) as conn:

    conn.execute("SET search_path TO gdelt_1000")

    rows = conn.execute("""
        SELECT DISTINCT e.event_id
        FROM mentions m1
        JOIN mentions m2
            ON m1.article_id = m2.article_id
        JOIN events e
            ON e.event_id = m2.event_id
        WHERE m1.event_id = %s
          AND e.event_id <> %s
          AND e.country_code = %s;
    """, (EVENT, EVENT, COUNTRY)).fetchall()

    q5 = [row["event_id"] for row in rows]

    q6 = conn.execute("""
        SELECT source,
               COUNT(DISTINCT article_id) AS total
        FROM mentions
        WHERE event_id = %s
        GROUP BY source
        ORDER BY total DESC, source ASC;
    """, (EVENT,)).fetchall()

    verify("PostgreSQL", q5, q6)

print("\nAll advanced queries passed!")
