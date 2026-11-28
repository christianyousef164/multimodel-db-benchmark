
import os
import json
import psycopg
from pathlib import Path
from psycopg.rows import dict_row
from dotenv import load_dotenv
from arango import ArangoClient
from surrealdb import Surreal

load_dotenv()

DATA = Path("data/processed/1000")
EVENT = 1332291273


def read_data(name):
    with open(DATA / f"{name}.jsonl", encoding="utf-8") as file:
        return [json.loads(line) for line in file]


# 1. Calculate correct answers using Python
events = read_data("events")
articles = read_data("articles")
mentions = read_data("mentions")

event = next(e for e in events if e["event_id"] == EVENT)

fields = ["event_id", "country_code", "event_code"]
expected_event = {field: event[field] for field in fields}

article_ids = {
    m["article_id"] for m in mentions
    if m["event_id"] == EVENT
}

expected_urls = sorted({
    a["url"] for a in articles
    if a["article_id"] in article_ids
})

expected_related = sorted({
    m["event_id"] for m in mentions
    if m["article_id"] in article_ids
    and m["event_id"] != EVENT
})


# 2. Function to verify the answers
def verify(name, q1, q2, q3):
    assert len(q1) == 1, f"{name}: Q1 record count failed"
    assert q1[0] == expected_event, f"{name}: Q1 failed"
    assert sorted(q2) == expected_urls, f"{name}: Q2 failed"
    assert sorted(q3) == expected_related, f"{name}: Q3 failed"

    print(f"\n{name}: All three queries passed!")
    print("Articles:", len(q2))
    print("Related events:", len(q3))


# 3. ArangoDB
client = ArangoClient(hosts="http://127.0.0.1:8529")
db = client.db(
    "gdelt_1000",
    username="root",
    password=os.environ["ARANGO_ROOT_PASSWORD"]
)

q1 = list(db.aql.execute("""
    FOR e IN events
        FILTER e._key == "1332291273"
        RETURN {
            event_id: e.event_id,
            country_code: e.country_code,
            event_code: e.event_code
        }
"""))

q2 = list(db.aql.execute("""
    FOR a IN 1..1 INBOUND "events/1332291273" mentions
        RETURN DISTINCT a.url
"""))

q3 = list(db.aql.execute("""
    FOR a IN 1..1 INBOUND "events/1332291273" mentions
        FOR e IN 1..1 OUTBOUND a mentions
            FILTER e.event_id != 1332291273
            RETURN DISTINCT e.event_id
"""))

verify("ArangoDB", q1, q2, q3)
client.close()


# 4. SurrealDB
with Surreal("ws://127.0.0.1:8001") as db:
    db.signin({
        "username": os.environ["SURREAL_USER"],
        "password": os.environ["SURREAL_PASS"]
    })

    db.use("gdelt", "gdelt_1000")

    q1 = db.query("""
        SELECT event_id, country_code, event_code
        FROM events:1332291273;
    """).first()

    q2 = db.query("""
        SELECT VALUE array::group(<-mentions<-articles.url)
        FROM ONLY events:1332291273;
    """).first()

    q3 = db.query("""
        SELECT VALUE array::complement(
            array::group(
                <-mentions<-articles->mentions->events.event_id
            ),
            [1332291273]
        )
        FROM ONLY events:1332291273;
    """).first()

    verify("SurrealDB", q1, q2, q3)


# 5. PostgreSQL
with psycopg.connect(
    host="127.0.0.1",
    port=5433,
    dbname="multimodel",
    user="benchmark",
    password=os.environ["POSTGRES_PASSWORD"],
    row_factory=dict_row
) as conn:

    conn.execute("SET search_path TO gdelt_1000")

    q1 = conn.execute("""
        SELECT event_id, country_code, event_code
        FROM events
        WHERE event_id = 1332291273;
    """).fetchall()

    q2 = conn.execute("""
        SELECT DISTINCT a.url
        FROM articles a
        JOIN mentions m ON a.article_id = m.article_id
        WHERE m.event_id = 1332291273;
    """).fetchall()

    q3 = conn.execute("""
        SELECT DISTINCT m2.event_id
        FROM mentions m1
        JOIN mentions m2 ON m1.article_id = m2.article_id
        WHERE m1.event_id = 1332291273
          AND m2.event_id <> 1332291273;
    """).fetchall()

    verify(
        "PostgreSQL",
        q1,
        [row["url"] for row in q2],
        [row["event_id"] for row in q3]
    )

print("\nAll databases returned equivalent results!")
