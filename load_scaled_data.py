"""Load one prepared GDELT dataset into one database, then ensure indexes."""
import argparse
import json
import os
from itertools import islice
from pathlib import Path
from time import perf_counter

import psycopg
from arango import ArangoClient
from dotenv import load_dotenv
from surrealdb import RecordID, Surreal
from tqdm import tqdm

SIZES = (1000, 10000, 100000, 1000000)
TABLES = ("events", "articles", "mentions")
KEYS = {"events": "event_id", "articles": "article_id", "mentions": "mention_id"}
FIELDS = {
    "events": ("event_id", "event_date", "date_added", "actor1_name", "actor2_name",
               "event_code", "event_root_code", "country_code", "action_location", "review_status"),
    "articles": ("article_id", "url", "mention_type"),
    "mentions": ("mention_id", "article_id", "event_id", "source", "event_first_seen", "mentioned_at", "confidence"),
}
FILTER_FIELDS = ["country_code", "event_root_code", "event_date"]


def read_records(folder, table):
    with (folder / f"{table}.jsonl").open(encoding="utf-8") as file:
        for line in file:
            yield json.loads(line)


def batches(records, size):
    while batch := list(islice(records, size)):
        yield batch


def expected_counts(folder):
    counts = {}
    for table in TABLES:
        with (folder / f"{table}.jsonl").open("rb") as file:
            counts[table] = sum(1 for _ in file)
    return counts


def should_load(table, actual, expected, size):
    if actual == expected:
        print(f"{table}: {actual:,} existing records; skipping reload")
        return False
    if size == 1000:
        raise RuntimeError(f"Original 1K database has unexpected {table} count: {actual} != {expected}; not modifying its data")
    if actual != 0:
        raise RuntimeError(f"{table} has {actual:,}/{expected:,} rows; partial or unexpected load. No records deleted. Stop and inspect before retrying.")
    return True


def verify_counts(count, expected):
    actual = {table: count(table) for table in TABLES}
    if actual != expected:
        raise RuntimeError(f"Count mismatch: {actual} != {expected}")
    print("Verified counts:", actual, flush=True)


def arango_record(table, r):
    r = dict(r, _key=str(r[KEYS[table]]))
    if table == "mentions":
        r["_from"] = f"articles/{r['article_id']}"
        r["_to"] = f"events/{r['event_id']}"
    return r


def load_arango(name, folder, expected, size, batch_size):
    client = ArangoClient(hosts="http://127.0.0.1:8529")
    password = os.environ["ARANGO_ROOT_PASSWORD"]
    try:
        system = client.db("_system", username="root", password=password)
        if not system.has_database(name):
            if size == 1000:
                raise RuntimeError("Original ArangoDB database not found")
            system.create_database(name)
        db = client.db(name, username="root", password=password)
        for table in TABLES:
            if not db.has_collection(table):
                if size == 1000:
                    raise RuntimeError(f"Original collection missing: {table}")
                db.create_collection(table, edge=(table == "mentions"))

        count = lambda table: db.collection(table).count()
        for table in TABLES:
            if not should_load(table, count(table), expected[table], size):
                continue
            collection = db.collection(table)
            with tqdm(total=expected[table], desc=f"ArangoDB {table}", unit="rows") as progress:
                for batch in batches(read_records(folder, table), batch_size):
                    result = collection.import_bulk(
                        [arango_record(table, r) for r in batch],
                        on_duplicate="error", halt_on_error=True,
                    )
                    if result.get("errors", 0) or result.get("created") != len(batch):
                        raise RuntimeError(f"ArangoDB import failed: {result}")
                    progress.update(len(batch))
        print("Ensuring ArangoDB composite index...", flush=True)
        db.collection("events").add_persistent_index(
            fields=FILTER_FIELDS, unique=False, sparse=False, name="idx_event_filter"
        )
        print("Event indexes:", db.collection("events").indexes(), flush=True)
        print("Mention indexes:", db.collection("mentions").indexes(), flush=True)
        verify_counts(count, expected)
    finally:
        client.close()


def surreal_record(table, r):
    r = dict(r, id=RecordID(table, r[KEYS[table]]))
    if table == "mentions":
        r["in"] = RecordID("articles", r["article_id"])
        r["out"] = RecordID("events", r["event_id"])
    return r


def load_surreal(name, folder, expected, size, batch_size):
    with Surreal("ws://127.0.0.1:8001") as db:
        db.signin({"username": os.environ["SURREAL_USER"], "password": os.environ["SURREAL_PASS"]})
        def run(query, **params):
            return db.query(query, params).first()

        run("DEFINE NAMESPACE IF NOT EXISTS gdelt;")
        db.use("gdelt", name)
        if size != 1000:
            run(f"DEFINE DATABASE IF NOT EXISTS {name};")
            run("DEFINE TABLE IF NOT EXISTS events;")
            run("DEFINE TABLE IF NOT EXISTS articles;")
            run("DEFINE TABLE IF NOT EXISTS mentions TYPE RELATION IN articles OUT events ENFORCED;")

        def count(table):
            rows = run(f"SELECT count() AS total FROM {table} GROUP ALL;")
            return int(rows[0]["total"]) if rows else 0

        for table in TABLES:
            if not should_load(table, count(table), expected[table], size):
                continue
            relation = " RELATION" if table == "mentions" else ""
            with tqdm(total=expected[table], desc=f"SurrealDB {table}", unit="rows") as progress:
                for batch in batches(read_records(folder, table), batch_size):
                    run(f"INSERT{relation} INTO {table} $rows RETURN NONE;",
                        rows=[surreal_record(table, r) for r in batch])
                    progress.update(len(batch))
        print("Ensuring SurrealDB indexes...", flush=True)
        run("DEFINE INDEX IF NOT EXISTS idx_event_filter ON TABLE events FIELDS country_code, event_root_code, event_date;")
        run("DEFINE INDEX IF NOT EXISTS idx_mention_event ON TABLE mentions FIELDS out;")
        print("Event table info:", run("INFO FOR TABLE events;"), flush=True)
        print("Mention table info:", run("INFO FOR TABLE mentions;"), flush=True)
        verify_counts(count, expected)


DDL = {
    "events": """CREATE TABLE IF NOT EXISTS events (
        event_id BIGINT PRIMARY KEY, event_date DATE, date_added TIMESTAMPTZ,
        actor1_name TEXT, actor2_name TEXT, event_code TEXT, event_root_code TEXT,
        country_code TEXT, action_location TEXT, review_status TEXT
    )""",
    "articles": """CREATE TABLE IF NOT EXISTS articles (
        article_id TEXT PRIMARY KEY, url TEXT, mention_type INTEGER
    )""",
    "mentions": """CREATE TABLE IF NOT EXISTS mentions (
        mention_id TEXT PRIMARY KEY, article_id TEXT REFERENCES articles(article_id),
        event_id BIGINT REFERENCES events(event_id), source TEXT,
        event_first_seen TIMESTAMPTZ, mentioned_at TIMESTAMPTZ, confidence INTEGER
    )""",
}


def load_postgres(name, folder, expected, size, batch_size):
    with psycopg.connect(host="127.0.0.1", port=5433, dbname="multimodel",
                         user="benchmark", password=os.environ["POSTGRES_PASSWORD"],
                         autocommit=True) as conn:
        # name is derived solely from a validated integer size, not free-form input.
        if size != 1000:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {name}")
        conn.execute(f"SET search_path TO {name}")
        if size != 1000:
            for table in TABLES:
                conn.execute(DDL[table])

        def count(table):
            return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

        for table in TABLES:
            if not should_load(table, count(table), expected[table], size):
                continue
            fields = FIELDS[table]
            columns = ", ".join(fields)
            # Each COPY is bounded to limit pending foreign-key trigger events.
            # The explicit transaction makes the entire table load atomic.
            with conn.transaction(), conn.cursor() as cur, tqdm(
                total=expected[table], desc=f"PostgreSQL {table}", unit="rows"
            ) as progress:
                for batch in batches(read_records(folder, table), max(batch_size, 10000)):
                    with cur.copy(f"COPY {table} ({columns}) FROM STDIN") as copy:
                        for r in batch:
                            copy.write_row(tuple(r[field] for field in fields))
                    progress.update(len(batch))
        print("Ensuring PostgreSQL indexes...", flush=True)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_event_filter ON events (country_code, event_root_code, event_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mention_event ON mentions (event_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mention_article ON mentions (article_id)")
        conn.execute("ANALYZE events")
        conn.execute("ANALYZE articles")
        conn.execute("ANALYZE mentions")
        rows = conn.execute("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = %s ORDER BY tablename, indexname", (name,)).fetchall()
        for row in rows:
            print(row)
        verify_counts(count, expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("engine", choices=("postgres", "arangodb", "surrealdb"))
    parser.add_argument("size", type=int, choices=SIZES)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    load_dotenv()
    folder = Path("data/processed") / str(args.size)
    expected = expected_counts(folder)
    if expected["events"] != args.size or not expected["articles"] or not expected["mentions"]:
        raise ValueError(f"Invalid prepared counts: {expected}")
    if args.size != 1000:
        summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
        if any(summary[table] != expected[table] for table in TABLES):
            raise ValueError("Prepared files do not match summary counts")
    print("Expected counts:", expected, flush=True)
    loader = {"postgres": load_postgres, "arangodb": load_arango, "surrealdb": load_surreal}[args.engine]
    start = perf_counter()
    loader(f"gdelt_{args.size}", folder, expected, args.size, args.batch_size)
    print(f"{args.engine} {args.size:,}: ready. Diagnostic elapsed time (including setup, index creation/checks, possible skips): {perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
