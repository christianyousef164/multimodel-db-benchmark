import os

import psycopg
from arango import ArangoClient
from dotenv import load_dotenv
from surrealdb import Surreal

# Read passwords from .env
load_dotenv()

# 1. ArangoDB
client = ArangoClient(hosts="http://127.0.0.1:8529")
arango = client.db(
    "_system",
    username="root",
    password=os.environ["ARANGO_ROOT_PASSWORD"],
)
print("ArangoDB connected:", arango.version())

# 2. SurrealDB
with Surreal("ws://127.0.0.1:8001") as db:
    db.signin(
        {
            "username": os.environ["SURREAL_USER"],
            "password": os.environ["SURREAL_PASS"],
        }
    )
    db.use("main", "main")
    result = db.query("RETURN 1 + 1;").first()
    assert result == 2, "SurrealDB returned an unexpected result"
    print("SurrealDB connected:", result)

# 3. PostgreSQL
with psycopg.connect(
    host="127.0.0.1",
    port=5433,
    dbname="multimodel",
    user="benchmark",
    password=os.environ["POSTGRES_PASSWORD"],
) as conn:
    result = conn.execute("SELECT 1 + 1").fetchone()
    print("PostgreSQL connected:", result[0])

client.close()
print("All three connections succeeded!")
