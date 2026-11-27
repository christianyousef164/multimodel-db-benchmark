
import json
import hashlib
import zipfile
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

DATA = Path("data/raw/gdelt2")
SIZE = 1000
OUTPUT = Path("data/processed") / str(SIZE)

OUTPUT.mkdir(parents=True, exist_ok=True)


def read_rows(suffix, columns):
    files = sorted(DATA.glob(f"*{suffix}"))

    for path in tqdm(files, desc=suffix):
        with zipfile.ZipFile(path) as archive:
            name = next(
                n for n in archive.namelist()
                if n.lower().endswith(".csv")
            )

            with archive.open(name) as file:
                for number, line in enumerate(file, start=1):
                    row = line.decode("utf-8").rstrip("\r\n").split("\t")

                    if len(row) != columns:
                        raise ValueError(f"Invalid row: {path}, {number}")

                    yield path, number, row


def write_record(file, record):
    file.write(json.dumps(record, ensure_ascii=False) + "\n")


def format_date(value):
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def format_timestamp(value):
    return datetime.strptime(
        value, "%Y%m%d%H%M%S"
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


# ==========================
# 1. Prepare events
# ==========================

event_ids = set()

with open(OUTPUT / "events.jsonl", "w", encoding="utf-8") as file:

    for path, number, row in read_rows(".export.CSV.zip", 61):

        event_id = int(row[0])

        if event_id in event_ids:
            raise ValueError(f"Duplicate event ID: {event_id}")

        event = {
            "event_id": event_id,
            "event_date": format_date(row[1]),
            "date_added": format_timestamp(row[59]),
            "actor1_name": row[6] or None,
            "actor2_name": row[16] or None,
            "event_code": row[26],
            "event_root_code": row[28],
            "country_code": row[53] or None,
            "action_location": row[52] or None,
            "review_status": "unreviewed",
        }

        write_record(file, event)
        event_ids.add(event_id)

        if len(event_ids) == SIZE:
            break

if len(event_ids) != SIZE:
    raise ValueError("Not enough unique events")


# ==========================
# 2. Prepare articles and mentions
# ==========================

articles = {}
shared_articles = set()
linked_events = set()
mention_count = 0

with (
    open(OUTPUT / "articles.jsonl", "w", encoding="utf-8") as article_file,
    open(OUTPUT / "mentions.jsonl", "w", encoding="utf-8") as mention_file,
):

    for path, number, row in read_rows(".mentions.CSV.zip", 16):

        event_id = int(row[0])

        if event_id not in event_ids:
            continue

        if row[3] != "1" or not row[4] or not row[5]:
            raise ValueError(f"Unexpected mention: {path}, {number}")

        url = row[5]

        # Generate a stable article identifier
        article_id = hashlib.sha256(
            f"{row[3]}\t{url}".encode("utf-8")
        ).hexdigest()

        # Create each article only once
        if article_id not in articles:

            article = {
                "article_id": article_id,
                "url": url,
                "mention_type": int(row[3]),
            }

            write_record(article_file, article)
            articles[article_id] = event_id

        elif articles[article_id] != event_id:
            shared_articles.add(article_id)

        batch = path.name.split(".")[0]

        mention = {
            "mention_id": f"{batch}_{number}",
            "article_id": article_id,
            "event_id": event_id,
            "source": row[4],
            "event_first_seen": format_timestamp(row[1]),
            "mentioned_at": format_timestamp(row[2]),
            "confidence": int(row[11]) if row[11] else None,
        }

        write_record(mention_file, mention)

        linked_events.add(event_id)
        mention_count += 1


# ==========================
# 3. Verify and summarize
# ==========================

if linked_events != event_ids:
    raise ValueError("Some selected events have no mentions")

summary = {
    "events": len(event_ids),
    "articles": len(articles),
    "mentions": mention_count,
    "shared_articles": len(shared_articles),
    "events_with_mentions": len(linked_events),
}

with open(OUTPUT / "summary.json", "w") as file:
    json.dump(summary, file, indent=4)

print("\n===== SAMPLE PREPARED =====")

for name, count in summary.items():
    print(f"{name}: {count}")

print("Output folder:", OUTPUT)
