
import zipfile
from pathlib import Path
from collections import Counter
from tqdm import tqdm

DATA = Path("data/raw/gdelt2")


# Read rows directly from compressed files
def read_rows(suffix):
    files = sorted(DATA.glob(f"*{suffix}"))

    for path in tqdm(files, desc=suffix):
        with zipfile.ZipFile(path) as archive:
            name = next(
                n for n in archive.namelist()
                if n.lower().endswith(".csv")
            )

            with archive.open(name) as file:
                for line in file:
                    yield line.decode("utf-8").rstrip("\r\n").split("\t")


# ==========================
# 1. Inspect Events
# ==========================

event_ids = set()
event_years = Counter()
countries = Counter()
event_categories = Counter()

event_rows = 0
bad_events = 0

for row in read_rows(".export.CSV.zip"):
    if len(row) != 61 or not row[0].isdigit():
        bad_events += 1
        continue

    event_rows += 1
    event_ids.add(int(row[0]))

    event_years[row[3]] += 1
    countries[row[53] or "MISSING"] += 1
    event_categories[row[28] or "MISSING"] += 1


# ==========================
# 2. Inspect Mentions
# ==========================

mention_rows = 0
bad_mentions = 0
unmatched_mentions = 0
missing_document_ids = 0
missing_sources = 0

linked_events = set()
documents = {}
shared_documents = set()
sources = set()
mention_types = Counter()

for row in read_rows(".mentions.CSV.zip"):
    if len(row) != 16 or not row[0].isdigit():
        bad_mentions += 1
        continue

    mention_rows += 1

    event_id = int(row[0])
    mention_type = row[3]
    source = row[4]
    identifier = row[5]

    mention_types[mention_type] += 1

    if event_id not in event_ids:
        unmatched_mentions += 1
        continue

    if not identifier or not mention_type:
        missing_document_ids += 1
        continue

    # Identify a document by its type and identifier
    document_id = (mention_type, identifier)

    linked_events.add(event_id)

    if source:
        sources.add(source)
    else:
        missing_sources += 1

    if document_id not in documents:
        documents[document_id] = event_id
    elif documents[document_id] != event_id:
        shared_documents.add(document_id)


# ==========================
# 3. Print the results
# ==========================

print("\n===== EVENTS =====")
print("Valid event rows:", event_rows)
print("Unique event IDs:", len(event_ids))
print("Duplicate event IDs:", event_rows - len(event_ids))
print("Malformed events:", bad_events)
print("Event years:", dict(sorted(event_years.items())))
print("Top country codes:", countries.most_common(5))
print("Missing event countries:", countries["MISSING"])
print("Top event categories:", event_categories.most_common(5))

print("\n===== MENTIONS =====")
print("Valid mention rows:", mention_rows)
print("Malformed mentions:", bad_mentions)
print("Unmatched mentions:", unmatched_mentions)
print("Missing document IDs in matched mentions:", missing_document_ids)
print("Missing sources in usable mentions:", missing_sources)
print("Mention types:", dict(mention_types))

print("\n===== RELATIONSHIPS =====")
print("Unique linked documents:", len(documents))
print("Unique linked sources:", len(sources))
print("Events with usable mentions:", len(linked_events))
print("Events without usable mentions:",
      len(event_ids) - len(linked_events))
print("Documents connecting multiple events:",
      len(shared_documents))
