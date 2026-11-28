"""Prepare nested GDELT datasets, preserving the original 1K sample."""
import argparse
import hashlib
import json
import zipfile
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

SIZES = (1000, 10000, 100000, 1000000)
TABLES = ("events", "articles", "mentions")
KEYS = {"events": "event_id", "articles": "article_id", "mentions": "mention_id"}


def raw_rows(raw, suffix):
    files = sorted(raw.glob(f"*.{suffix}.CSV.zip"))
    if not files:
        raise FileNotFoundError(f"No {suffix} archives in {raw}")
    for path in files:
        with zipfile.ZipFile(path) as archive:
            name = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
            with archive.open(name) as file:
                for number, line in enumerate(file, 1):
                    yield path.name.split(".")[0], number, line.decode("utf-8").rstrip("\r\n").split("\t")


def timestamp(value):
    return datetime.strptime(value, "%Y%m%d%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")


def event_record(r):
    return {
        "event_id": int(r[0]),
        "event_date": datetime.strptime(r[1], "%Y%m%d").date().isoformat(),
        "date_added": timestamp(r[59]),
        "actor1_name": r[6] or None,
        "actor2_name": r[16] or None,
        "event_code": r[26],
        "event_root_code": r[28],
        "country_code": r[53] or None,
        "action_location": r[52] or None,
        "review_status": "unreviewed",
    }


def records_by_id(path, key):
    with path.open(encoding="utf-8") as file:
        rows = [json.loads(line) for line in file]
    result = {r[key]: r for r in rows}
    if len(result) != len(rows):
        raise ValueError(f"Duplicate IDs in {path}")
    return result


def check_sample(original, generated):
    for table in TABLES:
        old = records_by_id(original / f"{table}.jsonl", KEYS[table])
        new = records_by_id(generated / f"{table}.jsonl", KEYS[table])
        if old != new:
            raise ValueError(f"{table}: regenerated 1K sample differs from original; nothing published")


def prepare(root, sizes=SIZES):
    sizes = tuple(sorted(set(sizes)))
    baseline = sizes[0]
    raw = root / "data/raw/gdelt2"
    output = root / "data/processed"
    original = output / str(baseline)
    for table in TABLES:
        if not (original / f"{table}.jsonl").is_file():
            raise FileNotFoundError(f"Missing original sample: {original}/{table}.jsonl")
    for size in sizes[1:]:
        if (output / str(size)).exists():
            raise FileExistsError(f"{output / str(size)} already exists; no datasets overwritten")
    # Never overwrite any published dataset. Generate into a temporary directory first.
    with TemporaryDirectory(prefix=".scale-", dir=output) as tmp:
        staging = Path(tmp)
        ranks = {}
        # Per dataset, map each article to its first event, or None when shared.
        owners = {size: {} for size in sizes}
        shared = dict.fromkeys(sizes, 0)
        mention_counts = dict.fromkeys(sizes, 0)
        linked = bytearray(sizes[-1])

        with ExitStack() as stack:
            writers = {}
            for size in sizes:
                folder = staging / str(size)
                folder.mkdir()
                for table in TABLES:
                    writers[size, table] = stack.enter_context(
                        (folder / f"{table}.jsonl").open("w", encoding="utf-8")
                    )

            print("Reading events...", flush=True)
            for _, _, r in raw_rows(raw, "export"):
                event_id = int(r[0])
                if event_id in ranks:
                    continue
                rank = len(ranks) + 1
                ranks[event_id] = rank
                line = json.dumps(event_record(r)) + "\n"
                for size in sizes:
                    if rank <= size:
                        writers[size, "events"].write(line)
                if rank % 100000 == 0:
                    print(f"  {rank:,} unique events", flush=True)
                if rank == sizes[-1]:
                    break
            if len(ranks) != sizes[-1]:
                raise ValueError(f"Not enough events: found {len(ranks)}")

            print("Reading all mentions...", flush=True)
            for number, (batch, row_no, r) in enumerate(raw_rows(raw, "mentions"), 1):
                event_id = int(r[0])
                rank = ranks.get(event_id)
                if rank is not None:
                    if r[3] != "1" or not r[4] or not r[5]:
                        raise ValueError(f"Unexpected selected mention: {batch}_{row_no}")
                    article_id = hashlib.sha256(f"{r[3]}\t{r[5]}".encode()).hexdigest()
                    article = {"article_id": article_id, "url": r[5], "mention_type": int(r[3])}
                    mention = {
                        "mention_id": f"{batch}_{row_no}",
                        "article_id": article_id,
                        "event_id": event_id,
                        "source": r[4],
                        "event_first_seen": timestamp(r[1]),
                        "mentioned_at": timestamp(r[2]),
                        "confidence": int(r[11]) if r[11] else None,
                    }
                    article_line = json.dumps(article) + "\n"
                    mention_line = json.dumps(mention) + "\n"
                    linked[rank - 1] = 1
                    for size in sizes:
                        if rank > size:
                            continue
                        seen = owners[size]
                        if article_id not in seen:
                            seen[article_id] = event_id
                            writers[size, "articles"].write(article_line)
                        elif seen[article_id] is not None and seen[article_id] != event_id:
                            seen[article_id] = None
                            shared[size] += 1
                        writers[size, "mentions"].write(mention_line)
                        mention_counts[size] += 1
                if number % 250000 == 0:
                    print(f"  {number:,} raw mentions examined", flush=True)

        for size in sizes:
            summary = {
                "events": size,
                "articles": len(owners[size]),
                "mentions": mention_counts[size],
                "shared_articles": shared[size],
                "events_with_mentions": sum(linked[:size]),
            }
            if summary["events_with_mentions"] != size:
                raise ValueError(f"Not all events have mentions at size {size}")
            (staging / str(size) / "summary.json").write_text(
                json.dumps(summary, indent=2) + "\n", encoding="utf-8"
            )
            print(f"{size:,}: {summary}", flush=True)

        check_sample(original, staging / str(baseline))
        print(f"Original {baseline:,}-event sample reproduced exactly (all three tables).", flush=True)
        # Existing larger datasets are not silently replaced either.
        for size in sizes[1:]:
            if (output / str(size)).exists():
                raise FileExistsError(f"{output / str(size)} already exists; no datasets overwritten")
        for size in sizes[1:]:
            (staging / str(size)).rename(output / str(size))
        print("New datasets ready; original sample unchanged.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    prepare(args.root)
