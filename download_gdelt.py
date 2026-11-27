"""Download only the GDELT files listed in gdelt_files.csv.

Run this script yourself from your Ubuntu project. Python's standard library
is sufficient. Existing files are reused only if their size and checksum match.
"""

import csv
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.client import HTTPException
from pathlib import Path
from urllib.request import urlopen


PROJECT = Path(__file__).resolve().parent
MANIFEST = PROJECT / "gdelt_files.csv"


def matches(path, entry):
    """Check the exact source bytes, rather than trusting a filename."""
    if not path.is_file() or path.stat().st_size != int(entry["bytes"]):
        return False
    return hashlib.md5(path.read_bytes()).hexdigest() == entry["md5"]


def download(entry):
    """Fetch one official file; retry a temporary network failure twice."""
    folder = "reference" if entry["kind"] == "reference" else "raw/gdelt2"
    destination = PROJECT / "data" / folder / entry["filename"]
    destination.parent.mkdir(parents=True, exist_ok=True)

    if matches(destination, entry):
        return entry["filename"], "already verified", None

    for attempt in range(3):
        try:
            with urlopen(entry["url"], timeout=60) as response:
                content = response.read()
            if len(content) != int(entry["bytes"]):
                raise ValueError("downloaded size does not match the manifest")
            if hashlib.md5(content).hexdigest() != entry["md5"]:
                raise ValueError("downloaded checksum does not match the manifest")

            temporary = destination.with_name(destination.name + ".part")
            temporary.write_bytes(content)
            temporary.replace(destination)
            return entry["filename"], "downloaded and verified", None
        except (OSError, ValueError, HTTPException) as error:
            if attempt == 2:
                return entry["filename"], "FAILED", str(error)
            time.sleep(2)


def main():
    with MANIFEST.open(encoding="utf-8", newline="") as file:
        entries = list(csv.DictReader(file))

    # Fail before any downloads if a manifest entry has an unexpected path.
    allowed = (
        "https://data.gdeltproject.org/",
        "https://www.gdeltproject.org/",
        "https://gdeltproject.org/",
    )
    for entry in entries:
        if Path(entry["filename"]).name != entry["filename"]:
            raise ValueError("Manifest filenames must not contain directories")
        if not entry["url"].startswith(allowed):
            raise ValueError("Manifest contains a non-GDELT download URL")
        if entry["kind"] not in {"events", "mentions", "reference"}:
            raise ValueError("Manifest contains an unsupported file kind")

    total_bytes = sum(int(entry["bytes"]) for entry in entries)
    print(f"Files: {len(entries):,}; full download: {total_bytes / 1_000_000:.1f} MB")
    print(f"Destination: {PROJECT / 'data'}")
    print("Four downloads at a time. Rerunning skips verified files.")

    failures = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = [pool.submit(download, entry) for entry in entries]
        try:
            for number, future in enumerate(as_completed(pending), start=1):
                filename, status, error = future.result()
                print(f"[{number}/{len(entries)}] {status}: {filename}", flush=True)
                if error:
                    failures.append((filename, error))
        except KeyboardInterrupt:
            for future in pending:
                future.cancel()
            print("\nStopping; waiting for the active downloads to finish.")
            raise

    if failures:
        print("\nSome files failed. Run the same command again to retry them:")
        for filename, error in failures:
            print(f"  {filename}: {error}")
        raise SystemExit(1)

    print("\nAll listed files are downloaded and verified.")
    print("Keep the ZIP files compressed. Database import is a separate step.")


if __name__ == "__main__":
    main()
