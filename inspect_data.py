
import csv
import io
import zipfile
from pathlib import Path

data_folder = Path("data/raw/gdelt2")

file_types = {
    "Events": ".export.CSV.zip",
    "Mentions": ".mentions.CSV.zip",
}

for name, suffix in file_types.items():
    files = sorted(data_folder.glob(f"*{suffix}"))

    if not files:
        raise FileNotFoundError(f"No {name} files found")

    sample_file = files[0]

    with zipfile.ZipFile(sample_file) as archive:
        csv_name = next(
            name for name in archive.namelist()
            if name.lower().endswith(".csv")
        )

        with archive.open(csv_name) as stream:
            text = io.TextIOWrapper(stream, encoding="utf-8")
            reader = csv.reader(text, delimiter="\t")
            first_row = next(reader)

    print(f"\n===== {name} =====")
    print("File:", sample_file.name)
    print("Number of columns:", len(first_row))
    print("First record ID:", first_row[0])

    print("\nFirst 10 column values:")
    for index, value in enumerate(first_row[:10], start=1):
        print(f"Column {index}: {value[:100]}")
