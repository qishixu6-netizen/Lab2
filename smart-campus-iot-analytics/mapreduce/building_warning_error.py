import argparse
import csv
from collections import Counter
from pathlib import Path


TARGET_STATUSES = {"WARNING", "ERROR"}


def iter_rows(input_path):
    with open(input_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if not row or not any((value or "").strip() for value in row.values()):
                continue
            yield {key: (value or "").strip() for key, value in row.items()}


def map_warning_error_by_building(rows):
    for row in rows:
        building = row.get("building", "")
        status = row.get("status", "").upper()
        if building and status in TARGET_STATUSES:
            yield building, 1


def reduce_counts(mapped_items):
    counts = Counter()
    for key, value in mapped_items:
        counts[key] += value
    return counts


def write_output(counts, output_path):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as out_file:
        for building, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
            out_file.write(f"{building} {count}\n")


def main():
    parser = argparse.ArgumentParser(description="MapReduce baseline: WARNING/ERROR count by building.")
    parser.add_argument("--input", default="data/Comp3006J MiniProject 2 Dataset.csv", help="Path to input IoT log CSV.")
    parser.add_argument("--output", default="outputs/building_warning_error.txt", help="Path to output text file.")
    args = parser.parse_args()

    rows = iter_rows(args.input)
    mapped_items = map_warning_error_by_building(rows)
    counts = reduce_counts(mapped_items)
    write_output(counts, args.output)


if __name__ == "__main__":
    main()
