import argparse
import csv
from collections import Counter
from pathlib import Path


def iter_rows(input_path):
    with open(input_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if not row or not any((value or "").strip() for value in row.values()):
                continue
            yield {key: (value or "").strip() for key, value in row.items()}


def map_device_counts(rows):
    for row in rows:
        device_id = row.get("device_id", "")
        if device_id:
            yield device_id, 1


def reduce_counts(mapped_items):
    counts = Counter()
    for key, value in mapped_items:
        counts[key] += value
    return counts


def write_output(counts, output_path, limit):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    ranked_devices = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    with open(output_path, "w", encoding="utf-8", newline="") as out_file:
        for device_id, count in ranked_devices:
            out_file.write(f"{device_id} {count}\n")


def main():
    parser = argparse.ArgumentParser(description="MapReduce baseline: top N most active devices.")
    parser.add_argument("--input", default="data/Comp3006J MiniProject 2 Dataset.csv", help="Path to input IoT log CSV.")
    parser.add_argument("--output", default="outputs/top_devices.txt", help="Path to output text file.")
    parser.add_argument("--limit", type=int, default=10, help="Number of devices to output.")
    args = parser.parse_args()

    rows = iter_rows(args.input)
    mapped_items = map_device_counts(rows)
    counts = reduce_counts(mapped_items)
    write_output(counts, args.output, args.limit)


if __name__ == "__main__":
    main()
