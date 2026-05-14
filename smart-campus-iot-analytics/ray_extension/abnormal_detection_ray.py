import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path

import ray


def safe_float(value):
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except ValueError:
        return None


def read_rows(input_path):
    rows = []
    with open(input_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if not row or not any((value or "").strip() for value in row.values()):
                continue
            rows.append({key: (value or "").strip() for key, value in row.items()})
    return rows


def split_chunks(rows, num_chunks):
    if not rows:
        return []
    chunk_size = max(1, math.ceil(len(rows) / num_chunks))
    return [rows[index : index + chunk_size] for index in range(0, len(rows), chunk_size)]


@ray.remote
def process_chunk(rows):
    partial = {}
    for row in rows:
        device_id = row.get("device_id", "")
        if not device_id:
            continue

        device = partial.setdefault(
            device_id,
            {
                "building": row.get("building", ""),
                "low_battery": False,
                "error_count": 0,
                "high_temp_count": 0,
            },
        )

        if not device["building"] and row.get("building", ""):
            device["building"] = row.get("building", "")

        battery_level = safe_float(row.get("battery_level"))
        if battery_level is not None and battery_level < 20:
            device["low_battery"] = True

        status = row.get("status", "").upper()
        if status == "ERROR":
            device["error_count"] += 1

        sensor_type = row.get("sensor_type", "").lower()
        value = safe_float(row.get("value"))
        if sensor_type == "temperature" and value is not None and value > 32:
            device["high_temp_count"] += 1

    return partial


def merge_partials(partials):
    merged = defaultdict(lambda: {"building": "", "low_battery": False, "error_count": 0, "high_temp_count": 0})

    for partial in partials:
        for device_id, stats in partial.items():
            device = merged[device_id]
            if not device["building"] and stats.get("building"):
                device["building"] = stats["building"]
            device["low_battery"] = device["low_battery"] or stats["low_battery"]
            device["error_count"] += stats["error_count"]
            device["high_temp_count"] += stats["high_temp_count"]

    return merged


def detect_abnormal_devices(merged_stats):
    abnormal_rows = []

    for device_id, stats in merged_stats.items():
        reasons = []
        if stats["low_battery"]:
            reasons.append("low battery")
        if stats["error_count"] >= 3:
            reasons.append("repeated errors")
        if stats["high_temp_count"] >= 3:
            reasons.append("repeated high temperature")

        for reason in reasons:
            abnormal_rows.append(
                {
                    "device_id": device_id,
                    "building": stats["building"] or "UNKNOWN",
                    "reason": reason,
                }
            )

    return sorted(abnormal_rows, key=lambda row: (row["device_id"], row["reason"]))


def write_output(rows, output_path):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=["device_id", "building", "reason"])
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Ray extension: abnormal device detection.")
    parser.add_argument("--input", default="data/Comp3006J MiniProject 2 Dataset.csv", help="Path to input IoT log CSV.")
    parser.add_argument("--output", default="outputs/abnormal_devices.csv", help="Path to output CSV file.")
    parser.add_argument("--chunks", type=int, default=4, help="Number of parallel chunks/tasks.")
    args = parser.parse_args()

    rows = read_rows(args.input)
    chunks = split_chunks(rows, max(1, args.chunks))

    ray.init(ignore_reinit_error=True)
    try:
        partial_refs = [process_chunk.remote(chunk) for chunk in chunks]
        partials = ray.get(partial_refs)
    finally:
        ray.shutdown()

    merged_stats = merge_partials(partials)
    abnormal_rows = detect_abnormal_devices(merged_stats)
    write_output(abnormal_rows, args.output)


if __name__ == "__main__":
    main()
