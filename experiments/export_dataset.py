import argparse
import csv
import json
from pathlib import Path

from analysis import _record_epoch, _series_to_timestamp_map, load_ground_truth


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a flattened dataset from one experiment result JSON.")
    parser.add_argument("result", help="Experiment result JSON file.")
    parser.add_argument("--output", help="Optional CSV output path.")
    args = parser.parse_args()

    result_path = Path(args.result)
    with result_path.open("r", encoding="utf-8") as handle:
        result = json.load(handle)

    ground_truth = load_ground_truth(Path(result["ground_truth_file"]))
    anomaly_by_second = _series_to_timestamp_map(result["queries"].get("detector_flags", {"data": {"result": []}}))
    confidence_by_second = _series_to_timestamp_map(result["queries"].get("monitoring_confidence", {"data": {"result": []}}))

    output_path = Path(args.output) if args.output else result_path.with_suffix(".dataset.csv")
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["timestamp", "elapsed_seconds", "asset_id", "asset_type", "mode", "event_label", "is_fault", "anomaly_flag", "monitoring_confidence"],
        )
        writer.writeheader()
        for record in ground_truth:
            epoch = _record_epoch(record)
            writer.writerow(
                {
                    "timestamp": record["timestamp"],
                    "elapsed_seconds": record["elapsed_seconds"],
                    "asset_id": record["asset_id"],
                    "asset_type": record["asset_type"],
                    "mode": record["mode"],
                    "event_label": record["event_label"],
                    "is_fault": int(record.get("mode", "nominal") != "nominal"),
                    "anomaly_flag": int(anomaly_by_second.get(epoch, 0.0) >= 1.0),
                    "monitoring_confidence": confidence_by_second.get(epoch, 0.0),
                }
            )

    print(output_path)


if __name__ == "__main__":
    main()
