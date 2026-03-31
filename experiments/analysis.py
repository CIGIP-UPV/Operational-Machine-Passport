import csv
import json
import math
from datetime import datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Tuple


def load_ground_truth(path: Path) -> List[Dict]:
    records = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def _series_to_timestamp_map(query_payload: Dict) -> Dict[int, float]:
    timeline: Dict[int, float] = {}
    results = query_payload.get("data", {}).get("result", [])
    for series in results:
        for timestamp_raw, value_raw in series.get("values", []):
            timestamp = int(float(timestamp_raw))
            value = float(value_raw)
            timeline[timestamp] = max(value, timeline.get(timestamp, 0.0))
    return timeline


def _interval_values(query_payload: Dict) -> List[float]:
    values: List[float] = []
    for series in query_payload.get("data", {}).get("result", []):
        values.extend(float(raw_value) for _, raw_value in series.get("values", []))
    return values


def _positive_event(record: Dict) -> bool:
    return record.get("mode", "nominal") != "nominal"


def _record_epoch(record: Dict) -> int:
    return int(datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00")).timestamp())


def _classification_metrics(truth_flags: List[bool], predicted_flags: List[bool]) -> Dict[str, float]:
    tp = sum(1 for truth, pred in zip(truth_flags, predicted_flags) if truth and pred)
    tn = sum(1 for truth, pred in zip(truth_flags, predicted_flags) if not truth and not pred)
    fp = sum(1 for truth, pred in zip(truth_flags, predicted_flags) if not truth and pred)
    fn = sum(1 for truth, pred in zip(truth_flags, predicted_flags) if truth and not pred)

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    specificity = tn / (tn + fp) if tn + fp else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return {
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "specificity": specificity,
        "f1": f1,
    }


def _fault_event_windows(records: List[Dict]) -> List[Tuple[int, int]]:
    windows: List[Tuple[int, int]] = []
    current_start = None
    previous_epoch = None
    for record in records:
        epoch = _record_epoch(record)
        is_fault = _positive_event(record)
        if is_fault and current_start is None:
            current_start = epoch
        if not is_fault and current_start is not None and previous_epoch is not None:
            windows.append((current_start, previous_epoch))
            current_start = None
        previous_epoch = epoch
    if current_start is not None and previous_epoch is not None:
        windows.append((current_start, previous_epoch))
    return windows


def _latency_metrics(records: List[Dict], anomaly_by_second: Dict[int, float]) -> Dict[str, float]:
    latencies = []
    for start, end in _fault_event_windows(records):
        detection = None
        for second in range(start, end + 1):
            if anomaly_by_second.get(second, 0.0) >= 1.0:
                detection = second
                break
        if detection is not None:
            latencies.append(detection - start)
    average = mean(latencies) if latencies else math.nan
    return {
        "fault_event_count": len(_fault_event_windows(records)),
        "detected_fault_event_count": len(latencies),
        "average_detection_latency_seconds": average,
        "max_detection_latency_seconds": max(latencies) if latencies else math.nan,
    }


def summarize_run(result_path: Path) -> Dict:
    with result_path.open("r", encoding="utf-8") as handle:
        result = json.load(handle)

    ground_truth_path = Path(result["ground_truth_file"])
    ground_truth = load_ground_truth(ground_truth_path)
    anomaly_payload = result["queries"].get("detector_flags", {"data": {"result": []}})
    anomaly_by_second = _series_to_timestamp_map(anomaly_payload)

    truth_flags = []
    predicted_flags = []
    for record in ground_truth:
        second = _record_epoch(record)
        truth_flags.append(_positive_event(record))
        predicted_flags.append(anomaly_by_second.get(second, 0.0) >= 1.0)

    classification = _classification_metrics(truth_flags, predicted_flags) if ground_truth else {}
    latency = _latency_metrics(ground_truth, anomaly_by_second) if ground_truth else {}

    exporter_cpu = _interval_values(result["queries"].get("exporter_cpu_rate", {"data": {"result": []}}))
    exporter_memory = _interval_values(result["queries"].get("exporter_memory", {"data": {"result": []}}))
    analytics_cpu = _interval_values(result["queries"].get("analytics_cpu_rate", {"data": {"result": []}}))
    analytics_memory = _interval_values(result["queries"].get("analytics_memory", {"data": {"result": []}}))

    summary = {
        "name": result["name"],
        "repetition": result.get("repetition", 1),
        "ground_truth_file": str(ground_truth_path),
        **classification,
        **latency,
        "avg_exporter_cpu_rate": mean(exporter_cpu) if exporter_cpu else 0.0,
        "avg_exporter_memory_bytes": mean(exporter_memory) if exporter_memory else 0.0,
        "avg_analytics_cpu_rate": mean(analytics_cpu) if analytics_cpu else 0.0,
        "avg_analytics_memory_bytes": mean(analytics_memory) if analytics_memory else 0.0,
    }
    return summary


def aggregate_summaries(summaries: Iterable[Dict]) -> Dict:
    rows = list(summaries)
    if not rows:
        return {}

    numeric_keys = [key for key, value in rows[0].items() if isinstance(value, (int, float)) and not isinstance(value, bool)]
    aggregated = {"run_count": len(rows)}
    for key in numeric_keys:
        series = [float(row[key]) for row in rows if row.get(key) is not None and not math.isnan(float(row[key]))]
        if not series:
            continue
        aggregated[f"{key}_mean"] = mean(series)
        aggregated[f"{key}_std"] = pstdev(series) if len(series) > 1 else 0.0
    return aggregated


def write_summary_csv(summaries: Iterable[Dict], output_path: Path) -> None:
    rows = list(summaries)
    if not rows:
        return
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
