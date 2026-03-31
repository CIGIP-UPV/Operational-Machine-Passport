import argparse
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable

from analysis import aggregate_summaries, summarize_run, write_summary_csv
from fault_actions import schedule_fault_actions


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCENARIO = ROOT / "experiments" / "scenarios" / "cnc_cross_layer.json"
RESULTS_DIR = ROOT / "experiments" / "results"
DEFAULT_QUERIES = {
    "detector_flags": "asset_detector_flag",
    "anomaly_scores": "asset_anomaly_score",
    "monitoring_confidence": "asset_monitoring_confidence",
    "root_cause_state": "asset_root_cause_state",
    "exporter_cpu_rate": 'rate(process_cpu_seconds_total{job="opcua_exporter"}[1m])',
    "exporter_memory": 'process_resident_memory_bytes{job="opcua_exporter"}',
    "analytics_cpu_rate": 'rate(process_cpu_seconds_total{job="analytics"}[1m])',
    "analytics_memory": 'process_resident_memory_bytes{job="analytics"}',
    "exporter_scrape_success": "asset_exporter_scrape_success",
    "exporter_scrape_duration": "asset_exporter_scrape_duration_seconds"
}


def _load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _wait_for_ready(base_url: str, timeout_seconds: int = 90) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/-/ready", timeout=5) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(2)
    raise TimeoutError("Prometheus was not ready before timeout.")


def _query_range(base_url: str, expression: str, start: int, end: int, step_seconds: int):
    params = urllib.parse.urlencode(
        {
            "query": expression,
            "start": start,
            "end": end,
            "step": step_seconds,
        }
    )
    with urllib.request.urlopen(f"{base_url}/api/v1/query_range?{params}", timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _run_compose(command, env: Dict[str, str]) -> None:
    subprocess.run(["docker", "compose", *command], cwd=ROOT, env=env, check=True)


def _host_ground_truth_path(run_name: str, repetition: int, timestamp: str) -> Path:
    return RESULTS_DIR / f"{run_name}_{timestamp}_run{repetition}_ground_truth.jsonl"


def _compose_queries(extra_queries: Dict[str, str]) -> Dict[str, str]:
    queries = dict(DEFAULT_QUERIES)
    queries.update(extra_queries)
    return queries


def _run_single_repetition(
    scenario: Dict,
    repetition: int,
    timestamp: str,
    prometheus_url: str,
    keep_running: bool,
) -> Path:
    env = os.environ.copy()
    env.update(scenario.get("env", {}))

    run_name = scenario["name"]
    ground_truth_host_path = _host_ground_truth_path(run_name, repetition, timestamp)
    env["GROUND_TRUTH_PATH"] = f"/data/{ground_truth_host_path.name}"

    _run_compose(["up", "-d", "--build"], env)
    try:
        _wait_for_ready(prometheus_url)
        warmup = int(scenario.get("warmup_seconds", 20))
        duration = int(scenario.get("duration_seconds", 120))
        time.sleep(warmup)
        started_at = int(time.time())
        scheduler = schedule_fault_actions(scenario.get("fault_actions", []), env)
        time.sleep(duration)
        if scheduler is not None:
            scheduler.join(timeout=duration + 5)
        ended_at = int(time.time())

        run_output_path = RESULTS_DIR / f"{run_name}_{timestamp}_run{repetition}.json"
        results = {
            "name": run_name,
            "repetition": repetition,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration,
            "ground_truth_file": str(ground_truth_host_path),
            "fault_actions": scenario.get("fault_actions", []),
            "queries": {},
        }

        for name, expression in _compose_queries(scenario.get("queries", {})).items():
            results["queries"][name] = _query_range(
                prometheus_url,
                expression,
                started_at,
                ended_at,
                int(scenario.get("range_step_seconds", 5)),
            )

        with run_output_path.open("w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2)

        return run_output_path
    finally:
        if not keep_running:
            _run_compose(["down"], env)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an experimental campaign for the monitoring stack.")
    parser.add_argument("--scenario", default=str(DEFAULT_SCENARIO), help="Path to the experiment scenario JSON file.")
    parser.add_argument("--prometheus-url", default="http://localhost:9093", help="Prometheus URL exposed through Caddy.")
    parser.add_argument("--keep-running", action="store_true", help="Keep the Docker stack running after the experiment.")
    args = parser.parse_args()

    scenario_path = Path(args.scenario)
    scenario = _load_json(scenario_path)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    repetitions = int(scenario.get("repetitions", 1))
    result_paths = [
        _run_single_repetition(
            scenario=scenario,
            repetition=repetition,
            timestamp=timestamp,
            prometheus_url=args.prometheus_url,
            keep_running=args.keep_running,
        )
        for repetition in range(1, repetitions + 1)
    ]

    summaries = [summarize_run(path) for path in result_paths]
    aggregate = aggregate_summaries(summaries)
    write_summary_csv(summaries, RESULTS_DIR / f"{scenario['name']}_{timestamp}_summary.csv")
    summary_path = RESULTS_DIR / f"{scenario['name']}_{timestamp}_summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "scenario": scenario["name"],
                "runs": [str(path) for path in result_paths],
                "summaries": summaries,
                "aggregate": aggregate,
            },
            handle,
            indent=2,
        )

    print(summary_path)


if __name__ == "__main__":
    main()
