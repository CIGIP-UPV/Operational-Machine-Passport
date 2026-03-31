import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments"))

from analysis import summarize_run


class AnalysisTest(unittest.TestCase):
    def test_summarize_run_computes_basic_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            ground_truth_path = tmp_path / "ground_truth.jsonl"
            ts0 = int(datetime(2026, 3, 20, 8, 0, 0, tzinfo=timezone.utc).timestamp())
            ts1 = int(datetime(2026, 3, 20, 8, 0, 1, tzinfo=timezone.utc).timestamp())
            ts2 = int(datetime(2026, 3, 20, 8, 0, 2, tzinfo=timezone.utc).timestamp())
            ground_truth_records = [
                {"timestamp": "2026-03-20T08:00:00+00:00", "elapsed_seconds": 0, "asset_id": "cnc-01", "asset_type": "cnc", "event_label": "stable", "mode": "nominal"},
                {"timestamp": "2026-03-20T08:00:01+00:00", "elapsed_seconds": 1, "asset_id": "cnc-01", "asset_type": "cnc", "event_label": "fault", "mode": "tool_wear"},
                {"timestamp": "2026-03-20T08:00:02+00:00", "elapsed_seconds": 2, "asset_id": "cnc-01", "asset_type": "cnc", "event_label": "fault", "mode": "tool_wear"}
            ]
            ground_truth_path.write_text("\n".join(json.dumps(item) for item in ground_truth_records) + "\n", encoding="utf-8")

            result_path = tmp_path / "result.json"
            result_payload = {
                "name": "demo",
                "repetition": 1,
                "ground_truth_file": str(ground_truth_path),
                "queries": {
                    "detector_flags": {
                        "data": {
                            "result": [
                                {
                                    "values": [
                                        [ts0, "0"],
                                        [ts1, "1"],
                                        [ts2, "1"]
                                    ]
                                }
                            ]
                        }
                    },
                    "exporter_cpu_rate": {"data": {"result": [{"values": [[ts1, "0.1"], [ts2, "0.2"]]}]}},
                    "exporter_memory": {"data": {"result": [{"values": [[ts1, "100"], [ts2, "120"]]}]}},
                    "analytics_cpu_rate": {"data": {"result": [{"values": [[ts1, "0.05"], [ts2, "0.08"]]}]}},
                    "analytics_memory": {"data": {"result": [{"values": [[ts1, "80"], [ts2, "90"]]}]}}
                }
            }
            result_path.write_text(json.dumps(result_payload), encoding="utf-8")

            summary = summarize_run(result_path)
            self.assertEqual(summary["tp"], 2)
            self.assertEqual(summary["fp"], 0)
            self.assertEqual(summary["fn"], 0)
            self.assertGreaterEqual(summary["precision"], 1.0)


if __name__ == "__main__":
    unittest.main()
