import sys
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "apps" / "analytics"))

from analytics.correlation import infer_root_cause
from analytics.detectors.mad import RollingMADDetector
from analytics.detectors.rules import RuleDetector
from analytics.detectors.zscore import RollingZScoreDetector


class DetectorTest(unittest.TestCase):
    def test_rule_detector_flags_high_temperature(self) -> None:
        detector = RuleDetector([{"pattern": "temperature", "warning_high": 70.0, "critical_high": 80.0}])
        outcome = detector.evaluate("spindle_temperature", 82.5)
        self.assertTrue(outcome.flag)
        self.assertEqual(outcome.severity, "critical")

    def test_zscore_detector_flags_outlier(self) -> None:
        detector = RollingZScoreDetector(window_size=10, threshold=2.0, min_history=5)
        for value in [10.0, 10.1, 10.2, 9.9, 10.0]:
            detector.observe("signal", value)
        outcome = detector.observe("signal", 15.0)
        self.assertTrue(outcome.flag)
        self.assertGreater(outcome.score, 2.0)

    def test_correlation_prefers_mixed_when_ot_and_it_are_degraded(self) -> None:
        outcome = infer_root_cause(
            ot_vote_ratio=0.8,
            exporter_up=True,
            cpu_rate=0.8,
            memory_bytes=400_000_000,
            scrape_success=1.0,
            scrape_duration=0.2,
        )
        self.assertEqual(outcome.hint, "mixed")
        self.assertLess(outcome.confidence, 0.8)

    def test_mad_detector_flags_outlier(self) -> None:
        detector = RollingMADDetector(window_size=10, threshold=3.0, min_history=5)
        for value in [10.0, 10.2, 9.8, 10.1, 10.0]:
            detector.observe("signal", value)
        outcome = detector.observe("signal", 14.0)
        self.assertTrue(outcome.flag)
        self.assertGreater(outcome.score, 3.0)


if __name__ == "__main__":
    unittest.main()
