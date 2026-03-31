import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class ConfigTest(unittest.TestCase):
    def test_cnc_scenario_contains_events_and_signals(self) -> None:
        scenario_path = ROOT / "opcua-demo" / "config" / "scenarios" / "cnc_baseline.json"
        with scenario_path.open("r", encoding="utf-8") as handle:
            scenario = json.load(handle)
        self.assertIn("signals", scenario)
        self.assertIn("events", scenario)
        self.assertGreaterEqual(len(scenario["events"]), 3)
        self.assertIn("spindle_temperature", scenario["signals"])

    def test_robot_scenario_exists_for_generalization(self) -> None:
        scenario_path = ROOT / "opcua-demo" / "config" / "scenarios" / "robot_arm_baseline.json"
        with scenario_path.open("r", encoding="utf-8") as handle:
            scenario = json.load(handle)
        self.assertEqual(scenario["asset"]["type"], "robot_arm")
        self.assertIn("joint_temperature", scenario["signals"])

    def test_secondary_cnc_scenario_exists_for_multi_machine_setup(self) -> None:
        scenario_path = ROOT / "opcua-demo" / "config" / "scenarios" / "cnc_secondary.json"
        with scenario_path.open("r", encoding="utf-8") as handle:
            scenario = json.load(handle)
        self.assertEqual(scenario["asset"]["id"], "cnc-02")
        self.assertEqual(scenario["asset"]["type"], "cnc")
        self.assertIn("spindle_temperature", scenario["signals"])

    def test_mqtt_cnc_scenario_exists_for_multi_protocol_setup(self) -> None:
        scenario_path = ROOT / "mqtt-demo" / "config" / "scenarios" / "cnc_mqtt_baseline.json"
        with scenario_path.open("r", encoding="utf-8") as handle:
            scenario = json.load(handle)
        self.assertEqual(scenario["asset"]["id"], "cnc-mqtt-01")
        self.assertEqual(scenario["asset"]["type"], "cnc")
        self.assertEqual(scenario["asset"]["topic_root"], "factory/cnc-mqtt-01")
        self.assertIn("spindle_temperature", scenario["signals"])


if __name__ == "__main__":
    unittest.main()
