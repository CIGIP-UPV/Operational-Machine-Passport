import sys
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "exporter"))

from opcua_exporter.mapper import MappingProfile
from opcua_exporter.models import NodeSample


class MappingProfileTest(unittest.TestCase):
    def test_temperature_signal_maps_to_sensor_metric(self) -> None:
        config = {
            "asset_type": "cnc",
            "rules": [
                {
                    "pattern": "Temperature",
                    "category": "sensor",
                    "signal": "spindle_temperature",
                    "subsystem": "spindle",
                    "unit": "celsius",
                    "criticality": "high",
                }
            ],
        }
        profile = MappingProfile(config)
        sample = NodeSample(
            browse_name="SpindleTemperature",
            namespace=2,
            nodeid="ns=2;s=Sensors/SpindleTemperature",
            path="Objects/CNC_Machine_01/Sensors/SpindleTemperature",
            value=67.2,
        )

        metadata = profile.map_sample(sample)
        self.assertEqual(metadata.metric_name, "asset_sensor_value")
        self.assertEqual(metadata.signal, "spindle_temperature")
        self.assertEqual(metadata.subsystem, "spindle")


if __name__ == "__main__":
    unittest.main()
