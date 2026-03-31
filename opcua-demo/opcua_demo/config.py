import json
import os
from pathlib import Path
from typing import Any, Dict


DEFAULT_SCENARIO_PATH = Path(__file__).resolve().parent.parent / "config" / "scenarios" / "cnc_baseline.json"


def load_scenario_config() -> Dict[str, Any]:
    raw_path = os.getenv("OPCUA_SCENARIO_FILE")
    scenario_path = Path(raw_path) if raw_path else DEFAULT_SCENARIO_PATH
    with scenario_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
