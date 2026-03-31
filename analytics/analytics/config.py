import json
import os
from pathlib import Path
from typing import Any, Dict


DEFAULT_RULES_PATH = Path(__file__).resolve().parent.parent / "config" / "rules.json"


def load_rules_config() -> Dict[str, Any]:
    raw_path = os.getenv("ANALYTICS_RULES_FILE")
    rules_path = Path(raw_path) if raw_path else DEFAULT_RULES_PATH
    with rules_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
