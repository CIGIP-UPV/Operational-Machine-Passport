import json
import os
from pathlib import Path
from typing import Any, Dict


PROFILE_DIR = Path(__file__).resolve().parent / "config" / "profiles"
DEFAULT_PROFILE_PATH = PROFILE_DIR / "generic.json"


def load_profile_config(profile_id: str = "", raw_path: str = "") -> Dict[str, Any]:
    env_path = raw_path or os.getenv("OPCUA_PROFILE_PATH", "")
    if env_path:
        profile_path = Path(env_path)
    elif profile_id:
        profile_path = PROFILE_DIR / f"{profile_id}.json"
    else:
        profile_path = DEFAULT_PROFILE_PATH
    if not profile_path.exists():
        profile_path = DEFAULT_PROFILE_PATH
    with profile_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
