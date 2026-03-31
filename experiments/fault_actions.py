import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, List


ROOT = Path(__file__).resolve().parent.parent


def _run_command(args: List[str], env: Dict[str, str]) -> None:
    subprocess.run(args, cwd=ROOT, env=env, check=True)


def _pause_service(service: str, duration_seconds: int, env: Dict[str, str]) -> None:
    _run_command(["docker", "compose", "pause", service], env)
    time.sleep(duration_seconds)
    _run_command(["docker", "compose", "unpause", service], env)


def _restart_service(service: str, env: Dict[str, str]) -> None:
    _run_command(["docker", "compose", "restart", service], env)


def schedule_fault_actions(actions: Iterable[Dict], env: Dict[str, str]) -> threading.Thread | None:
    normalized = sorted(actions, key=lambda action: int(action.get("at_second", 0)))
    if not normalized:
        return None

    def worker() -> None:
        base = time.time()
        for action in normalized:
            scheduled = base + int(action.get("at_second", 0))
            remaining = scheduled - time.time()
            if remaining > 0:
                time.sleep(remaining)
            action_type = action["action"]
            service = action["target"]
            if action_type == "restart_service":
                _restart_service(service, env)
            elif action_type == "pause_service":
                _pause_service(service, int(action.get("duration_seconds", 10)), env)
            else:
                raise ValueError(f"Unsupported fault action: {action_type}")

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread
