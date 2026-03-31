import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional

from prometheus_client import CONTENT_TYPE_LATEST, Gauge, REGISTRY, generate_latest

from .config import load_rules_config
from .correlation import ROOT_CAUSES, infer_root_cause
from .discovery_service import load_profile_config as load_discovery_profile_config
from .discovery_service import run_discovery, run_test_connection
from .detectors.mad import RollingMADDetector
from .detectors.rules import RuleDetector, RuleOutcome
from .detectors.zscore import RollingZScoreDetector
from .passport import build_passport
from .repository import AssetRepository


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("analytics")


PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
ANALYTICS_PORT = int(os.getenv("ANALYTICS_PORT", "9700"))
ANALYTICS_INTERVAL_SECONDS = float(os.getenv("ANALYTICS_INTERVAL_SECONDS", "5"))
EXPORTER_JOB_NAME = os.getenv("EXPORTER_JOB_NAME", "opcua_exporter")
GROUND_TRUTH_FILE = Path(os.getenv("GROUND_TRUTH_FILE", "/data/ground_truth.jsonl"))
MAX_SIGNAL_HISTORY = int(os.getenv("ANALYTICS_SIGNAL_HISTORY", "240"))
MAX_ASSET_HISTORY = int(os.getenv("ANALYTICS_ASSET_HISTORY", "240"))
ASSET_DB_PATH = Path(os.getenv("ASSET_DB_PATH", "/data/assets.db"))


ANOMALY_SCORE = Gauge(
    "asset_anomaly_score",
    "Anomaly score per asset signal and detector.",
    ["asset_id", "asset_type", "signal", "detector"],
)
DETECTOR_FLAG = Gauge(
    "asset_detector_flag",
    "Binary anomaly flag per detector.",
    ["asset_id", "asset_type", "signal", "detector"],
)
DETECTOR_VOTES = Gauge(
    "asset_detector_vote_total",
    "Total number of positive detector votes for a signal.",
    ["asset_id", "asset_type", "signal"],
)
MONITORING_CONFIDENCE = Gauge(
    "asset_monitoring_confidence",
    "Confidence on the monitoring pipeline for the asset.",
    ["asset_id", "asset_type"],
)
ROOT_CAUSE_STATE = Gauge(
    "asset_root_cause_state",
    "One-hot root cause state for the asset.",
    ["asset_id", "asset_type", "hint"],
)
ANALYTICS_CYCLE_SUCCESS = Gauge(
    "asset_analytics_cycle_success",
    "Whether the last analytics cycle completed successfully.",
)

SIGNAL_SELECTOR = '{__name__=~"asset_(signal|sensor|status|production|energy|maintenance|alarm)_value"}'


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _round(value: float, digits: int = 3) -> float:
    return round(float(value), digits)


def _query(expression: str) -> List[Dict]:
    endpoint = f"{PROMETHEUS_URL}/api/v1/query?{urllib.parse.urlencode({'query': expression})}"
    with urllib.request.urlopen(endpoint, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload["data"]["result"]


def _query_range(expression: str, start: int, end: int, step_seconds: int) -> List[Dict]:
    params = urllib.parse.urlencode(
        {
            "query": expression,
            "start": start,
            "end": end,
            "step": step_seconds,
        }
    )
    endpoint = f"{PROMETHEUS_URL}/api/v1/query_range?{params}"
    with urllib.request.urlopen(endpoint, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload["data"]["result"]


def _first_value(expression: str, default: float = 0.0) -> float:
    results = _query(expression)
    if not results:
        return default
    return float(results[0]["value"][1])


def _label_value_map(expression: str, label: str = "asset_id") -> Dict[str, float]:
    values = {}
    for result in _query(expression):
        metric = result.get("metric", {})
        key = metric.get(label)
        if not key:
            continue
        values[key] = float(result["value"][1])
    return values


def _asset_signal_samples() -> Iterable[Dict]:
    return _query(SIGNAL_SELECTOR)


def _category_from_metric(metric_name: str) -> str:
    if metric_name.startswith("asset_") and metric_name.endswith("_value"):
        return metric_name[6:-6]
    return "signal"


def _severity_from_votes(rule_outcome: RuleOutcome, votes: int) -> str:
    if rule_outcome.severity == "critical" or votes >= 2:
        return "critical"
    if rule_outcome.flag or votes == 1:
        return "warning"
    return "nominal"


def _status_from_root_cause(root_cause: str, active_anomalies: int) -> str:
    if root_cause in {"mixed", "observability_outage"}:
        return "critical"
    if root_cause in {"asset_fault", "observability_degradation"} or active_anomalies > 0:
        return "warning"
    return "nominal"


def _summary_for_root_cause(root_cause: str, asset_id: str, evidences: List[Dict], confidence: float) -> str:
    evidence_text = ", ".join(item["label"] for item in evidences[:3]) if evidences else "no active evidences"
    if root_cause == "asset_fault":
        return f"Asset {asset_id} shows process-side anomalies with confidence {_round(confidence, 2)} based on {evidence_text}."
    if root_cause == "observability_degradation":
        return f"Monitoring pipeline degradation is likely for {asset_id}; the strongest indicators are {evidence_text}."
    if root_cause == "mixed":
        return f"Both asset behaviour and observability health look degraded for {asset_id}; current evidence includes {evidence_text}."
    if root_cause == "observability_outage":
        return f"The observability path for {asset_id} is not reliable right now; latest indicators are {evidence_text}."
    return f"{asset_id} is operating in nominal conditions with confidence {_round(confidence, 2)}."


def _collection_mode_for(connection_type: str) -> str:
    return "subscription" if connection_type == "mqtt" else "scrape"


def _continuity_label_for(connection_type: str) -> str:
    return "message continuity" if connection_type == "mqtt" else "sample continuity"


def _continuity_score(scrape_success: float, scrape_duration: float, connection_type: str) -> float:
    threshold = ANALYTICS_INTERVAL_SECONDS * 1.2 if connection_type == "mqtt" else 2.0
    duration_penalty = max(0.0, scrape_duration - threshold) * 15.0
    return _round(max(0.0, min(100.0, scrape_success * 100.0 - duration_penalty)), 1)


def _connector_health_state(exporter_reachable: bool, scrape_success: float, scrape_duration: float, continuity_score: float) -> str:
    if not exporter_reachable or scrape_success <= 0:
        return "outage"
    if scrape_duration >= 2.0 or continuity_score < 45.0:
        return "degraded"
    return "healthy"


def _connection_context(asset: Optional[Dict], observability: Optional[Dict] = None) -> Dict:
    asset = asset or {}
    observability = observability or {}
    primary_connection = asset.get("primary_connection") or {}
    config = primary_connection.get("config") or {}
    connection_type = (
        observability.get("connector_type")
        or primary_connection.get("connection_type")
        or asset.get("primary_connection_type")
        or "unknown"
    )
    collection_mode = observability.get("collection_mode") or _collection_mode_for(connection_type)
    continuity_label = observability.get("continuity_label") or _continuity_label_for(connection_type)
    endpoint = primary_connection.get("endpoint_or_host") or asset.get("opcua_endpoint") or ""
    return {
        "connection_type": connection_type,
        "endpoint_or_host": endpoint,
        "broker_url": config.get("broker_url") or (endpoint if connection_type == "mqtt" else ""),
        "topic_root": config.get("topic_root", ""),
        "client_id": config.get("client_id", ""),
        "collection_mode": collection_mode,
        "connector_status": observability.get("connector_status") or asset.get("connection_status") or "unknown",
        "connector_health": observability.get("connector_health") or "unknown",
        "continuity_score": observability.get("continuity_score"),
        "continuity_label": continuity_label,
        "last_seen_at": observability.get("last_seen_at") or asset.get("last_seen_at"),
        "freshness_seconds": observability.get("freshness_seconds"),
    }


def _tail_ground_truth(asset_id: str, limit: int = 120) -> List[Dict]:
    source_files = []
    if GROUND_TRUTH_FILE.exists():
        source_files.append(GROUND_TRUTH_FILE)
    source_files.extend(
        path
        for path in sorted(GROUND_TRUTH_FILE.parent.glob("ground_truth*.jsonl"))
        if path not in source_files
    )
    if not source_files:
        return []

    records: Deque[Dict] = deque(maxlen=limit)
    for source_file in source_files:
        with source_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if asset_id and record.get("asset_id") != asset_id:
                    continue
                records.append(
                    {
                        "timestamp": record.get("timestamp"),
                        "elapsed_seconds": record.get("elapsed_seconds", 0),
                        "event_label": record.get("event_label", "nominal"),
                        "mode": record.get("mode", "nominal"),
                        "signals": record.get("signals", {}),
                    }
                )
    return list(records)


class DashboardStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshot: Dict = {"generated_at": None, "pipeline": {}, "assets": []}
        self._asset_history: Dict[str, Deque[Dict]] = defaultdict(lambda: deque(maxlen=MAX_ASSET_HISTORY))
        self._signal_history: Dict[str, Dict[str, Deque[float]]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=MAX_SIGNAL_HISTORY)))

    def update(self, snapshot: Dict, history_entries: Dict[str, Dict], signal_entries: Dict[str, Dict[str, float]]) -> None:
        with self._lock:
            self._snapshot = snapshot
            for asset_id, entry in history_entries.items():
                self._asset_history[asset_id].append(entry)
            for asset_id, entries in signal_entries.items():
                for signal_key, value in entries.items():
                    self._signal_history[asset_id][signal_key].append(float(value))

    def snapshot(self) -> Dict:
        with self._lock:
            return json.loads(json.dumps(self._snapshot))

    def asset_history(self, asset_id: str, limit: int = 120) -> List[Dict]:
        with self._lock:
            history = list(self._asset_history.get(asset_id, []))
        return history[-limit:]

    def asset_snapshot(self, asset_id: Optional[str]) -> Optional[Dict]:
        snapshot = self.snapshot()
        assets = snapshot.get("assets", [])
        if not assets:
            return None
        if asset_id:
            for asset in assets:
                if asset["asset_id"] == asset_id:
                    return asset
            return None
        return assets[0]

    def signal_trend_preview(self, asset_id: str, signal_key: str, current_value: float, limit: int = 12) -> List[float]:
        with self._lock:
            history = list(self._signal_history.get(asset_id, {}).get(signal_key, []))
        return [*_trim_sequence(history, limit - 1), _round(current_value, 4)]

    def asset_confidence_trend_preview(self, asset_id: str, current_value: float, limit: int = 12) -> List[float]:
        with self._lock:
            history = [float(item.get("monitoring_confidence", 0.0)) * 100 for item in self._asset_history.get(asset_id, [])]
        return [*_trim_sequence(history, limit - 1), _round(current_value * 100, 2)]


STORE = DashboardStore()
REPOSITORY = AssetRepository(ASSET_DB_PATH)


def _trim_sequence(values: List[float], limit: int) -> List[float]:
    if limit <= 0:
        return []
    return [float(value) for value in values[-limit:]]


def _ensure_example_assets() -> None:
    examples = [
        {
            "asset_id": "cnc-01",
            "display_name": "CNC Machine 01",
            "asset_type": "cnc",
            "manufacturer": "Example CNC",
            "model": "Twin-Spindle A",
            "serial_number": "CNC-01-DEMO",
            "location": "Cell A",
            "description": "Primary CNC example registered by default for OPC UA monitoring demos.",
            "opcua_endpoint": "opc.tcp://opcua-simulator:4840/freeopcua/assets/",
            "profile_id": "cnc",
            "status": "active",
            "connection_status": "unknown",
            "manufacture_date": "2024-09-15",
            "country_of_origin": "ES",
            "rated_power_kw": 18.5,
            "interfaces": ["opcua", "ethernet-ip"],
        },
        {
            "asset_id": "cnc-02",
            "display_name": "CNC Machine 02",
            "asset_type": "cnc",
            "manufacturer": "Example CNC",
            "model": "Twin-Spindle B",
            "serial_number": "CNC-02-DEMO",
            "location": "Cell B",
            "description": "Secondary CNC example pointing to the second OPC UA simulator endpoint.",
            "opcua_endpoint": "opc.tcp://opcua-simulator-cnc-02:4840/freeopcua/assets/",
            "profile_id": "cnc",
            "status": "active",
            "connection_status": "unknown",
            "manufacture_date": "2025-01-10",
            "country_of_origin": "ES",
            "rated_power_kw": 20.0,
            "interfaces": ["opcua", "mqtt"],
        },
        {
            "asset_id": "cnc-mqtt-01",
            "display_name": "CNC MQTT 01",
            "asset_type": "cnc",
            "manufacturer": "Example CNC",
            "model": "MQTT Gateway Twin",
            "serial_number": "CNC-MQTT-01-DEMO",
            "location": "Cell C",
            "description": "Built-in CNC example publishing through MQTT for interoperability demos.",
            "connection_type": "mqtt",
            "connection_config": {
                "broker_url": "mqtt://mqtt-broker:1883",
                "topic_root": "factory/cnc-mqtt-01",
                "qos": 0,
                "client_id": "opc-observe-cnc-mqtt-01",
            },
            "mqtt_broker_url": "mqtt://mqtt-broker:1883",
            "mqtt_topic_root": "factory/cnc-mqtt-01",
            "mqtt_qos": 0,
            "mqtt_client_id": "opc-observe-cnc-mqtt-01",
            "profile_id": "cnc",
            "status": "active",
            "connection_status": "unknown",
            "manufacture_date": "2025-02-18",
            "country_of_origin": "ES",
            "rated_power_kw": 17.0,
            "interfaces": ["mqtt", "opcua"],
        },
    ]

    for example in examples:
        existing = REPOSITORY.get_asset(example["asset_id"])
        if existing:
            continue
        REPOSITORY.upsert_asset(example)


_ensure_example_assets()


def _backfill_legacy_connections() -> None:
    for asset in REPOSITORY.list_assets():
        if asset.get("primary_connection"):
            continue
        if asset.get("opcua_endpoint"):
            REPOSITORY.upsert_connection(
                asset["asset_id"],
                {
                    "connection_type": "opcua",
                    "endpoint_or_host": asset["opcua_endpoint"],
                    "config": {
                        "endpoint": asset["opcua_endpoint"],
                        "security_mode": asset.get("opcua_security_mode") or "none",
                        "username": asset.get("opcua_username") or "",
                    },
                    "status": asset.get("connection_status", "unknown"),
                    "last_connection_check_at": asset.get("last_connection_check_at"),
                    "last_seen_at": asset.get("last_seen_at"),
                    "is_primary": True,
                },
            )


_backfill_legacy_connections()


def _available_profiles() -> List[Dict]:
    profiles = []
    profile_dir = Path(__file__).resolve().parents[1] / "config" / "profiles"
    for profile_path in sorted(profile_dir.glob("*.json")):
        try:
            config = load_discovery_profile_config(profile_path.stem)
        except (OSError, json.JSONDecodeError):
            continue
        profiles.append(
            {
                "id": profile_path.stem,
                "asset_type": config.get("asset_type", profile_path.stem),
                "default_category": config.get("default_category", "signal"),
                "rule_count": len(config.get("rules", [])),
            }
        )
    return profiles


def _placeholder_signal_from_record(signal: Dict) -> Dict:
    value = signal.get("sample_value", 0.0) or 0.0
    return {
        "signal_key": signal["signal_key"],
        "signal": signal["signal"],
        "display_name": signal.get("display_name") or signal["signal"],
        "category": signal.get("category", "signal"),
        "value": _round(value, 4),
        "unit": signal.get("unit", "unknown"),
        "subsystem": signal.get("subsystem", "unknown"),
        "criticality": signal.get("criticality", "medium"),
        "path": signal.get("path", ""),
        "source_ref": signal.get("path", "") or signal.get("signal_key", ""),
        "severity": "nominal",
        "detector_vote_total": 0,
        "anomaly_score": 0.0,
        "trend": [_round(value, 4)],
        "detectors": {
            "rules": {"flag": False, "score": 0.0, "severity": "nominal"},
            "zscore": {"flag": False, "score": 0.0},
            "mad": {"flag": False, "score": 0.0},
        },
    }


def _placeholder_asset_from_registry(asset_id: str) -> Optional[Dict]:
    asset = REPOSITORY.get_asset(asset_id)
    if not asset:
        return None
    stored_signals = REPOSITORY.list_signals(asset_id)
    signal_payloads = [_placeholder_signal_from_record(signal) for signal in stored_signals]
    passport = REPOSITORY.get_passport(asset_id) or {}
    diagnostics = passport.get("diagnostics", {})
    observability = passport.get("observability", {})
    monitoring_confidence = diagnostics.get("monitoring_confidence", 0.0) / 100.0
    connection = _connection_context(asset, observability)
    return {
        "asset_id": asset["asset_id"],
        "display_name": asset.get("display_name") or asset["asset_id"],
        "asset_type": asset.get("asset_type", "generic"),
        "primary_connection_type": connection["connection_type"],
        "status": "warning" if asset.get("connection_status") not in {"connected", "monitored"} else "nominal",
        "kpis": {
            "active_anomalies": diagnostics.get("active_anomalies", 0),
            "detector_votes": 0,
            "signals_tracked": len(signal_payloads),
            "monitoring_confidence": _round(monitoring_confidence),
        },
        "diagnosis": {
            "root_cause": diagnostics.get("root_cause", "nominal"),
            "monitoring_confidence": _round(monitoring_confidence),
            "summary": diagnostics.get("summary", "This asset is registered but does not have live Prometheus-backed telemetry yet."),
            "evidences": [],
            "top_signal": diagnostics.get("top_signal"),
            "active_anomalies": diagnostics.get("active_anomalies", 0),
            "vote_ratio": 0.0,
        },
        "trend": [diagnostics.get("monitoring_confidence", 0.0)],
        "connection": connection,
        "observability": observability,
        "signals": signal_payloads,
        "registry": asset,
        "passport_summary": {
            "health_score": diagnostics.get("health_score", 0.0),
            "coverage_ratio": passport.get("semantic", {}).get("coverage_ratio", 0.0),
            "connection_status": asset.get("connection_status", "unknown"),
            "exporter_reachable": observability.get("exporter_reachable", False),
        },
    }


def _merge_asset_record(asset: Dict, live_asset: Optional[Dict], pipeline: Optional[Dict] = None) -> Dict:
    passport = REPOSITORY.get_passport(asset["asset_id"]) or {}
    diagnostics = passport.get("diagnostics", {})
    live_status = live_asset or _placeholder_asset_from_registry(asset["asset_id"]) or {}
    connection = live_status.get("connection") or _connection_context(asset, passport.get("observability", {}))
    return {
        "asset_id": asset["asset_id"],
        "display_name": asset.get("display_name") or asset["asset_id"],
        "asset_type": asset.get("asset_type", "generic"),
        "manufacturer": asset.get("manufacturer"),
        "model": asset.get("model"),
        "serial_number": asset.get("serial_number"),
        "location": asset.get("location"),
        "description": asset.get("description"),
        "nameplate": asset.get("nameplate", {}),
        "connections": asset.get("connections", []),
        "primary_connection": asset.get("primary_connection"),
        "primary_connection_type": (asset.get("primary_connection") or {}).get("connection_type"),
        "opcua_endpoint": asset.get("opcua_endpoint"),
        "profile_id": asset.get("profile_id", "generic"),
        "status": asset.get("status", "draft"),
        "connection_status": asset.get("connection_status", "unknown"),
        "last_seen_at": asset.get("last_seen_at"),
        "last_discovered_at": asset.get("last_discovered_at"),
        "connection": connection,
        "live": {
            "available": bool(live_asset),
            "active_anomalies": live_status.get("kpis", {}).get("active_anomalies", diagnostics.get("active_anomalies", 0)),
            "monitoring_confidence": live_status.get("diagnosis", {}).get("monitoring_confidence", diagnostics.get("monitoring_confidence", 0.0)),
            "root_cause": live_status.get("diagnosis", {}).get("root_cause", diagnostics.get("root_cause", "nominal")),
            "signals_tracked": live_status.get("kpis", {}).get("signals_tracked", passport.get("semantic", {}).get("signal_count", 0)),
        },
        "passport_summary": {
            "health_score": diagnostics.get("health_score", 0.0),
            "coverage_ratio": passport.get("semantic", {}).get("coverage_ratio", 0.0),
            "connection_status": asset.get("connection_status", "unknown"),
            "exporter_reachable": passport.get("observability", {}).get("exporter_reachable", False),
        },
        "tags": asset.get("tags", []),
        "pipeline": pipeline or {},
    }


def _refresh_passport(asset_id: str, live_asset: Optional[Dict], pipeline: Dict) -> Optional[Dict]:
    asset = REPOSITORY.get_asset(asset_id)
    if not asset:
        return None
    nameplate = REPOSITORY.get_nameplate(asset_id)
    stored_signals = REPOSITORY.list_signals(asset_id)
    signal_mappings = REPOSITORY.list_signal_mappings(asset_id)
    stored_nodes = REPOSITORY.list_nodes(asset_id)
    events = REPOSITORY.list_events(asset_id, limit=20)
    notes = REPOSITORY.list_notes(asset_id, limit=20)
    maintenance_events = REPOSITORY.list_maintenance_events(asset_id, limit=20)
    software_inventory = REPOSITORY.list_software_inventory(asset_id)
    components = REPOSITORY.list_components(asset_id)
    documents = REPOSITORY.list_documents(asset_id)
    compliance_certificates = REPOSITORY.list_compliance_certificates(asset_id)
    access_policy = REPOSITORY.get_access_policy(asset_id)
    integrity_record = REPOSITORY.get_integrity_record(asset_id)
    sustainability_record = REPOSITORY.get_sustainability_record(asset_id)
    ownership_events = REPOSITORY.list_ownership_events(asset_id, limit=20)
    passport = build_passport(
        asset,
        nameplate,
        stored_signals,
        signal_mappings,
        stored_nodes,
        live_asset,
        pipeline,
        events,
        notes,
        maintenance_events,
        software_inventory,
        components,
        documents,
        compliance_certificates,
        access_policy,
        integrity_record,
        sustainability_record,
        ownership_events,
    )
    baselines = passport.get("baseline", {}).get("signals", [])
    REPOSITORY.replace_baselines(asset_id, baselines)
    REPOSITORY.replace_passport(asset_id, passport)
    return passport


def _range_expression(asset_id: str, signal: str, path: str = "") -> str:
    matchers = [
        '__name__=~"asset_(signal|sensor|status|production|energy|maintenance|alarm)_value"',
        f'asset_id="{asset_id}"',
        f'signal="{signal}"',
    ]
    if path:
        matchers.append(f'path="{path}"')
    return "{" + ", ".join(matchers) + "}"


def _series_payload(asset_id: str, signal: str, path: str = "", minutes: int = 30, step_seconds: int = 15) -> Dict:
    end = int(time.time())
    start = end - int(minutes * 60)
    results = _query_range(_range_expression(asset_id, signal, path=path), start, end, step_seconds)
    points = []
    metadata = {}
    for result in results:
        if not metadata:
            metadata = {
                "metric_name": result.get("metric", {}).get("__name__", "asset_signal_value"),
                "subsystem": result.get("metric", {}).get("subsystem", "unknown"),
                "unit": result.get("metric", {}).get("unit", "unknown"),
                "criticality": result.get("metric", {}).get("criticality", "medium"),
            }
        for timestamp, value in result.get("values", []):
            points.append(
                {
                    "timestamp": datetime.fromtimestamp(float(timestamp), tz=timezone.utc).isoformat(),
                    "value": _round(float(value), 4),
                }
            )
    if not points:
        stored_signal = next(
            (
                item
                for item in REPOSITORY.list_signals(asset_id)
                if item.get("signal") == signal and (not path or item.get("path") == path)
            ),
            None,
        )
        if stored_signal:
            metadata = {
                "metric_name": f"asset_{stored_signal.get('category', 'signal')}_value",
                "subsystem": stored_signal.get("subsystem", "unknown"),
                "unit": stored_signal.get("unit", "unknown"),
                "criticality": stored_signal.get("criticality", "medium"),
            }
            sample_value = _round(float(stored_signal.get("sample_value") or 0.0), 4)
            now = _utc_now()
            points = [{"timestamp": now, "value": sample_value}]
    return {
        "asset_id": asset_id,
        "signal": signal,
        "path": path,
        "points": points,
        "minutes": minutes,
        "step_seconds": step_seconds,
        "metadata": metadata,
    }


def _timeline_payload(asset_id: str, limit: int = 80) -> Dict:
    history = STORE.asset_history(asset_id, limit=limit)
    ground_truth = _tail_ground_truth(asset_id, limit=limit)
    asset = REPOSITORY.get_asset(asset_id)
    passport = REPOSITORY.get_passport(asset_id) or {}
    live_asset = _current_live_assets_by_id().get(asset_id)
    connection = (live_asset or {}).get("connection") or _connection_context(asset, passport.get("observability", {}))
    first_fault = next((item for item in ground_truth if item.get("mode") != "nominal"), None)
    fault_ts = None
    if first_fault:
        try:
            fault_ts = datetime.fromisoformat(first_fault["timestamp"])
        except (TypeError, ValueError):
            fault_ts = None

    first_detection = None
    for item in history:
        detected = item.get("root_cause") not in {"nominal", "observability_degradation"} or item.get("active_anomalies", 0) > 0
        if not detected:
            continue
        if fault_ts is None:
            first_detection = item
            break
        try:
            detection_ts = datetime.fromisoformat(item["timestamp"])
        except (TypeError, ValueError):
            continue
        if detection_ts >= fault_ts:
            first_detection = item
            break

    detection_delay_seconds = None
    if fault_ts and first_detection:
        try:
            detect_ts = datetime.fromisoformat(first_detection["timestamp"])
            detection_delay_seconds = max(0.0, (detect_ts - fault_ts).total_seconds())
        except (TypeError, ValueError):
            detection_delay_seconds = None

    return {
        "asset_id": asset_id,
        "context": {
            "asset_type": (asset or {}).get("asset_type", "generic"),
            "connection_type": connection.get("connection_type", "unknown"),
            "collection_mode": connection.get("collection_mode"),
            "connector_status": connection.get("connector_status"),
            "connector_health": connection.get("connector_health"),
            "continuity_score": connection.get("continuity_score"),
            "continuity_label": connection.get("continuity_label"),
        },
        "ground_truth": ground_truth,
        "analytics": history,
        "summary": {
            "first_fault_at": first_fault["timestamp"] if first_fault else None,
            "first_detection_at": first_detection["timestamp"] if first_detection else None,
            "detection_delay_seconds": detection_delay_seconds,
        },
    }


def _current_live_assets_by_id() -> Dict[str, Dict]:
    snapshot = STORE.snapshot()
    return {asset["asset_id"]: asset for asset in snapshot.get("assets", [])}


def _state_payload(asset_id: Optional[str] = None) -> Dict:
    snapshot = STORE.snapshot()
    pipeline = snapshot.get("pipeline", {})
    live_assets = _current_live_assets_by_id()
    if asset_id:
        live_asset = live_assets.get(asset_id)
        if live_asset:
            registry_asset = REPOSITORY.get_asset(asset_id) or REPOSITORY.upsert_live_asset(live_asset)
            payload_asset = dict(live_asset)
            payload_asset["registry"] = registry_asset
            payload_asset["passport_summary"] = _merge_asset_record(registry_asset, live_asset, pipeline)["passport_summary"]
            return {
                "generated_at": snapshot.get("generated_at"),
                "pipeline": pipeline,
                "assets": [payload_asset],
            }

        placeholder = _placeholder_asset_from_registry(asset_id)
        return {
            "generated_at": snapshot.get("generated_at"),
            "pipeline": pipeline,
            "assets": [placeholder] if placeholder else [],
        }

    assets = []
    for live_asset in snapshot.get("assets", []):
        registry_asset = REPOSITORY.get_asset(live_asset["asset_id"]) or REPOSITORY.upsert_live_asset(live_asset)
        payload_asset = dict(live_asset)
        payload_asset["registry"] = registry_asset
        payload_asset["passport_summary"] = _merge_asset_record(registry_asset, live_asset, pipeline)["passport_summary"]
        assets.append(payload_asset)
    return {
        "generated_at": snapshot.get("generated_at"),
        "pipeline": pipeline,
        "assets": assets,
    }


def _assets_payload() -> Dict:
    snapshot = STORE.snapshot()
    pipeline = snapshot.get("pipeline", {})
    live_assets = _current_live_assets_by_id()
    assets = [_merge_asset_record(asset, live_assets.get(asset["asset_id"]), pipeline) for asset in REPOSITORY.list_assets()]
    return {
        "generated_at": snapshot.get("generated_at"),
        "profiles": _available_profiles(),
        "assets": assets,
    }


def _passport_payload(asset_id: str) -> Optional[Dict]:
    asset = REPOSITORY.get_asset(asset_id)
    if not asset:
        return None
    live_asset = _current_live_assets_by_id().get(asset_id)
    snapshot = STORE.snapshot()
    passport = _refresh_passport(asset_id, live_asset=live_asset, pipeline=snapshot.get("pipeline", {}))
    return {
        "generated_at": snapshot.get("generated_at"),
        "asset": _merge_asset_record(asset, live_asset, snapshot.get("pipeline", {})),
        "passport": passport,
        "events": REPOSITORY.list_events(asset_id, limit=25),
        "notes": REPOSITORY.list_notes(asset_id, limit=25),
        "baselines": REPOSITORY.list_baselines(asset_id),
        "signals": REPOSITORY.list_signals(asset_id),
        "mappings": REPOSITORY.list_signal_mappings(asset_id),
        "nodes": REPOSITORY.list_nodes(asset_id),
    }


def _json_response(handler: BaseHTTPRequestHandler, payload: Dict, status: int = 200) -> None:
    encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(encoded)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(encoded)


def _read_json_body(handler: BaseHTTPRequestHandler) -> Dict:
    content_length = int(handler.headers.get("Content-Length", "0"))
    if content_length <= 0:
        return {}
    raw = handler.rfile.read(content_length)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


class AnalyticsHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:  # pragma: no cover - keep logs compact
        LOGGER.debug("HTTP %s - %s", self.address_string(), format % args)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        path_parts = [part for part in parsed.path.split("/") if part]

        try:
            if parsed.path in {"/", "/health"}:
                _json_response(
                    self,
                    {
                        "status": "ok",
                        "generated_at": STORE.snapshot().get("generated_at"),
                    },
                )
                return

            if parsed.path == "/metrics":
                payload = generate_latest(REGISTRY)
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(payload)
                return

            if parsed.path == "/api/state":
                asset_id = params.get("asset_id", [None])[0]
                _json_response(self, _state_payload(asset_id))
                return

            if parsed.path == "/api/assets":
                _json_response(self, _assets_payload())
                return

            if parsed.path == "/api/diagnosis":
                asset_id = params.get("asset_id", [None])[0]
                asset = _state_payload(asset_id).get("assets", [None])[0]
                if not asset:
                    _json_response(self, {"error": "Asset not found."}, status=404)
                    return
                _json_response(self, {"generated_at": STORE.snapshot().get("generated_at"), "diagnosis": asset.get("diagnosis", {})})
                return

            if len(path_parts) >= 3 and path_parts[:2] == ["api", "assets"]:
                asset_id = path_parts[2]
                if len(path_parts) == 3:
                    asset = REPOSITORY.get_asset(asset_id)
                    if not asset:
                        _json_response(self, {"error": "Asset not found."}, status=404)
                        return
                    live_asset = _current_live_assets_by_id().get(asset_id)
                    _json_response(
                        self,
                        {
                            "generated_at": STORE.snapshot().get("generated_at"),
                            "asset": _merge_asset_record(asset, live_asset, STORE.snapshot().get("pipeline", {})),
                        },
                    )
                    return

                if len(path_parts) == 4 and path_parts[3] == "passport":
                    payload = _passport_payload(asset_id)
                    if not payload:
                        _json_response(self, {"error": "Asset not found."}, status=404)
                        return
                    _json_response(self, payload)
                    return

                if len(path_parts) == 4 and path_parts[3] == "events":
                    _json_response(self, {"asset_id": asset_id, "events": REPOSITORY.list_events(asset_id, limit=50)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "notes":
                    _json_response(self, {"asset_id": asset_id, "notes": REPOSITORY.list_notes(asset_id, limit=50)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "signals":
                    _json_response(self, {"asset_id": asset_id, "signals": REPOSITORY.list_signals(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "mappings":
                    _json_response(self, {"asset_id": asset_id, "mappings": REPOSITORY.list_signal_mappings(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "connections":
                    _json_response(self, {"asset_id": asset_id, "connections": REPOSITORY.list_connections(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "nodes":
                    _json_response(self, {"asset_id": asset_id, "nodes": REPOSITORY.list_nodes(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "components":
                    _json_response(self, {"asset_id": asset_id, "components": REPOSITORY.list_components(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "software":
                    _json_response(self, {"asset_id": asset_id, "software": REPOSITORY.list_software_inventory(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "maintenance":
                    _json_response(self, {"asset_id": asset_id, "maintenance": REPOSITORY.list_maintenance_events(asset_id, limit=50)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "documents":
                    _json_response(self, {"asset_id": asset_id, "documents": REPOSITORY.list_documents(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "compliance":
                    _json_response(self, {"asset_id": asset_id, "compliance": REPOSITORY.list_compliance_certificates(asset_id)})
                    return

                if len(path_parts) == 4 and path_parts[3] == "access":
                    _json_response(self, {"asset_id": asset_id, "access": REPOSITORY.get_access_policy(asset_id) or {}})
                    return

                if len(path_parts) == 4 and path_parts[3] == "integrity":
                    _json_response(self, {"asset_id": asset_id, "integrity": REPOSITORY.get_integrity_record(asset_id) or {}})
                    return

                if len(path_parts) == 4 and path_parts[3] == "sustainability":
                    _json_response(self, {"asset_id": asset_id, "sustainability": REPOSITORY.get_sustainability_record(asset_id) or {}})
                    return

                if len(path_parts) == 4 and path_parts[3] == "ownership":
                    _json_response(self, {"asset_id": asset_id, "ownership": REPOSITORY.list_ownership_events(asset_id, limit=50)})
                    return

            if parsed.path == "/api/timeline":
                asset_id = params.get("asset_id", [None])[0]
                if not asset_id:
                    _json_response(self, {"error": "asset_id is required."}, status=400)
                    return
                limit = int(params.get("limit", ["80"])[0])
                _json_response(self, _timeline_payload(asset_id, limit=limit))
                return

            if parsed.path == "/api/series":
                asset_id = params.get("asset_id", [None])[0]
                signal = params.get("signal", [None])[0]
                path = params.get("path", [""])[0]
                if not asset_id or not signal:
                    _json_response(self, {"error": "asset_id and signal are required."}, status=400)
                    return
                minutes = int(params.get("minutes", ["30"])[0])
                step_seconds = int(params.get("step_seconds", ["15"])[0])
                _json_response(self, _series_payload(asset_id, signal, path=path, minutes=minutes, step_seconds=step_seconds))
                return

            _json_response(self, {"error": "Not found."}, status=404)
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            LOGGER.warning("API request failed: %s", exc)
            _json_response(self, {"error": str(exc)}, status=502)
        except Exception as exc:  # pragma: no cover - server must stay alive
            LOGGER.exception("Unexpected API failure: %s", exc)
            _json_response(self, {"error": "Unexpected analytics API failure."}, status=500)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [part for part in parsed.path.split("/") if part]

        try:
            payload = _read_json_body(self)

            if parsed.path == "/api/assets":
                if not payload.get("asset_id"):
                    _json_response(self, {"error": "asset_id is required."}, status=400)
                    return
                asset = REPOSITORY.upsert_asset(payload)
                _refresh_passport(asset["asset_id"], _current_live_assets_by_id().get(asset["asset_id"]), STORE.snapshot().get("pipeline", {}))
                _json_response(self, {"asset": asset}, status=201)
                return

            if parsed.path == "/api/assets/test-connection":
                connection_type = payload.get("connection_type", "opcua")
                connection_config = payload.get("connection_config") or {}
                endpoint = (
                    payload.get("opcua_endpoint")
                    or payload.get("mqtt_broker_url")
                    or connection_config.get("endpoint")
                    or connection_config.get("broker_url")
                    or connection_config.get("endpoint_or_host")
                    or ""
                ).strip()
                if not endpoint:
                    _json_response(self, {"error": "A connection endpoint is required."}, status=400)
                    return
                if connection_type == "mqtt":
                    connection_config = {
                        **connection_config,
                        "topic_root": payload.get("mqtt_topic_root") or connection_config.get("topic_root", ""),
                        "qos": payload.get("mqtt_qos", connection_config.get("qos", 0)),
                        "client_id": payload.get("mqtt_client_id") or connection_config.get("client_id", ""),
                        "username": payload.get("mqtt_username") or connection_config.get("username", ""),
                        "password": payload.get("mqtt_password") or connection_config.get("password", ""),
                    }
                result = run_test_connection(endpoint, connection_type=connection_type, config=connection_config)
                _json_response(self, result)
                return

            if parsed.path == "/api/assets/discover":
                connection_type = payload.get("connection_type", "opcua")
                connection_config = payload.get("connection_config") or {}
                endpoint = (
                    payload.get("opcua_endpoint")
                    or payload.get("mqtt_broker_url")
                    or connection_config.get("endpoint")
                    or connection_config.get("broker_url")
                    or connection_config.get("endpoint_or_host")
                    or ""
                ).strip()
                if not endpoint:
                    _json_response(self, {"error": "A connection endpoint is required."}, status=400)
                    return
                profile_id = payload.get("profile_id", "generic")
                asset_type = payload.get("asset_type", "")
                if connection_type == "mqtt":
                    connection_config = {
                        **connection_config,
                        "topic_root": payload.get("mqtt_topic_root") or connection_config.get("topic_root", ""),
                        "qos": payload.get("mqtt_qos", connection_config.get("qos", 0)),
                        "client_id": payload.get("mqtt_client_id") or connection_config.get("client_id", ""),
                        "username": payload.get("mqtt_username") or connection_config.get("username", ""),
                        "password": payload.get("mqtt_password") or connection_config.get("password", ""),
                    }
                result = run_discovery(endpoint, profile_id=profile_id, asset_type_override=asset_type, connection_type=connection_type, config=connection_config)
                _json_response(self, result)
                return

            if len(path_parts) >= 4 and path_parts[:2] == ["api", "assets"]:
                asset_id = path_parts[2]
                asset = REPOSITORY.get_asset(asset_id)

                if len(path_parts) == 4 and path_parts[3] == "test-connection":
                    primary_connection = (asset or {}).get("primary_connection")
                    if not asset or not primary_connection:
                        _json_response(self, {"error": "Asset connection is not configured."}, status=400)
                        return
                    result = run_test_connection(
                        primary_connection["endpoint_or_host"],
                        connection_type=primary_connection.get("connection_type", "opcua"),
                        config=primary_connection.get("config", {}),
                    )
                    REPOSITORY.upsert_asset(
                        {
                            **asset,
                            "last_connection_check_at": result.get("checked_at"),
                            "connection_status": "connected" if result.get("reachable") else "error",
                        }
                    )
                    _json_response(self, result)
                    return

                if len(path_parts) == 4 and path_parts[3] == "discover":
                    primary_connection = (asset or {}).get("primary_connection")
                    if not asset or not primary_connection:
                        _json_response(self, {"error": "Asset connection is not configured."}, status=400)
                        return
                    result = run_discovery(
                        primary_connection["endpoint_or_host"],
                        profile_id=asset.get("profile_id", "generic"),
                        asset_type_override=asset.get("asset_type", ""),
                        connection_type=primary_connection.get("connection_type", "opcua"),
                        config=primary_connection.get("config", {}),
                    )
                    REPOSITORY.save_discovery(asset_id, result, profile_id=asset.get("profile_id", "generic"))
                    REPOSITORY.add_event(
                        asset_id,
                        "discovery",
                        "nominal",
                        "OPC UA discovery completed",
                        f"Discovered {result['signal_count']} mapped signals and {result['node_count']} nodes.",
                        payload={"profile_id": asset.get("profile_id", "generic")},
                    )
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"discovery": result, "passport": passport})
                    return

                if len(path_parts) == 5 and path_parts[3] == "passport" and path_parts[4] == "rebuild":
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    if not passport:
                        _json_response(self, {"error": "Asset not found."}, status=404)
                        return
                    _json_response(self, {"passport": passport})
                    return

                if len(path_parts) == 4 and path_parts[3] == "notes":
                    note = (payload.get("note") or "").strip()
                    if not note:
                        _json_response(self, {"error": "note is required."}, status=400)
                        return
                    created = REPOSITORY.add_note(asset_id, note=note, author=payload.get("author", "operator"))
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"note": created, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "components":
                    if not payload.get("component_id") or not payload.get("name"):
                        _json_response(self, {"error": "component_id and name are required."}, status=400)
                        return
                    created = REPOSITORY.add_component(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"component": created, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "software":
                    if not payload.get("software_id") or not payload.get("name") or not payload.get("version"):
                        _json_response(self, {"error": "software_id, name and version are required."}, status=400)
                        return
                    created = REPOSITORY.add_software_item(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"software": created, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "maintenance":
                    if not payload.get("action"):
                        _json_response(self, {"error": "action is required."}, status=400)
                        return
                    created = REPOSITORY.add_maintenance_event(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"maintenance_event": created, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "documents":
                    if not payload.get("document_type") or not payload.get("title") or not payload.get("ref"):
                        _json_response(self, {"error": "document_type, title and ref are required."}, status=400)
                        return
                    created = REPOSITORY.add_document(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"document": created, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "compliance":
                    if not payload.get("certificate_type") or not payload.get("title") or not payload.get("ref"):
                        _json_response(self, {"error": "certificate_type, title and ref are required."}, status=400)
                        return
                    created = REPOSITORY.add_compliance_certificate(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"certificate": created, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "access":
                    record = REPOSITORY.upsert_access_policy(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"access": record, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "integrity":
                    record = REPOSITORY.upsert_integrity_record(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"integrity": record, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "sustainability":
                    record = REPOSITORY.upsert_sustainability_record(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"sustainability": record, "passport": passport}, status=201)
                    return

                if len(path_parts) == 4 and path_parts[3] == "ownership":
                    if not payload.get("event_type") or not payload.get("owner_name"):
                        _json_response(self, {"error": "event_type and owner_name are required."}, status=400)
                        return
                    created = REPOSITORY.add_ownership_event(asset_id, payload)
                    passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                    _json_response(self, {"ownership_event": created, "passport": passport}, status=201)
                    return

            _json_response(self, {"error": "Not found."}, status=404)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.warning("API request failed: %s", exc)
            _json_response(self, {"error": str(exc)}, status=400)
        except Exception as exc:  # pragma: no cover - server must stay alive
            LOGGER.exception("Unexpected API failure: %s", exc)
            _json_response(self, {"error": "Unexpected analytics API failure."}, status=500)

    def do_PATCH(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path_parts = [part for part in parsed.path.split("/") if part]
        try:
            payload = _read_json_body(self)
            if len(path_parts) == 3 and path_parts[:2] == ["api", "assets"]:
                asset_id = path_parts[2]
                existing = REPOSITORY.get_asset(asset_id)
                if not existing:
                    _json_response(self, {"error": "Asset not found."}, status=404)
                    return
                asset = REPOSITORY.upsert_asset({**existing, **payload, "asset_id": asset_id})
                passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                _json_response(self, {"asset": asset, "passport": passport})
                return
            if len(path_parts) == 5 and path_parts[:2] == ["api", "assets"] and path_parts[3] == "mappings":
                asset_id = path_parts[2]
                mapping_id = int(path_parts[4])
                updated = REPOSITORY.update_signal_mapping(asset_id, mapping_id, payload)
                passport = _refresh_passport(asset_id, _current_live_assets_by_id().get(asset_id), STORE.snapshot().get("pipeline", {}))
                _json_response(self, {"mapping": updated, "passport": passport})
                return
            _json_response(self, {"error": "Not found."}, status=404)
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.warning("API request failed: %s", exc)
            _json_response(self, {"error": str(exc)}, status=400)
        except Exception as exc:  # pragma: no cover - server must stay alive
            LOGGER.exception("Unexpected API failure: %s", exc)
            _json_response(self, {"error": "Unexpected analytics API failure."}, status=500)


def _serve_http() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", ANALYTICS_PORT), AnalyticsHandler)
    LOGGER.info("Starting analytics HTTP service on port %s", ANALYTICS_PORT)
    server.serve_forever()


def _observability_evidences(exporter_up: bool, exporter_cpu: float, exporter_memory: float, scrape_success: float, scrape_duration: float) -> List[Dict]:
    evidences = []
    if not exporter_up:
        evidences.append(
            {
                "kind": "observability",
                "severity": "critical",
                "label": "Exporter is down",
                "reason": "Prometheus reports the exporter target as unavailable.",
                "value": 0,
                "signal": "exporter_up",
            }
        )
    if scrape_success < 1.0:
        evidences.append(
            {
                "kind": "observability",
                "severity": "critical",
                "label": "Exporter scrape failed",
                "reason": "Latest OPC UA scrape was not successful.",
                "value": _round(scrape_success),
                "signal": "scrape_success",
            }
        )
    if exporter_cpu >= 0.7:
        evidences.append(
            {
                "kind": "observability",
                "severity": "warning",
                "label": "High exporter CPU load",
                "reason": "Exporter CPU rate is above the degradation threshold.",
                "value": _round(exporter_cpu),
                "signal": "exporter_cpu_rate",
            }
        )
    if exporter_memory >= 350_000_000:
        evidences.append(
            {
                "kind": "observability",
                "severity": "warning",
                "label": "High exporter memory footprint",
                "reason": "Exporter resident memory exceeds the expected envelope.",
                "value": _round(exporter_memory / 1024 / 1024, 1),
                "signal": "exporter_memory_mb",
            }
        )
    if scrape_duration >= 1.0:
        evidences.append(
            {
                "kind": "observability",
                "severity": "warning",
                "label": "Exporter scrape is slow",
                "reason": "Scrape duration is above the expected threshold.",
                "value": _round(scrape_duration),
                "signal": "scrape_duration_seconds",
            }
        )
    return evidences


def _rule_evidence(signal_payload: Dict, rule_outcome: RuleOutcome) -> Optional[Dict]:
    if not rule_outcome.flag:
        return None
    return {
        "kind": "signal",
        "severity": rule_outcome.severity,
        "label": f"{signal_payload['display_name']} is out of range",
        "reason": f"Rule-based detector classified {signal_payload['display_name']} as {rule_outcome.severity}.",
        "value": signal_payload["value"],
        "unit": signal_payload["unit"],
        "signal": signal_payload["signal"],
    }


def analytics_loop() -> None:
    rules_config = load_rules_config()
    rule_detector = RuleDetector(rules_config.get("rules", []))
    zscore_threshold = float(rules_config.get("zscore_threshold", 3.0))
    mad_threshold = float(rules_config.get("mad_threshold", 3.5))
    zscore_detector = RollingZScoreDetector(
        window_size=int(rules_config.get("zscore_window_size", 20)),
        threshold=zscore_threshold,
        min_history=int(rules_config.get("zscore_min_history", 5)),
    )
    mad_detector = RollingMADDetector(
        window_size=int(rules_config.get("mad_window_size", 20)),
        threshold=mad_threshold,
        min_history=int(rules_config.get("mad_min_history", 5)),
    )

    while True:
        try:
            asset_state: Dict[str, Dict] = defaultdict(
                lambda: {
                    "asset_type": "unknown",
                    "signals": [],
                    "votes": 0,
                    "total_checks": 0,
                    "active_anomalies": 0,
                }
            )

            for result in _asset_signal_samples():
                metric = result["metric"]
                asset_id = metric.get("asset_id", "unknown")
                asset_type = metric.get("asset_type", "unknown")
                signal = metric.get("signal", "unknown")
                metric_name = metric.get("__name__", "asset_signal_value")
                signal_path = metric.get("path", "")
                signal_leaf = signal_path.split("/")[-1] if signal_path else signal
                value = float(result["value"][1])

                rule_outcome = rule_detector.evaluate(signal, value)
                zscore_outcome = zscore_detector.observe((asset_id, signal), value)
                mad_outcome = mad_detector.observe((asset_id, signal), value)

                ANOMALY_SCORE.labels(asset_id, asset_type, signal, "rules").set(rule_outcome.score)
                ANOMALY_SCORE.labels(asset_id, asset_type, signal, "zscore").set(zscore_outcome.score)
                ANOMALY_SCORE.labels(asset_id, asset_type, signal, "mad").set(mad_outcome.score)
                DETECTOR_FLAG.labels(asset_id, asset_type, signal, "rules").set(1 if rule_outcome.flag else 0)
                DETECTOR_FLAG.labels(asset_id, asset_type, signal, "zscore").set(1 if zscore_outcome.flag else 0)
                DETECTOR_FLAG.labels(asset_id, asset_type, signal, "mad").set(1 if mad_outcome.flag else 0)

                positive_votes = int(rule_outcome.flag) + int(zscore_outcome.flag) + int(mad_outcome.flag)
                DETECTOR_VOTES.labels(asset_id, asset_type, signal).set(positive_votes)

                severity = _severity_from_votes(rule_outcome, positive_votes)
                normalized_score = max(
                    rule_outcome.score,
                    min(1.0, zscore_outcome.score / max(zscore_threshold, 1.0)),
                    min(1.0, mad_outcome.score / max(mad_threshold, 1.0)),
                )
                signal_payload = {
                    "signal_key": f"{signal}::{signal_path or 'root'}",
                    "signal": signal,
                    "display_name": (
                        f"{signal.replace('_', ' ').title()} · {signal_leaf}" if signal_leaf.lower() != signal.lower() else signal.replace("_", " ").title()
                    ),
                    "category": _category_from_metric(metric_name),
                    "value": _round(value, 4),
                    "unit": metric.get("unit", "unknown"),
                    "subsystem": metric.get("subsystem", "unknown"),
                    "criticality": metric.get("criticality", "medium"),
                    "path": metric.get("path", ""),
                    "nodeid": metric.get("nodeid"),
                    "namespace": metric.get("namespace"),
                    "severity": severity,
                    "detector_vote_total": positive_votes,
                    "anomaly_score": _round(normalized_score, 3),
                    "detectors": {
                        "rules": {
                            "flag": rule_outcome.flag,
                            "score": _round(rule_outcome.score),
                            "severity": rule_outcome.severity,
                        },
                        "zscore": {
                            "flag": zscore_outcome.flag,
                            "score": _round(zscore_outcome.score),
                        },
                        "mad": {
                            "flag": mad_outcome.flag,
                            "score": _round(mad_outcome.score),
                        },
                    },
                }

                rule_evidence = _rule_evidence(signal_payload, rule_outcome)
                if rule_evidence:
                    signal_payload["primary_evidence"] = rule_evidence

                state = asset_state[asset_id]
                state["asset_type"] = asset_type
                state["signals"].append(signal_payload)
                state["votes"] += positive_votes
                state["total_checks"] += 3
                if positive_votes > 0:
                    state["active_anomalies"] += 1

            exporter_up = _first_value(f'up{{job="{EXPORTER_JOB_NAME}"}}', default=0.0) >= 1.0
            exporter_cpu = _first_value(f'rate(process_cpu_seconds_total{{job="{EXPORTER_JOB_NAME}"}}[1m])', default=0.0)
            exporter_memory = _first_value(f'process_resident_memory_bytes{{job="{EXPORTER_JOB_NAME}"}}', default=0.0)
            exporter_scrape_success = _first_value("asset_exporter_scrape_success", default=0.0)
            exporter_scrape_duration = _first_value("asset_exporter_scrape_duration_seconds", default=0.0)
            asset_scrape_success = _label_value_map("asset_exporter_asset_scrape_success")
            asset_scrape_duration = _label_value_map("asset_exporter_asset_scrape_duration_seconds")
            analytics_cpu = _first_value('rate(process_cpu_seconds_total{job="analytics"}[1m])', default=0.0)
            analytics_memory = _first_value('process_resident_memory_bytes{job="analytics"}', default=0.0)
            timestamp = _utc_now()
            pipeline_payload = {
                "exporter_up": exporter_up,
                "exporter_scrape_success": _round(exporter_scrape_success),
                "exporter_scrape_duration_seconds": _round(exporter_scrape_duration),
                "exporter_cpu_rate": _round(exporter_cpu, 4),
                "exporter_memory_mb": _round(exporter_memory / 1024 / 1024, 1) if exporter_memory else 0.0,
                "analytics_cpu_rate": _round(analytics_cpu, 4),
                "analytics_memory_mb": _round(analytics_memory / 1024 / 1024, 1) if analytics_memory else 0.0,
            }

            assets = []
            history_entries = {}
            signal_entries: Dict[str, Dict[str, float]] = defaultdict(dict)

            for asset_id, state in asset_state.items():
                registry_asset = REPOSITORY.get_asset(asset_id) or {}
                primary_connection = registry_asset.get("primary_connection") or {}
                connection_type = primary_connection.get("connection_type", "unknown")
                collection_mode = _collection_mode_for(connection_type)
                continuity_score = _continuity_score(asset_scrape_success.get(asset_id, exporter_scrape_success), asset_scrape_duration.get(asset_id, exporter_scrape_duration), connection_type)
                continuity_label = _continuity_label_for(connection_type)
                signals = sorted(
                    state["signals"],
                    key=lambda item: (
                        {"critical": 0, "warning": 1, "nominal": 2}.get(item["severity"], 3),
                        -item["anomaly_score"],
                        item["signal"],
                    ),
                )
                vote_ratio = state["votes"] / state["total_checks"] if state["total_checks"] else 0.0
                asset_scrape_success_value = asset_scrape_success.get(asset_id, exporter_scrape_success)
                asset_scrape_duration_value = asset_scrape_duration.get(asset_id, exporter_scrape_duration)
                continuity_score = _continuity_score(asset_scrape_success_value, asset_scrape_duration_value, connection_type)
                connector_health = _connector_health_state(
                    exporter_reachable=exporter_up and asset_scrape_success_value > 0,
                    scrape_success=asset_scrape_success_value,
                    scrape_duration=asset_scrape_duration_value,
                    continuity_score=continuity_score,
                )
                connector_status = "connected" if connector_health == "healthy" else ("degraded" if connector_health == "degraded" else "disconnected")
                base_observability_evidence = _observability_evidences(
                    exporter_up=exporter_up and asset_scrape_success_value > 0,
                    exporter_cpu=exporter_cpu,
                    exporter_memory=exporter_memory,
                    scrape_success=asset_scrape_success_value,
                    scrape_duration=asset_scrape_duration_value,
                )
                outcome = infer_root_cause(
                    ot_vote_ratio=vote_ratio,
                    exporter_up=exporter_up and asset_scrape_success_value > 0,
                    cpu_rate=exporter_cpu,
                    memory_bytes=exporter_memory,
                    scrape_success=asset_scrape_success_value,
                    scrape_duration=asset_scrape_duration_value,
                )

                MONITORING_CONFIDENCE.labels(asset_id, state["asset_type"]).set(outcome.confidence)
                for hint in ROOT_CAUSES:
                    ROOT_CAUSE_STATE.labels(asset_id, state["asset_type"], hint).set(1 if hint == outcome.hint else 0)

                signal_evidences = [item["primary_evidence"] for item in signals if item.get("primary_evidence")]
                evidences = sorted(
                    signal_evidences + base_observability_evidence,
                    key=lambda item: ({"critical": 0, "warning": 1, "nominal": 2}.get(item["severity"], 3), item["label"]),
                )
                status = _status_from_root_cause(outcome.hint, state["active_anomalies"])
                top_signal = signals[0]["display_name"] if signals else None
                diagnosis = {
                    "root_cause": outcome.hint,
                    "monitoring_confidence": _round(outcome.confidence),
                    "summary": _summary_for_root_cause(outcome.hint, asset_id, evidences, outcome.confidence),
                    "evidences": evidences[:6],
                    "top_signal": top_signal,
                    "active_anomalies": state["active_anomalies"],
                    "vote_ratio": _round(vote_ratio),
                }
                kpis = {
                    "active_anomalies": state["active_anomalies"],
                    "detector_votes": state["votes"],
                    "signals_tracked": len(signals),
                    "monitoring_confidence": _round(outcome.confidence),
                }
                for signal in signals:
                    signal["connection_type"] = connection_type
                    signal["source_ref"] = signal.get("path") or signal.get("nodeid") or signal["signal_key"]
                    signal["trend"] = STORE.signal_trend_preview(asset_id, signal["signal_key"], signal["value"], limit=12)
                    signal_entries[asset_id][signal["signal_key"]] = signal["value"]
                asset_payload = {
                    "asset_id": asset_id,
                    "asset_type": state["asset_type"],
                    "display_name": registry_asset.get("display_name") or asset_id,
                    "primary_connection_type": connection_type,
                    "status": status,
                    "kpis": kpis,
                    "diagnosis": diagnosis,
                    "trend": STORE.asset_confidence_trend_preview(asset_id, outcome.confidence, limit=14),
                    "connection": {
                        "connection_type": connection_type,
                        "endpoint_or_host": primary_connection.get("endpoint_or_host") or registry_asset.get("opcua_endpoint") or "",
                        "broker_url": (primary_connection.get("config") or {}).get("broker_url", ""),
                        "topic_root": (primary_connection.get("config") or {}).get("topic_root", ""),
                        "client_id": (primary_connection.get("config") or {}).get("client_id", ""),
                        "collection_mode": collection_mode,
                        "connector_status": connector_status,
                        "connector_health": connector_health,
                        "continuity_score": continuity_score,
                        "continuity_label": continuity_label,
                        "last_seen_at": timestamp,
                        "freshness_seconds": 0.0,
                    },
                    "observability": {
                        "exporter_reachable": exporter_up and asset_scrape_success_value > 0,
                        "scrape_success": _round(asset_scrape_success_value),
                        "scrape_duration_seconds": _round(asset_scrape_duration_value),
                        "connector_type": connection_type,
                        "connector_status": connector_status,
                        "collection_mode": collection_mode,
                        "last_seen_at": timestamp,
                        "freshness_seconds": 0.0,
                        "continuity_score": continuity_score,
                        "continuity_label": continuity_label,
                        "connector_health": connector_health,
                    },
                    "signals": signals,
                }
                assets.append(asset_payload)
                REPOSITORY.upsert_live_asset(asset_payload)
                REPOSITORY.upsert_signal_inventory_from_live(asset_id, signals, profile_id=asset_payload["asset_type"])
                REPOSITORY.add_snapshot(
                    asset_id,
                    {
                        "timestamp": timestamp,
                        "status": status,
                        "monitoring_confidence": _round(outcome.confidence),
                        "active_anomalies": state["active_anomalies"],
                        "top_signal": top_signal,
                    },
                )
                _refresh_passport(asset_id, live_asset=asset_payload, pipeline=pipeline_payload)
                history_entries[asset_id] = {
                    "timestamp": timestamp,
                    "root_cause": outcome.hint,
                    "monitoring_confidence": _round(outcome.confidence),
                    "active_anomalies": state["active_anomalies"],
                    "vote_ratio": _round(vote_ratio),
                    "top_signal": top_signal,
                    "status": status,
                    "connection_type": connection_type,
                    "collection_mode": collection_mode,
                    "connector_status": connector_status,
                    "connector_health": connector_health,
                    "continuity_score": continuity_score,
                    "continuity_label": continuity_label,
                }

            snapshot = {
                "generated_at": timestamp,
                "pipeline": pipeline_payload,
                "assets": sorted(assets, key=lambda item: item["asset_id"]),
            }
            STORE.update(snapshot, history_entries, signal_entries)
            ANALYTICS_CYCLE_SUCCESS.set(1)
            LOGGER.info("Analytics cycle completed for %s assets", len(assets))
        except (urllib.error.URLError, TimeoutError, KeyError, ValueError) as exc:
            ANALYTICS_CYCLE_SUCCESS.set(0)
            LOGGER.warning("Analytics cycle failed: %s", exc)
        except Exception as exc:  # pragma: no cover - service should continue running
            ANALYTICS_CYCLE_SUCCESS.set(0)
            LOGGER.exception("Unexpected analytics failure: %s", exc)

        time.sleep(ANALYTICS_INTERVAL_SECONDS)


def main() -> None:
    server_thread = threading.Thread(target=_serve_http, daemon=True)
    server_thread.start()
    analytics_loop()


if __name__ == "__main__":
    main()
