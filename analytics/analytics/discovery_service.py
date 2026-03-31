import asyncio
import json
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, List

from asyncua import Client, ua


PROFILE_DIR = Path(__file__).resolve().parents[1] / "config" / "profiles"
ALLOWED_CATEGORIES = {"signal", "sensor", "status", "production", "energy", "maintenance", "alarm"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_identifier(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value or "").strip("_").lower()
    return normalized or "unknown"


def load_profile_config(profile_id: str) -> Dict[str, Any]:
    profile_path = PROFILE_DIR / f"{profile_id}.json"
    if not profile_path.exists():
        profile_path = PROFILE_DIR / "generic.json"
    with profile_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _mqtt_module():
    try:
        import paho.mqtt.client as mqtt  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on runtime image
        raise RuntimeError("MQTT support requires paho-mqtt to be installed.") from exc
    return mqtt


class DiscoveryMappingProfile:
    def __init__(self, config: Dict[str, Any], asset_type_override: str = "") -> None:
        self.rules = config.get("rules", [])
        self.default_category = sanitize_identifier(config.get("default_category", "signal"))
        self.asset_type = asset_type_override or config.get("asset_type", "generic")

    def map_node(self, node: Dict) -> Dict:
        haystack = f"{node['path']} {node['browse_name']} {node['nodeid']}"
        for rule in self.rules:
            if re.search(rule["pattern"], haystack, re.IGNORECASE):
                category = sanitize_identifier(rule.get("category", self.default_category))
                if category not in ALLOWED_CATEGORIES:
                    category = "signal"
                signal = sanitize_identifier(rule.get("signal", node["browse_name"]))
                subsystem = sanitize_identifier(rule.get("subsystem", node["path"].split("/")[-2] if "/" in node["path"] else "root"))
                return {
                    "asset_type": self.asset_type,
                    "category": category,
                    "criticality": rule.get("criticality", "medium"),
                    "signal": signal,
                    "display_name": f"{signal.replace('_', ' ').title()} · {node['browse_name']}",
                    "subsystem": subsystem,
                    "unit": rule.get("unit", "unknown"),
                    "signal_key": f"{signal}::{node['path']}",
                    "path": node["path"],
                    "nodeid": node["nodeid"],
                    "namespace": node["namespace"],
                    "sample_value": node["sample_value"],
                }

        fallback_signal = sanitize_identifier(node["browse_name"])
        return {
            "asset_type": self.asset_type,
            "category": "signal",
            "criticality": "medium",
            "signal": fallback_signal,
            "display_name": node["browse_name"],
            "subsystem": sanitize_identifier(node["path"].split("/")[-2] if "/" in node["path"] else "root"),
            "unit": "unknown",
            "signal_key": f"{fallback_signal}::{node['path']}",
            "path": node["path"],
            "nodeid": node["nodeid"],
            "namespace": node["namespace"],
            "sample_value": node["sample_value"],
        }


async def test_connection(endpoint: str) -> Dict:
    async with Client(url=endpoint) as client:
        await client.connect()
        namespace_array = await client.get_namespace_array()
        return {
            "reachable": True,
            "endpoint": endpoint,
            "connection_type": "opcua",
            "namespace_count": len(namespace_array),
            "checked_at": utc_now(),
        }


async def discover_asset(endpoint: str, profile_id: str = "generic", asset_type_override: str = "") -> Dict:
    config = load_profile_config(profile_id)
    profile = DiscoveryMappingProfile(config, asset_type_override=asset_type_override)
    nodes: List[Dict] = []
    signals: List[Dict] = []
    namespaces = set()

    async with Client(url=endpoint) as client:
        root = client.nodes.objects
        visited = set()

        async def walk(node, path_parts: List[str]) -> None:
            try:
                nodeid = str(node.nodeid)
                if nodeid in visited:
                    return
                visited.add(nodeid)

                browse_name = await node.read_browse_name()
                current_path = [*path_parts, browse_name.Name]
                children = await node.get_children()
                for child in children:
                    await walk(child, current_path)

                value = await node.read_value()
                namespace = node.nodeid.NamespaceIndex
                if namespace == 0:
                    return
                if isinstance(value, (bool, int, float)):
                    sample_value = float(value) if isinstance(value, bool) else value
                    sample_type = "boolean" if isinstance(value, bool) else "numeric"
                    namespaces.add(f"ns{namespace}")
                    node_payload = {
                        "browse_name": browse_name.Name,
                        "namespace": f"ns{namespace}",
                        "nodeid": nodeid,
                        "path": "/".join(current_path),
                        "sample_value": sample_value,
                        "sample_type": sample_type,
                    }
                    nodes.append(node_payload)
                    signals.append(profile.map_node(node_payload))
            except ua.UaStatusCodeError:
                return
            except Exception:
                return

        await walk(root, [])

    categories = {}
    for signal in signals:
        categories[signal["category"]] = categories.get(signal["category"], 0) + 1

    return {
        "endpoint": endpoint,
        "connection_type": "opcua",
        "asset_type": asset_type_override or config.get("asset_type", "generic"),
        "profile_id": profile_id,
        "discovered_at": utc_now(),
        "namespace_count": len(namespaces),
        "namespaces": sorted(namespaces),
        "node_count": len(nodes),
        "signal_count": len(signals),
        "categories": categories,
        "nodes": sorted(nodes, key=lambda item: item["path"]),
        "signals": sorted(signals, key=lambda item: (item["category"], item["display_name"])),
    }


def _flatten_mqtt_payload(topic: str, payload: Any, prefix: str = "") -> List[Dict]:
    results: List[Dict] = []
    current_prefix = prefix.strip("/")
    if isinstance(payload, dict):
        source = payload.get("signals") if "signals" in payload and isinstance(payload.get("signals"), dict) else payload
        for key, value in source.items():
            nested_prefix = "/".join(part for part in [current_prefix, str(key)] if part)
            results.extend(_flatten_mqtt_payload(topic, value, nested_prefix))
        return results
    if isinstance(payload, list):
        for index, value in enumerate(payload):
            nested_prefix = "/".join(part for part in [current_prefix, str(index)] if part)
            results.extend(_flatten_mqtt_payload(topic, value, nested_prefix))
        return results
    if isinstance(payload, bool):
        results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": float(payload), "sample_type": "boolean"})
        return results
    if isinstance(payload, (int, float)):
        results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": payload, "sample_type": "numeric"})
        return results
    if isinstance(payload, str):
        raw = payload.strip()
        if raw.lower() in {"true", "false"}:
            results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": 1.0 if raw.lower() == "true" else 0.0, "sample_type": "boolean"})
            return results
        try:
            numeric = float(raw)
        except ValueError:
            return results
        results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": numeric, "sample_type": "numeric"})
    return results


def test_mqtt_connection(
    broker_url: str,
    client_id: str = "",
    username: str = "",
    password: str = "",
    topic_root: str = "",
    qos: int = 0,
) -> Dict:
    parsed = urlparse(broker_url)
    host = parsed.hostname or broker_url
    port = parsed.port or 1883
    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as exc:
        raise RuntimeError(f"MQTT broker {broker_url} is not reachable: {exc}") from exc

    return {
        "reachable": True,
        "endpoint": broker_url,
        "connection_type": "mqtt",
        "topic_root": topic_root or "#",
        "qos": qos,
        "client_id": client_id,
        "checked_at": utc_now(),
    }


def discover_mqtt_asset(
    broker_url: str,
    profile_id: str = "generic",
    asset_type_override: str = "",
    topic_root: str = "",
    qos: int = 0,
    client_id: str = "",
    username: str = "",
    password: str = "",
    listen_seconds: float = 2.0,
) -> Dict:
    mqtt = _mqtt_module()
    config = load_profile_config(profile_id)
    profile = DiscoveryMappingProfile(config, asset_type_override=asset_type_override)
    topic_filter = f"{topic_root.rstrip('/')}/#" if topic_root else "#"
    captured: Dict[str, Dict] = {}

    client = mqtt.Client(client_id=client_id or "", clean_session=True)
    if username:
        client.username_pw_set(username, password or None)

    def on_message(_client, _userdata, msg):
        payload_bytes = msg.payload.decode("utf-8", errors="ignore").strip()
        if not payload_bytes:
            return
        try:
            parsed_payload = json.loads(payload_bytes)
        except json.JSONDecodeError:
            parsed_payload = payload_bytes
        for item in _flatten_mqtt_payload(msg.topic, parsed_payload):
            path = item["path"]
            captured[path] = {
                "browse_name": item["browse_name"],
                "namespace": "mqtt",
                "nodeid": f"mqtt:{path}",
                "path": path,
                "sample_value": item["value"],
                "sample_type": item["sample_type"],
            }

    client.on_message = on_message
    client.connect(parsed.hostname if (parsed := urlparse(broker_url)).hostname else broker_url, parsed.port or 1883, keepalive=10)
    client.subscribe(topic_filter, qos=qos)
    client.loop_start()
    try:
        end = time.time() + listen_seconds
        while time.time() < end:
            time.sleep(0.1)
    finally:
        client.loop_stop()
        client.disconnect()

    nodes = sorted(captured.values(), key=lambda item: item["path"])
    signals = [profile.map_node(node) for node in nodes]
    categories = {}
    for signal in signals:
        categories[signal["category"]] = categories.get(signal["category"], 0) + 1

    return {
        "endpoint": broker_url,
        "connection_type": "mqtt",
        "asset_type": asset_type_override or config.get("asset_type", "generic"),
        "profile_id": profile_id,
        "discovered_at": utc_now(),
        "namespace_count": 1 if nodes else 0,
        "namespaces": ["mqtt"] if nodes else [],
        "node_count": len(nodes),
        "signal_count": len(signals),
        "categories": categories,
        "topic_root": topic_root or "#",
        "nodes": nodes,
        "signals": sorted(signals, key=lambda item: (item["category"], item["display_name"])),
    }


def run_test_connection(endpoint: str, connection_type: str = "opcua", config: Dict[str, Any] | None = None) -> Dict:
    config = config or {}
    if connection_type == "mqtt":
        return test_mqtt_connection(
            broker_url=endpoint,
            client_id=config.get("client_id", ""),
            username=config.get("username", ""),
            password=config.get("password", ""),
            topic_root=config.get("topic_root", ""),
            qos=int(config.get("qos", 0) or 0),
        )
    return asyncio.run(test_connection(endpoint))


def run_discovery(
    endpoint: str,
    profile_id: str = "generic",
    asset_type_override: str = "",
    connection_type: str = "opcua",
    config: Dict[str, Any] | None = None,
) -> Dict:
    config = config or {}
    if connection_type == "mqtt":
        return discover_mqtt_asset(
            broker_url=endpoint,
            profile_id=profile_id,
            asset_type_override=asset_type_override,
            topic_root=config.get("topic_root", ""),
            qos=int(config.get("qos", 0) or 0),
            client_id=config.get("client_id", ""),
            username=config.get("username", ""),
            password=config.get("password", ""),
            listen_seconds=float(config.get("listen_seconds", 2.0) or 2.0),
        )
    return asyncio.run(discover_asset(endpoint, profile_id=profile_id, asset_type_override=asset_type_override))
