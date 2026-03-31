import json
import socket
import time
from typing import Any, Dict, List
from urllib.parse import urlparse

from .models import NodeSample


def _mqtt_module():
    try:
        import paho.mqtt.client as mqtt  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on runtime image
        raise RuntimeError("MQTT support requires paho-mqtt to be installed.") from exc
    return mqtt


def test_connection(broker_url: str) -> None:
    parsed = urlparse(broker_url)
    host = parsed.hostname or broker_url
    port = parsed.port or 1883
    with socket.create_connection((host, port), timeout=5):
        return None


def _flatten_payload(topic: str, payload: Any, prefix: str = "") -> List[Dict]:
    results: List[Dict] = []
    current_prefix = prefix.strip("/")
    if isinstance(payload, dict):
        source = payload.get("signals") if "signals" in payload and isinstance(payload.get("signals"), dict) else payload
        for key, value in source.items():
            nested_prefix = "/".join(part for part in [current_prefix, str(key)] if part)
            results.extend(_flatten_payload(topic, value, nested_prefix))
        return results
    if isinstance(payload, list):
        for index, value in enumerate(payload):
            nested_prefix = "/".join(part for part in [current_prefix, str(index)] if part)
            results.extend(_flatten_payload(topic, value, nested_prefix))
        return results
    if isinstance(payload, bool):
        results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": 1.0 if payload else 0.0})
        return results
    if isinstance(payload, (int, float)):
        results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": float(payload)})
        return results
    if isinstance(payload, str):
        raw = payload.strip()
        if raw.lower() in {"true", "false"}:
            results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": 1.0 if raw.lower() == "true" else 0.0})
            return results
        try:
            numeric = float(raw)
        except ValueError:
            return results
        results.append({"browse_name": current_prefix.split("/")[-1] or topic.split("/")[-1], "path": f"{topic}/{current_prefix}".rstrip("/"), "value": numeric})
    return results


def collect_numeric_samples(
    broker_url: str,
    topic_root: str = "",
    qos: int = 0,
    client_id: str = "",
    username: str = "",
    password: str = "",
    listen_seconds: float = 2.0,
) -> List[NodeSample]:
    mqtt = _mqtt_module()
    parsed = urlparse(broker_url)
    host = parsed.hostname or broker_url
    port = parsed.port or 1883
    topic_filter = f"{topic_root.rstrip('/')}/#" if topic_root else "#"
    captured: Dict[str, NodeSample] = {}

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
        for item in _flatten_payload(msg.topic, parsed_payload):
            captured[item["path"]] = NodeSample(
                browse_name=item["browse_name"],
                namespace="mqtt",
                nodeid=f"mqtt:{item['path']}",
                path=item["path"],
                value=item["value"],
            )

    client.on_message = on_message
    client.connect(host, port, keepalive=10)
    client.subscribe(topic_filter, qos=qos)
    client.loop_start()
    try:
        end = time.time() + listen_seconds
        while time.time() < end:
            time.sleep(0.1)
    finally:
        client.loop_stop()
        client.disconnect()

    return sorted(captured.values(), key=lambda sample: sample.path)
