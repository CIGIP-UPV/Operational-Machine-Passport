import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

import paho.mqtt.client as mqtt


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("mqtt_simulator")


SCENARIO_FILE = Path(os.getenv("MQTT_SCENARIO_FILE", "/app/config/scenarios/cnc_mqtt_baseline.json"))
BROKER_URL = os.getenv("MQTT_BROKER_URL", "mqtt://mqtt-broker:1883")
TOPIC_ROOT_OVERRIDE = os.getenv("MQTT_TOPIC_ROOT", "")
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "opc-observe-mqtt-simulator")
USERNAME = os.getenv("MQTT_USERNAME", "")
PASSWORD = os.getenv("MQTT_PASSWORD", "")
QOS = int(os.getenv("MQTT_QOS", "0"))
GROUND_TRUTH_PATH = Path(os.getenv("GROUND_TRUTH_PATH", "/data/ground_truth_cnc_mqtt_01.jsonl"))


@dataclass
class ActiveEvent:
    end_at: int
    label: str
    mode: str
    start_at: int


class AssetScenario:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.asset = config["asset"]
        self.runtime = config.get("runtime", {})
        self.signals = config["signals"]
        self.events: List[ActiveEvent] = [
            ActiveEvent(
                start_at=int(event["start_at"]),
                end_at=int(event["end_at"]),
                mode=event["mode"],
                label=event.get("label", event["mode"]),
            )
            for event in config["events"]
        ]
        self.rng = random.Random(int(self.runtime.get("seed", 42)))
        self._state: Dict[str, Any] = {}
        self._initialize_state()

    @property
    def update_interval_seconds(self) -> float:
        return float(self.runtime.get("update_interval_seconds", 1.0))

    @property
    def initial_delay_seconds(self) -> float:
        return float(self.runtime.get("initial_delay_seconds", 0.0))

    @property
    def topic_root(self) -> str:
        return TOPIC_ROOT_OVERRIDE.strip("/") or self.asset.get("topic_root", "").strip("/")

    def _initialize_state(self) -> None:
        for name, definition in self.signals.items():
            initial = definition.get("initial")
            if initial is None:
                initial = self._sample_value(name, definition, self.events[0].mode, 0)
            self._state[name] = initial

    def signal_items(self) -> Iterable:
        return self.signals.items()

    def active_event(self, elapsed_seconds: int) -> ActiveEvent:
        for event in self.events:
            if event.start_at <= elapsed_seconds <= event.end_at:
                return event
        return self.events[-1]

    def next_step(self, elapsed_seconds: int) -> Dict[str, Any]:
        event = self.active_event(elapsed_seconds)
        phase_elapsed = max(0, elapsed_seconds - event.start_at)
        for name, definition in self.signals.items():
            self._state[name] = self._sample_value(name, definition, event.mode, phase_elapsed)
        return dict(self._state)

    def _sample_value(self, name: str, definition: Dict[str, Any], mode: str, phase_elapsed: int) -> Any:
        kind = definition.get("kind", "float")
        mode_config = (definition.get("modes") or {}).get(mode) or (definition.get("modes") or {}).get("nominal", {})
        previous = self._state.get(name, definition.get("initial"))

        if kind == "counter":
            increment = float(mode_config.get("increment_mean", 1.0))
            increment += self.rng.uniform(-float(mode_config.get("increment_noise", 0.0)), float(mode_config.get("increment_noise", 0.0)))
            increment = max(0.0, increment)
            return int(float(previous or 0) + increment)

        if kind == "bool":
            return self.rng.random() < float(mode_config.get("true_probability", 0.5))

        baseline = float(mode_config.get("baseline", definition.get("initial", 0.0)))
        drift_per_step = float(mode_config.get("drift_per_step", 0.0))
        noise = float(mode_config.get("noise", 0.0))
        value = baseline + drift_per_step * phase_elapsed + self.rng.uniform(-noise, noise)
        if mode_config.get("min") is not None:
            value = max(float(mode_config["min"]), value)
        if mode_config.get("max") is not None:
            value = min(float(mode_config["max"]), value)
        if kind == "int":
            return int(round(value))
        return round(value, int(definition.get("precision", 3)))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_scenario() -> AssetScenario:
    with SCENARIO_FILE.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    return AssetScenario(config)


def connect_client() -> mqtt.Client:
    parsed = urlparse(BROKER_URL)
    host = parsed.hostname or BROKER_URL
    port = parsed.port or 1883
    client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
    if USERNAME:
        client.username_pw_set(USERNAME, PASSWORD or None)

    while True:
        try:
            client.connect(host, port, keepalive=15)
            client.loop_start()
            LOGGER.info("Connected MQTT simulator to %s:%s", host, port)
            return client
        except Exception as exc:  # pragma: no cover - runtime retry loop
            LOGGER.warning("MQTT broker is not reachable yet (%s). Retrying in 2s.", exc)
            time.sleep(2)


def publish_signals(client: mqtt.Client, scenario: AssetScenario, values: Dict[str, Any]) -> None:
    for signal_name, definition in scenario.signal_items():
        path = definition["path"].strip("/")
        topic = "/".join(part for part in [scenario.topic_root, path] if part)
        payload = json.dumps(values[signal_name])
        client.publish(topic, payload=payload, qos=QOS, retain=True)


def append_ground_truth(scenario: AssetScenario, elapsed_seconds: int, values: Dict[str, Any]) -> None:
    event = scenario.active_event(elapsed_seconds)
    GROUND_TRUTH_PATH.parent.mkdir(parents=True, exist_ok=True)
    with GROUND_TRUTH_PATH.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "timestamp": utc_now(),
                    "asset_id": scenario.asset["id"],
                    "elapsed_seconds": elapsed_seconds,
                    "mode": event.mode,
                    "event_label": event.label,
                    "signals": values,
                }
            )
            + "\n"
        )


def main() -> None:
    scenario = load_scenario()
    client = connect_client()
    elapsed_seconds = 0

    if scenario.initial_delay_seconds:
        LOGGER.info("Waiting %.1fs before publishing MQTT telemetry.", scenario.initial_delay_seconds)
        time.sleep(scenario.initial_delay_seconds)

    LOGGER.info("Starting MQTT simulator for %s on topic root %s", scenario.asset["id"], scenario.topic_root)

    try:
        while True:
            values = scenario.next_step(elapsed_seconds)
            publish_signals(client, scenario, values)
            append_ground_truth(scenario, elapsed_seconds, values)
            elapsed_seconds += int(scenario.update_interval_seconds)
            time.sleep(scenario.update_interval_seconds)
    finally:  # pragma: no cover - runtime cleanup
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
