import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

from asyncua import Server, ua

from .config import load_scenario_config
from .machine_model import AssetScenario


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("opcua_simulator")
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("asyncua.server").setLevel(logging.WARNING)
logging.getLogger("asyncua.common").setLevel(logging.WARNING)


def _variant_type(kind: str) -> ua.VariantType:
    if kind == "bool":
        return ua.VariantType.Boolean
    if kind == "int":
        return ua.VariantType.Int64
    if kind == "counter":
        return ua.VariantType.Int64
    if kind == "string":
        return ua.VariantType.String
    return ua.VariantType.Double


async def _ensure_object(parent, namespace: int, name: str, cache: Dict[Tuple[int, str], Any]):
    cache_key = (id(parent), name)
    if cache_key in cache:
        return cache[cache_key]
    obj = await parent.add_object(namespace, name)
    cache[cache_key] = obj
    return obj


async def _create_nodes(server: Server, scenario: AssetScenario):
    asset = scenario.asset
    namespace = await server.register_namespace(asset["namespace_uri"])
    root = await server.nodes.objects.add_object(namespace, asset["name"])
    object_cache: Dict[Tuple[int, str], Any] = {}
    nodes = {}

    for signal_name, definition in scenario.signal_items():
        path_parts = definition["path"].split("/")
        parent = root
        for object_name in path_parts[:-1]:
            parent = await _ensure_object(parent, namespace, object_name, object_cache)
        leaf_name = path_parts[-1]
        initial_value = scenario.current_values()[signal_name]
        node = await parent.add_variable(
            f"ns={namespace};s={definition['path']}",
            leaf_name,
            ua.Variant(initial_value, _variant_type(definition.get("kind", "float"))),
        )
        await node.set_writable()
        nodes[signal_name] = node
    return nodes


def _ground_truth_path() -> Path:
    raw = os.getenv("GROUND_TRUTH_PATH")
    if raw:
        return Path(raw)
    return Path("/tmp/opcua_ground_truth.jsonl")


def _write_ground_truth(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


async def main() -> None:
    config = load_scenario_config()
    scenario = AssetScenario(config)

    server = Server()
    await server.init()
    endpoint = os.getenv("OPCUA_ENDPOINT", config["asset"]["endpoint"])
    server.set_endpoint(endpoint)

    nodes = await _create_nodes(server, scenario)
    ground_truth_path = _ground_truth_path()
    elapsed = 0

    LOGGER.info("Starting OPC UA simulator for asset %s at %s", config["asset"]["id"], endpoint)
    LOGGER.info("Writing ground truth to %s", ground_truth_path)

    async with server:
        if scenario.initial_delay_seconds > 0:
            await asyncio.sleep(scenario.initial_delay_seconds)
        while True:
            event = scenario.active_event(elapsed)
            values = scenario.next_step(elapsed)
            for signal_name, node in nodes.items():
                definition = scenario.signals[signal_name]
                kind = definition.get("kind", "float")
                await node.write_value(ua.Variant(values[signal_name], _variant_type(kind)))
            _write_ground_truth(
                ground_truth_path,
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "elapsed_seconds": elapsed,
                    "asset_id": config["asset"]["id"],
                    "asset_type": config["asset"]["type"],
                    "event_label": event.label,
                    "mode": event.mode,
                    "signals": values,
                },
            )
            await asyncio.sleep(scenario.update_interval_seconds)
            elapsed += int(max(1, round(scenario.update_interval_seconds)))


if __name__ == "__main__":
    asyncio.run(main())
