import asyncio
import logging
import os
import time
from typing import Dict, List

from prometheus_client import start_http_server

from .config import load_profile_config
from .discovery import collect_numeric_samples
from .mapper import MappingProfile
from .metrics_registry import MetricsRegistry
from .mqtt_connector import collect_numeric_samples as collect_mqtt_numeric_samples
from .registry import load_registered_assets


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("opcua_exporter")
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("asyncua.client").setLevel(logging.WARNING)
logging.getLogger("asyncua.common").setLevel(logging.WARNING)


OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://opcua-simulator:4840/freeopcua/assets/")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9687"))
SCRAPE_INTERVAL_SECONDS = float(os.getenv("SCRAPE_INTERVAL_SECONDS", "2"))
ASSET_ID = os.getenv("ASSET_ID", "asset-01")
ASSET_TYPE = os.getenv("ASSET_TYPE", "")
ASSET_DB_PATH = os.getenv("ASSET_DB_PATH", "/registry/assets.db")
EXPORTER_MULTI_ASSET_ENABLED = os.getenv("EXPORTER_MULTI_ASSET_ENABLED", "true").lower() not in {"0", "false", "no"}


def load_target_assets() -> List[Dict]:
    if EXPORTER_MULTI_ASSET_ENABLED:
        assets = load_registered_assets(ASSET_DB_PATH)
        if assets:
            return assets

    return [
        {
            "asset_id": ASSET_ID,
            "asset_type": ASSET_TYPE or "generic",
            "connection_type": "opcua",
            "endpoint_or_host": OPCUA_ENDPOINT,
            "connection_config": {"endpoint": OPCUA_ENDPOINT},
            "profile_id": "generic",
            "status": "active",
        }
    ]


async def scrape_asset(target: Dict, registry: MetricsRegistry) -> Dict:
    started = time.perf_counter()
    success = True
    node_count = 0
    asset_id = target["asset_id"]
    asset_type = target.get("asset_type") or "generic"
    connection_type = target.get("connection_type") or "opcua"
    endpoint = target["endpoint_or_host"]
    connection_config = target.get("connection_config") or {}
    profile = MappingProfile(load_profile_config(profile_id=target.get("profile_id", "generic")), asset_type_override=asset_type)

    try:
        if connection_type == "mqtt":
            samples = await asyncio.to_thread(
                collect_mqtt_numeric_samples,
                endpoint,
                topic_root=connection_config.get("topic_root", ""),
                qos=int(connection_config.get("qos", 0) or 0),
                client_id=connection_config.get("client_id", ""),
                username=connection_config.get("username", ""),
                password=connection_config.get("password", ""),
                listen_seconds=float(connection_config.get("listen_seconds", SCRAPE_INTERVAL_SECONDS) or SCRAPE_INTERVAL_SECONDS),
            )
        else:
            samples = await collect_numeric_samples(endpoint)
        node_count = len(samples)
        for sample in samples:
            metadata = profile.map_sample(sample)
            registry.publish_sample(asset_id, metadata, sample)
        LOGGER.info("Exported %s %s samples from %s for asset %s", node_count, connection_type.upper(), endpoint, asset_id)
    except Exception as exc:  # pragma: no cover - exporter should stay alive
        success = False
        LOGGER.exception("Scrape failed for %s: %s", asset_id, exc)

    duration = time.perf_counter() - started
    registry.record_asset_scrape(asset_id, asset_type, duration, success, node_count)
    return {
        "asset_id": asset_id,
        "asset_type": asset_type,
        "success": success,
        "node_count": node_count,
        "duration_seconds": duration,
    }


async def scrape_loop() -> None:
    registry = MetricsRegistry()

    while True:
        cycle_started = time.perf_counter()
        targets = load_target_assets()
        results = await asyncio.gather(*(scrape_asset(target, registry) for target in targets))
        successful_assets = sum(1 for item in results if item["success"])
        total_nodes = sum(item["node_count"] for item in results)
        registry.record_cycle(
            duration_seconds=time.perf_counter() - cycle_started,
            successful_assets=successful_assets,
            total_assets=len(targets),
            total_nodes=total_nodes,
        )
        await asyncio.sleep(SCRAPE_INTERVAL_SECONDS)


def main() -> None:
    LOGGER.info("Starting industrial exporter on port %s", EXPORTER_PORT)
    start_http_server(EXPORTER_PORT)
    asyncio.run(scrape_loop())


if __name__ == "__main__":
    main()
