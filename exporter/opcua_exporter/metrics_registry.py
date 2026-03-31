import time
from typing import Dict

from prometheus_client import Gauge

from .models import SignalMetadata


LABELS = ["asset_id", "asset_type", "signal", "subsystem", "unit", "criticality", "nodeid", "namespace", "path"]


class MetricsRegistry:
    def __init__(self) -> None:
        self._gauges: Dict[str, Gauge] = {}
        self.scrape_success = Gauge("asset_exporter_scrape_success", "Exporter scrape success status.")
        self.scrape_duration = Gauge("asset_exporter_scrape_duration_seconds", "Duration of the last OPC UA scrape.")
        self.scraped_nodes = Gauge("asset_exporter_scraped_nodes_total", "Number of numeric nodes exported.")
        self.asset_scrape_success = Gauge(
            "asset_exporter_asset_scrape_success",
            "Per-asset exporter scrape success status.",
            ["asset_id", "asset_type"],
        )
        self.asset_scrape_duration = Gauge(
            "asset_exporter_asset_scrape_duration_seconds",
            "Per-asset OPC UA scrape duration.",
            ["asset_id", "asset_type"],
        )
        self.asset_scraped_nodes = Gauge(
            "asset_exporter_asset_scraped_nodes_total",
            "Per-asset number of numeric nodes exported.",
            ["asset_id", "asset_type"],
        )
        self.registered_assets_total = Gauge(
            "asset_exporter_registered_assets_total",
            "Number of assets currently configured in the exporter registry.",
        )
        self.active_assets_total = Gauge(
            "asset_exporter_active_assets_total",
            "Number of assets successfully scraped in the last cycle.",
        )

    def gauge_for(self, metric_name: str) -> Gauge:
        if metric_name not in self._gauges:
            self._gauges[metric_name] = Gauge(metric_name, f"Semantic value exported by OPC UA exporter for {metric_name}.", LABELS)
        return self._gauges[metric_name]

    def publish_sample(self, asset_id: str, metadata: SignalMetadata, sample) -> None:
        namespace = sample.namespace if isinstance(sample.namespace, str) else f"ns{sample.namespace}"
        self.gauge_for(metadata.metric_name).labels(
            asset_id=asset_id,
            asset_type=metadata.asset_type,
            signal=metadata.signal,
            subsystem=metadata.subsystem,
            unit=metadata.unit,
            criticality=metadata.criticality,
            nodeid=sample.nodeid,
            namespace=namespace,
            path=sample.path,
        ).set(float(sample.value))

    def record_scrape(self, duration_seconds: float, success: bool, node_count: int) -> None:
        self.scrape_duration.set(duration_seconds)
        self.scrape_success.set(1 if success else 0)
        self.scraped_nodes.set(node_count)

    def record_asset_scrape(self, asset_id: str, asset_type: str, duration_seconds: float, success: bool, node_count: int) -> None:
        self.asset_scrape_duration.labels(asset_id=asset_id, asset_type=asset_type).set(duration_seconds)
        self.asset_scrape_success.labels(asset_id=asset_id, asset_type=asset_type).set(1 if success else 0)
        self.asset_scraped_nodes.labels(asset_id=asset_id, asset_type=asset_type).set(node_count)

    def record_cycle(self, duration_seconds: float, successful_assets: int, total_assets: int, total_nodes: int) -> None:
        ratio = successful_assets / total_assets if total_assets else 0.0
        self.scrape_duration.set(duration_seconds)
        self.scrape_success.set(ratio)
        self.scraped_nodes.set(total_nodes)
        self.registered_assets_total.set(total_assets)
        self.active_assets_total.set(successful_assets)

    def timed_scrape(self):
        started = time.perf_counter()
        return started
