import re
from typing import Any, Dict

from .models import NodeSample, SignalMetadata


ALLOWED_CATEGORIES = {"signal", "sensor", "status", "production", "energy", "maintenance", "alarm"}


def sanitize_identifier(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return normalized or "unknown"


class MappingProfile:
    def __init__(self, config: Dict[str, Any], asset_type_override: str = "") -> None:
        self.config = config
        self.rules = config.get("rules", [])
        self.default_category = sanitize_identifier(config.get("default_category", "signal"))
        self.asset_type = asset_type_override or config.get("asset_type", "generic_asset")

    def map_sample(self, sample: NodeSample) -> SignalMetadata:
        haystack = f"{sample.path} {sample.browse_name} {sample.nodeid}"
        for rule in self.rules:
            if re.search(rule["pattern"], haystack, re.IGNORECASE):
                category = sanitize_identifier(rule.get("category", self.default_category))
                if category not in ALLOWED_CATEGORIES:
                    category = "signal"
                signal_name = sanitize_identifier(rule.get("signal", sample.browse_name))
                subsystem = sanitize_identifier(rule.get("subsystem", sample.path.split("/")[-2] if "/" in sample.path else "root"))
                unit = rule.get("unit", "unknown")
                criticality = rule.get("criticality", "medium")
                return SignalMetadata(
                    asset_type=self.asset_type,
                    category=category,
                    criticality=criticality,
                    metric_name=f"asset_{category}_value",
                    signal=signal_name,
                    subsystem=subsystem,
                    unit=unit,
                )

        fallback_signal = sanitize_identifier(sample.browse_name)
        subsystem = sanitize_identifier(sample.path.split("/")[-2] if "/" in sample.path else "root")
        return SignalMetadata(
            asset_type=self.asset_type,
            category="signal",
            criticality="medium",
            metric_name="asset_signal_value",
            signal=fallback_signal,
            subsystem=subsystem,
            unit="unknown",
        )
