from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NodeSample:
    browse_name: str
    namespace: Any
    nodeid: str
    path: str
    value: Any


@dataclass(frozen=True)
class SignalMetadata:
    asset_type: str
    category: str
    criticality: str
    metric_name: str
    signal: str
    subsystem: str
    unit: str
