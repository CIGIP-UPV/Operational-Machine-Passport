from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional


def _round(value: float, digits: int = 2) -> float:
    return round(float(value), digits)


def _freshness_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())


def _connector_health(exporter_reachable: bool, scrape_success: float, scrape_duration_seconds: float, continuity_score: float, freshness_seconds: Optional[float]) -> str:
    if not exporter_reachable or scrape_success <= 0:
        return "outage"
    if scrape_duration_seconds >= 2.0 or continuity_score < 45 or (freshness_seconds is not None and freshness_seconds > 30):
        return "degraded"
    return "healthy"


def _baseline_from_live_signals(live_asset: Optional[Dict], stored_signals: List[Dict]) -> List[Dict]:
    baselines = []
    if live_asset and live_asset.get("signals"):
        for signal in live_asset["signals"]:
            trend = [float(item) for item in signal.get("trend", []) if item is not None]
            if not trend:
                trend = [float(signal.get("value", 0.0))]
            baselines.append(
                {
                    "signal_key": signal["signal_key"],
                    "display_name": signal["display_name"],
                    "min_value": min(trend),
                    "max_value": max(trend),
                    "avg_value": _round(sum(trend) / len(trend), 4),
                    "sample_count": len(trend),
                    "confidence": min(1.0, 0.2 + len(trend) / 20.0),
                }
            )
        return baselines

    for signal in stored_signals:
        sample = signal.get("sample_value")
        if sample is None:
            continue
        baselines.append(
            {
                "signal_key": signal["signal_key"],
                "display_name": signal["display_name"],
                "min_value": sample,
                "max_value": sample,
                "avg_value": sample,
                "sample_count": 1,
                "confidence": 0.1,
            }
        )
    return baselines


def build_passport(
    asset: Dict,
    nameplate_data: Optional[Dict],
    stored_signals: List[Dict],
    signal_mappings: List[Dict],
    stored_nodes: List[Dict],
    live_asset: Optional[Dict],
    pipeline: Dict,
    events: List[Dict],
    notes: List[Dict],
    maintenance_events: List[Dict],
    software_inventory: List[Dict],
    components: List[Dict],
    documents: List[Dict],
    compliance_certificates: List[Dict],
    access_policy: Optional[Dict],
    integrity_record: Optional[Dict],
    sustainability_record: Optional[Dict],
    ownership_events: List[Dict],
) -> Dict:
    live_signals = live_asset.get("signals", []) if live_asset else []
    effective_signals = live_signals or stored_signals
    semantic_inventory = signal_mappings or stored_signals
    categories = Counter(signal.get("category", "signal") for signal in semantic_inventory)
    subsystems = sorted({signal.get("subsystem", "unknown") for signal in semantic_inventory})
    active_mapping_count = sum(1 for item in signal_mappings if item.get("is_active"))
    manual_mapping_count = sum(1 for item in signal_mappings if item.get("mapping_source") == "manual")
    inactive_mapping_count = sum(1 for item in signal_mappings if not item.get("is_active"))
    mapping_denominator = len(stored_nodes) or len(stored_signals) or len(signal_mappings) or 1
    coverage_ratio = (active_mapping_count / mapping_denominator) if signal_mappings else ((sum(categories.values()) / max(1, len(stored_nodes))) if stored_nodes else 0.0)
    active_ratio = active_mapping_count / max(1, len(signal_mappings)) if signal_mappings else 0.0
    manual_ratio = manual_mapping_count / max(1, len(signal_mappings)) if signal_mappings else 0.0
    mapping_confidence = _round(min(1.0, coverage_ratio * 0.7 + active_ratio * 0.2 + manual_ratio * 0.1) * 100, 1)
    unmapped_signal_count = max(0, mapping_denominator - active_mapping_count)
    active_anomalies = live_asset.get("kpis", {}).get("active_anomalies", 0) if live_asset else 0
    monitoring_confidence = live_asset.get("diagnosis", {}).get("monitoring_confidence", 0.0) if live_asset else 0.0
    live_observability = live_asset.get("observability", {}) if live_asset else {}
    exporter_reachable = bool(live_observability.get("exporter_reachable", pipeline.get("exporter_up"))) if pipeline else bool(live_observability.get("exporter_reachable"))
    scrape_success = float(live_observability.get("scrape_success", pipeline.get("exporter_scrape_success", 0.0)) or 0.0) if pipeline else float(live_observability.get("scrape_success", 0.0) or 0.0)
    scrape_duration_seconds = float(live_observability.get("scrape_duration_seconds", pipeline.get("exporter_scrape_duration_seconds", 0.0)) or 0.0) if pipeline else float(live_observability.get("scrape_duration_seconds", 0.0) or 0.0)
    health_score = max(
        0.0,
        min(
            100.0,
            100.0
            - (active_anomalies * 10.0)
            - (0 if exporter_reachable else 20.0)
            - max(0.0, 1.0 - scrape_success) * 20.0,
        ),
    )

    priority_signals = sorted(
        effective_signals,
        key=lambda item: (
            {"critical": 0, "warning": 1, "nominal": 2}.get(item.get("severity", "nominal"), 3),
            -(item.get("anomaly_score", 0.0) or 0.0),
            item.get("display_name", ""),
        ),
    )[:6]

    baselines = _baseline_from_live_signals(live_asset, stored_signals)
    baseline_confidence = _round((sum(item["confidence"] for item in baselines) / len(baselines)) * 100, 1) if baselines else 0.0
    nameplate_data = nameplate_data or {}
    access_policy = access_policy or {}
    integrity_record = integrity_record or {}
    sustainability_record = sustainability_record or {}
    interfaces = nameplate_data.get("interfaces", [])
    operating_ranges = nameplate_data.get("operating_ranges", {})
    next_maintenance_due = next((item.get("next_due") for item in maintenance_events if item.get("next_due")), None)
    connections = asset.get("connections", [])
    primary_connection = asset.get("primary_connection") or (connections[0] if connections else None) or {}
    primary_connection_type = primary_connection.get("connection_type", "unknown")
    primary_config = primary_connection.get("config", {})
    collection_mode = live_observability.get("collection_mode") or ("subscription" if primary_connection_type == "mqtt" else "scrape")
    continuity_score = float(live_observability.get("continuity_score", 0.0) or 0.0)
    freshness_seconds = _freshness_seconds(live_observability.get("last_seen_at") or asset.get("last_seen_at"))
    connector_health = live_observability.get("connector_health") or _connector_health(
        exporter_reachable=exporter_reachable,
        scrape_success=scrape_success,
        scrape_duration_seconds=scrape_duration_seconds,
        continuity_score=continuity_score,
        freshness_seconds=freshness_seconds,
    )

    return {
        "schema": "machine-passport",
        "schema_version": "1.1.0",
        "passport_id": f"urn:opc-observe:machine-passport:{asset['asset_id']}",
        "identification_link": f"https://opc-observe.local/assets/{asset['asset_id']}/passport",
        "issued_at": asset.get("created_at"),
        "updated_at": asset.get("updated_at"),
        "issuer": {
            "name": asset.get("manufacturer") or "OPC Observe",
            "role": "manufacturer" if asset.get("manufacturer") else "platform",
        },
        "asset_id": asset["asset_id"],
        "identity": {
            "display_name": asset.get("display_name") or asset["asset_id"],
            "asset_type": asset.get("asset_type", "generic"),
            "manufacturer": asset.get("manufacturer") or "Unknown",
            "model": asset.get("model") or "Unknown",
            "serial_number": asset.get("serial_number") or "Unknown",
            "location": asset.get("location") or "Unassigned",
            "description": asset.get("description") or "",
            "tags": asset.get("tags", []),
        },
        "nameplate": {
            "product_name": asset.get("display_name") or asset["asset_id"],
            "manufacturer": asset.get("manufacturer") or "Unknown",
            "model": asset.get("model") or "Unknown",
            "serial_number": asset.get("serial_number") or "Unknown",
            "manufacture_date": nameplate_data.get("manufacture_date"),
            "country_of_origin": nameplate_data.get("country_of_origin") or "Unknown",
            "rated_power_kw": nameplate_data.get("rated_power_kw"),
            "interfaces": interfaces,
            "operating_ranges": operating_ranges,
        },
        "connectivity": {
            "primary_connection_type": primary_connection_type,
            "endpoint": primary_connection.get("endpoint_or_host") or asset.get("opcua_endpoint") or "Not configured",
            "security_mode": primary_connection.get("config", {}).get("security_mode") or asset.get("opcua_security_mode") or "none",
            "username": primary_connection.get("config", {}).get("username") or asset.get("opcua_username") or "",
            "broker_url": primary_config.get("broker_url") or (primary_connection.get("endpoint_or_host") if primary_connection_type == "mqtt" else ""),
            "topic_root": primary_config.get("topic_root", ""),
            "qos": primary_config.get("qos"),
            "client_id": primary_config.get("client_id", ""),
            "collection_mode": collection_mode,
            "connector_health": connector_health,
            "connection_status": asset.get("connection_status") or "unknown",
            "last_connection_check_at": asset.get("last_connection_check_at"),
            "last_discovered_at": asset.get("last_discovered_at"),
            "last_seen_at": asset.get("last_seen_at"),
            "profile_id": asset.get("profile_id") or "generic",
            "status": asset.get("status") or "draft",
            "connections": [
                {
                    "connection_type": item.get("connection_type", "unknown"),
                    "endpoint_or_host": item.get("endpoint_or_host", ""),
                    "status": item.get("status", "unknown"),
                    "is_primary": item.get("is_primary", False),
                    "topic_root": item.get("config", {}).get("topic_root", ""),
                    "client_id": item.get("config", {}).get("client_id", ""),
                }
                for item in connections
            ],
        },
        "semantic": {
            "signal_count": len(stored_signals),
            "node_count": len(stored_nodes),
            "coverage_ratio": _round(coverage_ratio * 100, 1),
            "mapping_count": len(signal_mappings),
            "active_mapping_count": active_mapping_count,
            "manual_mapping_count": manual_mapping_count,
            "inactive_mapping_count": inactive_mapping_count,
            "unmapped_signal_count": unmapped_signal_count,
            "mapping_confidence": mapping_confidence,
            "categories": dict(categories),
            "subsystems": subsystems,
            "critical_signals": [signal.get("display_name") for signal in semantic_inventory if signal.get("criticality") == "critical"][:8],
            "signals_preview": [
                {
                    "display_name": signal.get("display_name"),
                    "signal_key": signal.get("signal_key"),
                    "category": signal.get("category"),
                    "subsystem": signal.get("subsystem"),
                    "unit": signal.get("unit"),
                    "criticality": signal.get("criticality"),
                    "mapping_source": signal.get("mapping_source", "auto"),
                    "is_active": signal.get("is_active", True),
                    "source_ref": signal.get("source_ref") or signal.get("path"),
                }
                for signal in semantic_inventory[:12]
            ],
        },
        "baseline": {
            "status": "ready" if baselines else "learning",
            "confidence": baseline_confidence,
            "signals": baselines[:8],
        },
        "diagnostics": {
            "health_score": _round(health_score, 1),
            "monitoring_confidence": _round(monitoring_confidence * 100, 1),
            "active_anomalies": active_anomalies,
            "root_cause": live_asset.get("diagnosis", {}).get("root_cause", "nominal") if live_asset else "nominal",
            "top_signal": live_asset.get("diagnosis", {}).get("top_signal") if live_asset else None,
            "summary": live_asset.get("diagnosis", {}).get("summary") if live_asset else "No live diagnosis is available for this asset yet.",
            "priority_signals": [
                {
                    "display_name": signal.get("display_name"),
                    "severity": signal.get("severity", "nominal"),
                    "anomaly_score": signal.get("anomaly_score", 0.0),
                    "value": signal.get("value", signal.get("sample_value")),
                    "unit": signal.get("unit", "unknown"),
                }
                for signal in priority_signals
            ],
        },
        "maintenance": {
            "next_due": next_maintenance_due,
            "events": maintenance_events[:10],
        },
        "software": {
            "inventory_count": len(software_inventory),
            "items": software_inventory[:10],
        },
        "components": {
            "component_count": len(components),
            "items": components[:10],
        },
        "documents": {
            "document_count": len(documents),
            "items": documents[:10],
        },
        "compliance": {
            "certificate_count": len(compliance_certificates),
            "items": compliance_certificates[:10],
        },
        "access": {
            "tier": access_policy.get("access_tier", "internal"),
            "audience": access_policy.get("audience", "operators"),
            "policy_ref": access_policy.get("policy_ref", ""),
            "justification": access_policy.get("justification", ""),
            "contact": access_policy.get("contact", ""),
        },
        "integrity": {
            "revision": integrity_record.get("revision", "1"),
            "record_hash": integrity_record.get("record_hash", ""),
            "signature_ref": integrity_record.get("signature_ref", ""),
            "signed_by": integrity_record.get("signed_by", ""),
            "last_verified_at": integrity_record.get("last_verified_at"),
        },
        "sustainability": {
            "pcf_kg_co2e": sustainability_record.get("pcf_kg_co2e"),
            "energy_class": sustainability_record.get("energy_class", ""),
            "recyclable_ratio": sustainability_record.get("recyclable_ratio"),
            "takeback_available": sustainability_record.get("takeback_available", False),
            "end_of_life_instructions": sustainability_record.get("end_of_life_instructions", ""),
        },
        "custody": {
            "current_owner": ownership_events[0]["owner_name"] if ownership_events else None,
            "event_count": len(ownership_events),
            "events": ownership_events[:10],
        },
        "observability": {
            "exporter_reachable": exporter_reachable,
            "scrape_success": _round(scrape_success * 100, 1),
            "scrape_duration_seconds": scrape_duration_seconds,
            "exporter_cpu_rate": pipeline.get("exporter_cpu_rate", 0.0) if pipeline else 0.0,
            "exporter_memory_mb": pipeline.get("exporter_memory_mb", 0.0) if pipeline else 0.0,
            "analytics_cpu_rate": pipeline.get("analytics_cpu_rate", 0.0) if pipeline else 0.0,
            "analytics_memory_mb": pipeline.get("analytics_memory_mb", 0.0) if pipeline else 0.0,
            "connector_type": primary_connection_type,
            "connector_status": live_observability.get("connector_status") or asset.get("connection_status") or "unknown",
            "collection_mode": collection_mode,
            "last_seen_at": live_observability.get("last_seen_at") or asset.get("last_seen_at"),
            "freshness_seconds": _round(freshness_seconds, 1) if freshness_seconds is not None else None,
            "continuity_score": _round(continuity_score, 1),
            "continuity_label": live_observability.get("continuity_label") or ("message continuity" if primary_connection_type == "mqtt" else "sample continuity"),
            "connector_health": connector_health,
        },
        "events": events[:10],
        "notes": notes[:10],
    }
