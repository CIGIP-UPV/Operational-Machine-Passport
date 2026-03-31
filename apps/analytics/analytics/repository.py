import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .storage import AssetDatabase


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AssetRepository:
    def __init__(self, database_path: Path) -> None:
        self.db = AssetDatabase(database_path)

    def _primary_connection_from_payload(self, asset_id: str, payload: Dict, existing: Optional[Dict]) -> Optional[Dict]:
        connection_type = (
            payload.get("connection_type")
            or payload.get("primary_connection_type")
            or ((existing or {}).get("primary_connection") or {}).get("connection_type")
        )
        config = dict(payload.get("connection_config") or {})
        endpoint = payload.get("opcua_endpoint") or config.get("endpoint_or_host") or config.get("endpoint") or ""

        if connection_type == "mqtt":
            endpoint = (
                payload.get("mqtt_broker_url")
                or config.get("broker_url")
                or config.get("endpoint_or_host")
                or endpoint
            )
            config.setdefault("broker_url", endpoint)
            topic_root = payload.get("mqtt_topic_root") or config.get("topic_root") or ""
            if topic_root:
                config["topic_root"] = topic_root
            qos = payload.get("mqtt_qos")
            if qos is not None and qos != "":
                config["qos"] = int(qos)
            client_id = payload.get("mqtt_client_id") or config.get("client_id") or ""
            if client_id:
                config["client_id"] = client_id
            username = payload.get("mqtt_username") or config.get("username") or ""
            if username:
                config["username"] = username
            password = payload.get("mqtt_password") or config.get("password") or ""
            if password:
                config["password"] = password
        elif connection_type == "opcua" or endpoint:
            connection_type = connection_type or "opcua"
            endpoint = endpoint or (existing or {}).get("opcua_endpoint") or ""
            config.setdefault("endpoint", endpoint)
            if payload.get("opcua_security_mode") or (existing or {}).get("opcua_security_mode"):
                config["security_mode"] = payload.get("opcua_security_mode") or (existing or {}).get("opcua_security_mode") or "none"
            if payload.get("opcua_username") or (existing or {}).get("opcua_username"):
                config["username"] = payload.get("opcua_username") or (existing or {}).get("opcua_username") or ""
        else:
            return None

        if not endpoint:
            return None

        return {
            "asset_id": asset_id,
            "connection_type": connection_type,
            "endpoint_or_host": endpoint,
            "config": config,
            "status": payload.get("connection_status") or ((existing or {}).get("primary_connection") or {}).get("status") or "unknown",
            "last_connection_check_at": payload.get("last_connection_check_at") or ((existing or {}).get("primary_connection") or {}).get("last_connection_check_at"),
            "last_seen_at": payload.get("last_seen_at") or ((existing or {}).get("primary_connection") or {}).get("last_seen_at"),
        }

    def list_assets(self) -> List[Dict]:
        rows = self.db.query_all(
            """
            SELECT *
            FROM assets
            ORDER BY COALESCE(last_seen_at, updated_at) DESC, asset_id ASC
            """
        )
        return [self._decode_asset(row) for row in rows]

    def get_asset(self, asset_id: str) -> Optional[Dict]:
        row = self.db.query_one("SELECT * FROM assets WHERE asset_id = ?", (asset_id,))
        return self._decode_asset(row) if row else None

    def list_connections(self, asset_id: str) -> List[Dict]:
        rows = self.db.query_all(
            """
            SELECT id, asset_id, connection_type, is_primary, endpoint_or_host, config_json, status,
                   last_connection_check_at, last_seen_at, created_at, updated_at
            FROM asset_connections
            WHERE asset_id = ?
            ORDER BY is_primary DESC, id ASC
            """,
            (asset_id,),
        )
        for row in rows:
            row["is_primary"] = bool(row.get("is_primary"))
            row["config"] = json.loads(row.pop("config_json") or "{}")
        return rows

    def get_primary_connection(self, asset_id: str) -> Optional[Dict]:
        rows = self.list_connections(asset_id)
        return rows[0] if rows else None

    def upsert_connection(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        config = payload.get("config", {}) or {}
        is_primary = bool(payload.get("is_primary", True))
        existing = self.db.query_one(
            """
            SELECT id
            FROM asset_connections
            WHERE asset_id = ? AND connection_type = ? AND endpoint_or_host = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (asset_id, payload["connection_type"], payload["endpoint_or_host"]),
        )
        if is_primary:
            self.db.execute("UPDATE asset_connections SET is_primary = 0 WHERE asset_id = ?", (asset_id,))
        if existing:
            self.db.execute(
                """
                UPDATE asset_connections
                SET is_primary = ?, config_json = ?, status = ?, last_connection_check_at = ?, last_seen_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    1 if is_primary else 0,
                    json.dumps(config),
                    payload.get("status", "unknown"),
                    payload.get("last_connection_check_at"),
                    payload.get("last_seen_at"),
                    now,
                    existing["id"],
                ),
            )
        else:
            self.db.execute(
                """
                INSERT INTO asset_connections (
                    asset_id, connection_type, is_primary, endpoint_or_host, config_json, status,
                    last_connection_check_at, last_seen_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset_id,
                    payload["connection_type"],
                    1 if is_primary else 0,
                    payload["endpoint_or_host"],
                    json.dumps(config),
                    payload.get("status", "unknown"),
                    payload.get("last_connection_check_at"),
                    payload.get("last_seen_at"),
                    now,
                    now,
                ),
            )
        return self.get_primary_connection(asset_id) if is_primary else self.list_connections(asset_id)[-1]

    def replace_signal_mappings(self, asset_id: str, mappings: Iterable[Dict], connection_id: Optional[int] = None) -> None:
        self.db.execute("DELETE FROM asset_signal_mappings WHERE asset_id = ?", (asset_id,))
        now = utc_now()
        self.db.executemany(
            """
            INSERT INTO asset_signal_mappings (
                asset_id, connection_id, source_ref, signal_key, display_name, category, subsystem,
                unit, datatype, criticality, mapping_source, is_active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    asset_id,
                    connection_id,
                    item.get("path") or item.get("source_ref") or item.get("signal_key"),
                    item["signal_key"],
                    item.get("display_name") or item["signal_key"],
                    item.get("category", "signal"),
                    item.get("subsystem", "unknown"),
                    item.get("unit", "unknown"),
                    item.get("datatype") or ("boolean" if item.get("unit") == "boolean" else "numeric"),
                    item.get("criticality", "medium"),
                    item.get("mapping_source", "auto"),
                    1,
                    now,
                    now,
                )
                for item in mappings
            ),
        )

    def list_signal_mappings(self, asset_id: str) -> List[Dict]:
        rows = self.db.query_all(
            """
            SELECT id, connection_id, source_ref, signal_key, display_name, category, subsystem, unit, datatype,
                   criticality, mapping_source, is_active, created_at, updated_at
            FROM asset_signal_mappings
            WHERE asset_id = ?
            ORDER BY category ASC, display_name ASC
            """,
            (asset_id,),
        )
        for row in rows:
            row["is_active"] = bool(row.get("is_active"))
        return rows

    def get_signal_mapping(self, asset_id: str, mapping_id: int) -> Optional[Dict]:
        row = self.db.query_one(
            """
            SELECT id, connection_id, source_ref, signal_key, display_name, category, subsystem, unit, datatype,
                   criticality, mapping_source, is_active, created_at, updated_at
            FROM asset_signal_mappings
            WHERE asset_id = ? AND id = ?
            """,
            (asset_id, mapping_id),
        )
        if row:
            row["is_active"] = bool(row.get("is_active"))
        return row

    def _propagate_mapping_to_signal_inventory(self, asset_id: str, previous_mapping: Dict, mapping: Dict) -> None:
        path = previous_mapping.get("source_ref") or mapping.get("source_ref") or ""
        if not path:
            return

        signal_key = mapping["signal_key"]
        if "::" not in signal_key:
            signal_key = f"{signal_key}::{path}"
        signal_name = signal_key.split("::", 1)[0]
        old_signal_key = previous_mapping.get("signal_key")

        self.db.execute(
            """
            UPDATE asset_signals
            SET signal_key = ?, signal = ?, display_name = ?, category = ?, subsystem = ?, unit = ?, criticality = ?
            WHERE asset_id = ? AND path = ?
            """,
            (
                signal_key,
                signal_name,
                mapping.get("display_name") or signal_name,
                mapping.get("category", "signal"),
                mapping.get("subsystem", "unknown"),
                mapping.get("unit", "unknown"),
                mapping.get("criticality", "medium"),
                asset_id,
                path,
            ),
        )

        if old_signal_key and old_signal_key != signal_key:
            self.db.execute(
                """
                UPDATE asset_baselines
                SET signal_key = ?
                WHERE asset_id = ? AND signal_key = ?
                """,
                (signal_key, asset_id, old_signal_key),
            )

    def update_signal_mapping(self, asset_id: str, mapping_id: int, payload: Dict) -> Dict:
        current = self.get_signal_mapping(asset_id, mapping_id)
        if not current:
            raise KeyError(f"Unknown signal mapping {mapping_id} for asset {asset_id}")

        now = utc_now()
        source_ref = current.get("source_ref", "")
        signal_key_base = payload.get("signal_key") or current.get("signal_key", "").split("::", 1)[0]
        signal_key = signal_key_base if "::" in signal_key_base else f"{signal_key_base}::{source_ref}"
        merged = {
            "source_ref": source_ref,
            "signal_key": signal_key,
            "display_name": payload.get("display_name") or current.get("display_name") or signal_key_base,
            "category": payload.get("category") or current.get("category") or "signal",
            "subsystem": payload.get("subsystem") or current.get("subsystem") or "unknown",
            "unit": payload.get("unit") or current.get("unit") or "unknown",
            "datatype": payload.get("datatype") or current.get("datatype") or "numeric",
            "criticality": payload.get("criticality") or current.get("criticality") or "medium",
            "mapping_source": payload.get("mapping_source") or "manual",
            "is_active": bool(payload.get("is_active")) if "is_active" in payload else bool(current.get("is_active")),
        }

        self.db.execute(
            """
            UPDATE asset_signal_mappings
            SET signal_key = ?, display_name = ?, category = ?, subsystem = ?, unit = ?, datatype = ?,
                criticality = ?, mapping_source = ?, is_active = ?, updated_at = ?
            WHERE asset_id = ? AND id = ?
            """,
            (
                merged["signal_key"],
                merged["display_name"],
                merged["category"],
                merged["subsystem"],
                merged["unit"],
                merged["datatype"],
                merged["criticality"],
                merged["mapping_source"],
                1 if merged["is_active"] else 0,
                now,
                asset_id,
                mapping_id,
            ),
        )

        self._propagate_mapping_to_signal_inventory(asset_id, current, merged)
        return self.get_signal_mapping(asset_id, mapping_id) or {}

    def upsert_asset(self, payload: Dict) -> Dict:
        existing = self.get_asset(payload["asset_id"])
        existing_nameplate = self.get_nameplate(payload["asset_id"]) if existing else None
        now = utc_now()
        merged = {
            "asset_id": payload["asset_id"],
            "display_name": payload.get("display_name") or payload["asset_id"],
            "asset_type": payload.get("asset_type") or (existing or {}).get("asset_type") or "generic",
            "manufacturer": payload.get("manufacturer") or (existing or {}).get("manufacturer"),
            "model": payload.get("model") or (existing or {}).get("model"),
            "serial_number": payload.get("serial_number") or (existing or {}).get("serial_number"),
            "location": payload.get("location") or (existing or {}).get("location"),
            "description": payload.get("description") or (existing or {}).get("description"),
            "opcua_endpoint": payload.get("opcua_endpoint") or (existing or {}).get("opcua_endpoint"),
            "opcua_security_mode": payload.get("opcua_security_mode") or (existing or {}).get("opcua_security_mode") or "none",
            "opcua_username": payload.get("opcua_username") or (existing or {}).get("opcua_username"),
            "profile_id": payload.get("profile_id") or (existing or {}).get("profile_id") or "generic",
            "status": payload.get("status") or (existing or {}).get("status") or "draft",
            "connection_status": payload.get("connection_status") or (existing or {}).get("connection_status") or "unknown",
            "last_connection_check_at": payload.get("last_connection_check_at") or (existing or {}).get("last_connection_check_at"),
            "last_discovered_at": payload.get("last_discovered_at") or (existing or {}).get("last_discovered_at"),
            "last_seen_at": payload.get("last_seen_at") or (existing or {}).get("last_seen_at"),
            "tags_json": json.dumps(payload.get("tags") if "tags" in payload else (existing or {}).get("tags", [])),
            "created_at": (existing or {}).get("created_at") or now,
            "updated_at": now,
        }
        self.db.execute(
            """
            INSERT INTO assets (
                asset_id, display_name, asset_type, manufacturer, model, serial_number, location, description,
                opcua_endpoint, opcua_security_mode, opcua_username, profile_id, status, connection_status,
                last_connection_check_at, last_discovered_at, last_seen_at, tags_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id) DO UPDATE SET
                display_name=excluded.display_name,
                asset_type=excluded.asset_type,
                manufacturer=excluded.manufacturer,
                model=excluded.model,
                serial_number=excluded.serial_number,
                location=excluded.location,
                description=excluded.description,
                opcua_endpoint=excluded.opcua_endpoint,
                opcua_security_mode=excluded.opcua_security_mode,
                opcua_username=excluded.opcua_username,
                profile_id=excluded.profile_id,
                status=excluded.status,
                connection_status=excluded.connection_status,
                last_connection_check_at=excluded.last_connection_check_at,
                last_discovered_at=excluded.last_discovered_at,
                last_seen_at=excluded.last_seen_at,
                tags_json=excluded.tags_json,
                updated_at=excluded.updated_at
            """,
            (
                merged["asset_id"],
                merged["display_name"],
                merged["asset_type"],
                merged["manufacturer"],
                merged["model"],
                merged["serial_number"],
                merged["location"],
                merged["description"],
                merged["opcua_endpoint"],
                merged["opcua_security_mode"],
                merged["opcua_username"],
                merged["profile_id"],
                merged["status"],
                merged["connection_status"],
                merged["last_connection_check_at"],
                merged["last_discovered_at"],
                merged["last_seen_at"],
                merged["tags_json"],
                merged["created_at"],
                merged["updated_at"],
            ),
        )
        nameplate_fields = {"manufacture_date", "country_of_origin", "rated_power_kw", "interfaces", "operating_ranges"}
        if existing_nameplate or any(field in payload for field in nameplate_fields):
            self.upsert_nameplate(
                payload["asset_id"],
                {
                    "manufacture_date": payload.get("manufacture_date", (existing_nameplate or {}).get("manufacture_date")),
                    "country_of_origin": payload.get("country_of_origin", (existing_nameplate or {}).get("country_of_origin")),
                    "rated_power_kw": payload.get("rated_power_kw", (existing_nameplate or {}).get("rated_power_kw")),
                    "interfaces": payload.get("interfaces", (existing_nameplate or {}).get("interfaces", [])),
                    "operating_ranges": payload.get("operating_ranges", (existing_nameplate or {}).get("operating_ranges", {})),
                },
            )
        connection_payload = self._primary_connection_from_payload(payload["asset_id"], payload, existing)
        if connection_payload:
            self.upsert_connection(payload["asset_id"], connection_payload)
        return self.get_asset(payload["asset_id"]) or self._decode_asset(merged)

    def upsert_live_asset(self, asset_payload: Dict) -> Dict:
        asset_id = asset_payload["asset_id"]
        existing = self.get_asset(asset_id)
        display_name = (existing or {}).get("display_name") or asset_id
        status = asset_payload.get("status") or (existing or {}).get("status") or "active"
        now = utc_now()
        asset = self.upsert_asset(
            {
                "asset_id": asset_id,
                "display_name": display_name,
                "asset_type": asset_payload.get("asset_type") or (existing or {}).get("asset_type") or "generic",
                "profile_id": (existing or {}).get("profile_id") or asset_payload.get("asset_type") or "generic",
                "status": status if status != "draft" else "active",
                "connection_status": asset_payload.get("connection", {}).get("connector_status") or "connected",
                "last_seen_at": now,
            }
        )
        primary_connection = (existing or {}).get("primary_connection") or asset.get("primary_connection")
        if primary_connection:
            self.upsert_connection(
                asset_id,
                {
                    "connection_type": primary_connection["connection_type"],
                    "endpoint_or_host": primary_connection["endpoint_or_host"],
                    "config": primary_connection.get("config", {}),
                    "status": asset_payload.get("connection", {}).get("connector_status") or "connected",
                    "last_connection_check_at": primary_connection.get("last_connection_check_at"),
                    "last_seen_at": now,
                    "is_primary": True,
                },
            )
        return self.get_asset(asset_id) or asset

    def save_discovery(self, asset_id: str, discovery: Dict, profile_id: str) -> None:
        now = utc_now()
        asset = self.get_asset(asset_id)
        if not asset:
            raise KeyError(f"Unknown asset {asset_id}")
        primary_connection = self.get_primary_connection(asset_id)

        self.upsert_asset(
            {
                "asset_id": asset_id,
                "display_name": asset["display_name"],
                "asset_type": asset.get("asset_type") or discovery.get("asset_type") or "generic",
                "manufacturer": asset.get("manufacturer"),
                "model": asset.get("model"),
                "serial_number": asset.get("serial_number"),
                "location": asset.get("location"),
                "description": asset.get("description"),
                "opcua_endpoint": asset.get("opcua_endpoint"),
                "opcua_security_mode": asset.get("opcua_security_mode") or "none",
                "opcua_username": asset.get("opcua_username"),
                "profile_id": profile_id,
                "status": "active",
                "connection_status": "connected",
                "last_connection_check_at": now,
                "last_discovered_at": now,
                "last_seen_at": asset.get("last_seen_at"),
                "tags": asset.get("tags", []),
            }
        )

        self.db.execute("DELETE FROM asset_nodes WHERE asset_id = ?", (asset_id,))
        self.db.execute("DELETE FROM asset_signals WHERE asset_id = ?", (asset_id,))
        self.db.executemany(
            """
            INSERT INTO asset_nodes (
                asset_id, nodeid, browse_name, path, namespace, sample_value, sample_type, discovered_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    asset_id,
                    node["nodeid"],
                    node["browse_name"],
                    node["path"],
                    node["namespace"],
                    node.get("sample_value"),
                    node.get("sample_type", "numeric"),
                    now,
                    now,
                )
                for node in discovery.get("nodes", [])
            ),
        )
        self.db.executemany(
            """
            INSERT INTO asset_signals (
                asset_id, signal_key, signal, display_name, category, subsystem, unit, criticality, path, nodeid,
                namespace, sample_value, source_profile, discovered_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    asset_id,
                    signal["signal_key"],
                    signal["signal"],
                    signal["display_name"],
                    signal["category"],
                    signal["subsystem"],
                    signal["unit"],
                    signal["criticality"],
                    signal["path"],
                    signal.get("nodeid"),
                    signal.get("namespace"),
                    signal.get("sample_value"),
                    profile_id,
                    now,
                    now,
                )
                for signal in discovery.get("signals", [])
            ),
        )
        self.replace_signal_mappings(
            asset_id,
            [
                {
                    **signal,
                    "source_ref": signal.get("path", ""),
                    "datatype": signal.get("sample_type", "numeric"),
                    "mapping_source": "auto",
                }
                for signal in discovery.get("signals", [])
            ],
            connection_id=primary_connection.get("id") if primary_connection else None,
        )

    def list_nodes(self, asset_id: str) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT nodeid, browse_name, path, namespace, sample_value, sample_type, discovered_at, last_seen_at
            FROM asset_nodes
            WHERE asset_id = ?
            ORDER BY path ASC
            """,
            (asset_id,),
        )

    def list_signals(self, asset_id: str) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT signal_key, signal, display_name, category, subsystem, unit, criticality, path, nodeid, namespace,
                   sample_value, source_profile, discovered_at, last_seen_at
            FROM asset_signals
            WHERE asset_id = ?
            ORDER BY category ASC, display_name ASC
            """,
            (asset_id,),
        )

    def upsert_nameplate(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        interfaces = payload.get("interfaces", [])
        if isinstance(interfaces, str):
            interfaces = [item.strip() for item in interfaces.split(",") if item.strip()]
        operating_ranges = payload.get("operating_ranges", {}) or {}
        self.db.execute(
            """
            INSERT INTO asset_nameplate (
                asset_id, manufacture_date, country_of_origin, rated_power_kw, interfaces_json, operating_ranges_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id) DO UPDATE SET
                manufacture_date=excluded.manufacture_date,
                country_of_origin=excluded.country_of_origin,
                rated_power_kw=excluded.rated_power_kw,
                interfaces_json=excluded.interfaces_json,
                operating_ranges_json=excluded.operating_ranges_json,
                updated_at=excluded.updated_at
            """,
            (
                asset_id,
                payload.get("manufacture_date"),
                payload.get("country_of_origin"),
                payload.get("rated_power_kw"),
                json.dumps(interfaces),
                json.dumps(operating_ranges),
                now,
            ),
        )
        return self.get_nameplate(asset_id) or {}

    def get_nameplate(self, asset_id: str) -> Optional[Dict]:
        row = self.db.query_one(
            """
            SELECT asset_id, manufacture_date, country_of_origin, rated_power_kw, interfaces_json, operating_ranges_json, updated_at
            FROM asset_nameplate
            WHERE asset_id = ?
            """,
            (asset_id,),
        )
        if not row:
            return None
        row["interfaces"] = json.loads(row.pop("interfaces_json") or "[]")
        row["operating_ranges"] = json.loads(row.pop("operating_ranges_json") or "{}")
        return row

    def add_component(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        cursor = self.db.execute(
            """
            INSERT INTO asset_components (
                asset_id, component_id, name, part_number, supplier, is_replaceable, criticality, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                payload["component_id"],
                payload["name"],
                payload.get("part_number"),
                payload.get("supplier"),
                1 if payload.get("is_replaceable", True) else 0,
                payload.get("criticality", "medium"),
                payload.get("notes", ""),
                now,
                now,
            ),
        )
        return {
            "id": cursor.lastrowid,
            "asset_id": asset_id,
            "component_id": payload["component_id"],
            "name": payload["name"],
            "part_number": payload.get("part_number"),
            "supplier": payload.get("supplier"),
            "is_replaceable": bool(payload.get("is_replaceable", True)),
            "criticality": payload.get("criticality", "medium"),
            "notes": payload.get("notes", ""),
            "created_at": now,
            "updated_at": now,
        }

    def list_components(self, asset_id: str) -> List[Dict]:
        rows = self.db.query_all(
            """
            SELECT id, component_id, name, part_number, supplier, is_replaceable, criticality, notes, created_at, updated_at
            FROM asset_components
            WHERE asset_id = ?
            ORDER BY id DESC
            """,
            (asset_id,),
        )
        for row in rows:
            row["is_replaceable"] = bool(row.get("is_replaceable"))
        return rows

    def add_software_item(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        cursor = self.db.execute(
            """
            INSERT INTO asset_software_inventory (
                asset_id, software_id, name, software_type, version, hash, update_channel, support_start, support_end, sbom_ref, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                payload["software_id"],
                payload["name"],
                payload.get("software_type", "firmware"),
                payload["version"],
                payload.get("hash", ""),
                payload.get("update_channel", ""),
                payload.get("support_start"),
                payload.get("support_end"),
                payload.get("sbom_ref", ""),
                now,
                now,
            ),
        )
        return {
            "id": cursor.lastrowid,
            "asset_id": asset_id,
            "software_id": payload["software_id"],
            "name": payload["name"],
            "software_type": payload.get("software_type", "firmware"),
            "version": payload["version"],
            "hash": payload.get("hash", ""),
            "update_channel": payload.get("update_channel", ""),
            "support_start": payload.get("support_start"),
            "support_end": payload.get("support_end"),
            "sbom_ref": payload.get("sbom_ref", ""),
            "created_at": now,
            "updated_at": now,
        }

    def list_software_inventory(self, asset_id: str) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT id, software_id, name, software_type, version, hash, update_channel, support_start, support_end, sbom_ref, created_at, updated_at
            FROM asset_software_inventory
            WHERE asset_id = ?
            ORDER BY id DESC
            """,
            (asset_id,),
        )

    def add_document(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        cursor = self.db.execute(
            """
            INSERT INTO asset_documents (
                asset_id, document_type, title, ref, issuer, visibility, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                payload["document_type"],
                payload["title"],
                payload["ref"],
                payload.get("issuer", ""),
                payload.get("visibility", "internal"),
                now,
                now,
            ),
        )
        return {
            "id": cursor.lastrowid,
            "asset_id": asset_id,
            "document_type": payload["document_type"],
            "title": payload["title"],
            "ref": payload["ref"],
            "issuer": payload.get("issuer", ""),
            "visibility": payload.get("visibility", "internal"),
            "created_at": now,
            "updated_at": now,
        }

    def list_documents(self, asset_id: str) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT id, document_type, title, ref, issuer, visibility, created_at, updated_at
            FROM asset_documents
            WHERE asset_id = ?
            ORDER BY id DESC
            """,
            (asset_id,),
        )

    def add_maintenance_event(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        event_at = payload.get("event_at") or now
        cursor = self.db.execute(
            """
            INSERT INTO asset_maintenance_events (
                asset_id, event_at, action, actor, result, notes, parts_changed, next_due, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                event_at,
                payload["action"],
                payload.get("actor", "operator"),
                payload.get("result", "ok"),
                payload.get("notes", ""),
                payload.get("parts_changed", ""),
                payload.get("next_due"),
                now,
                now,
            ),
        )
        return {
            "id": cursor.lastrowid,
            "asset_id": asset_id,
            "event_at": event_at,
            "action": payload["action"],
            "actor": payload.get("actor", "operator"),
            "result": payload.get("result", "ok"),
            "notes": payload.get("notes", ""),
            "parts_changed": payload.get("parts_changed", ""),
            "next_due": payload.get("next_due"),
            "created_at": now,
            "updated_at": now,
        }

    def list_maintenance_events(self, asset_id: str, limit: int = 50) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT id, event_at, action, actor, result, notes, parts_changed, next_due, created_at, updated_at
            FROM asset_maintenance_events
            WHERE asset_id = ?
            ORDER BY event_at DESC, id DESC
            LIMIT ?
            """,
            (asset_id, limit),
        )

    def add_compliance_certificate(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        cursor = self.db.execute(
            """
            INSERT INTO asset_compliance_certificates (
                asset_id, certificate_type, title, ref, issuer, valid_from, valid_until, status, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                payload["certificate_type"],
                payload["title"],
                payload["ref"],
                payload.get("issuer", ""),
                payload.get("valid_from"),
                payload.get("valid_until"),
                payload.get("status", "active"),
                payload.get("notes", ""),
                now,
                now,
            ),
        )
        return {
            "id": cursor.lastrowid,
            "asset_id": asset_id,
            "certificate_type": payload["certificate_type"],
            "title": payload["title"],
            "ref": payload["ref"],
            "issuer": payload.get("issuer", ""),
            "valid_from": payload.get("valid_from"),
            "valid_until": payload.get("valid_until"),
            "status": payload.get("status", "active"),
            "notes": payload.get("notes", ""),
            "created_at": now,
            "updated_at": now,
        }

    def list_compliance_certificates(self, asset_id: str) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT id, certificate_type, title, ref, issuer, valid_from, valid_until, status, notes, created_at, updated_at
            FROM asset_compliance_certificates
            WHERE asset_id = ?
            ORDER BY id DESC
            """,
            (asset_id,),
        )

    def upsert_access_policy(self, asset_id: str, payload: Dict) -> Dict:
        current = self.get_access_policy(asset_id) or {}
        now = utc_now()
        merged = {
            "access_tier": payload.get("access_tier") or current.get("access_tier") or "internal",
            "audience": payload.get("audience") or current.get("audience") or "operators",
            "policy_ref": payload.get("policy_ref") or current.get("policy_ref") or "",
            "justification": payload.get("justification") or current.get("justification") or "",
            "contact": payload.get("contact") or current.get("contact") or "",
        }
        self.db.execute(
            """
            INSERT INTO asset_access_policies (
                asset_id, access_tier, audience, policy_ref, justification, contact, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id) DO UPDATE SET
                access_tier=excluded.access_tier,
                audience=excluded.audience,
                policy_ref=excluded.policy_ref,
                justification=excluded.justification,
                contact=excluded.contact,
                updated_at=excluded.updated_at
            """,
            (
                asset_id,
                merged["access_tier"],
                merged["audience"],
                merged["policy_ref"],
                merged["justification"],
                merged["contact"],
                now,
            ),
        )
        return self.get_access_policy(asset_id) or {}

    def get_access_policy(self, asset_id: str) -> Optional[Dict]:
        return self.db.query_one(
            """
            SELECT asset_id, access_tier, audience, policy_ref, justification, contact, updated_at
            FROM asset_access_policies
            WHERE asset_id = ?
            """,
            (asset_id,),
        )

    def upsert_integrity_record(self, asset_id: str, payload: Dict) -> Dict:
        current = self.get_integrity_record(asset_id) or {}
        now = utc_now()
        merged = {
            "revision": payload.get("revision") or current.get("revision") or "1",
            "record_hash": payload.get("record_hash") or current.get("record_hash") or "",
            "signature_ref": payload.get("signature_ref") or current.get("signature_ref") or "",
            "signed_by": payload.get("signed_by") or current.get("signed_by") or "",
            "last_verified_at": payload.get("last_verified_at") or current.get("last_verified_at"),
        }
        self.db.execute(
            """
            INSERT INTO asset_passport_integrity (
                asset_id, revision, record_hash, signature_ref, signed_by, last_verified_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id) DO UPDATE SET
                revision=excluded.revision,
                record_hash=excluded.record_hash,
                signature_ref=excluded.signature_ref,
                signed_by=excluded.signed_by,
                last_verified_at=excluded.last_verified_at,
                updated_at=excluded.updated_at
            """,
            (
                asset_id,
                merged["revision"],
                merged["record_hash"],
                merged["signature_ref"],
                merged["signed_by"],
                merged["last_verified_at"],
                now,
            ),
        )
        return self.get_integrity_record(asset_id) or {}

    def get_integrity_record(self, asset_id: str) -> Optional[Dict]:
        return self.db.query_one(
            """
            SELECT asset_id, revision, record_hash, signature_ref, signed_by, last_verified_at, updated_at
            FROM asset_passport_integrity
            WHERE asset_id = ?
            """,
            (asset_id,),
        )

    def upsert_sustainability_record(self, asset_id: str, payload: Dict) -> Dict:
        current = self.get_sustainability_record(asset_id) or {}
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO asset_sustainability_records (
                asset_id, pcf_kg_co2e, energy_class, recyclable_ratio, takeback_available, end_of_life_instructions, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id) DO UPDATE SET
                pcf_kg_co2e=excluded.pcf_kg_co2e,
                energy_class=excluded.energy_class,
                recyclable_ratio=excluded.recyclable_ratio,
                takeback_available=excluded.takeback_available,
                end_of_life_instructions=excluded.end_of_life_instructions,
                updated_at=excluded.updated_at
            """,
            (
                asset_id,
                payload.get("pcf_kg_co2e", current.get("pcf_kg_co2e")),
                payload.get("energy_class", current.get("energy_class", "")),
                payload.get("recyclable_ratio", current.get("recyclable_ratio")),
                1 if payload.get("takeback_available", current.get("takeback_available", False)) else 0,
                payload.get("end_of_life_instructions", current.get("end_of_life_instructions", "")),
                now,
            ),
        )
        return self.get_sustainability_record(asset_id) or {}

    def get_sustainability_record(self, asset_id: str) -> Optional[Dict]:
        row = self.db.query_one(
            """
            SELECT asset_id, pcf_kg_co2e, energy_class, recyclable_ratio, takeback_available, end_of_life_instructions, updated_at
            FROM asset_sustainability_records
            WHERE asset_id = ?
            """,
            (asset_id,),
        )
        if row:
            row["takeback_available"] = bool(row.get("takeback_available"))
        return row

    def add_ownership_event(self, asset_id: str, payload: Dict) -> Dict:
        now = utc_now()
        effective_at = payload.get("effective_at") or now
        cursor = self.db.execute(
            """
            INSERT INTO asset_ownership_events (
                asset_id, event_type, owner_name, effective_at, location, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                payload["event_type"],
                payload["owner_name"],
                effective_at,
                payload.get("location", ""),
                payload.get("notes", ""),
                now,
                now,
            ),
        )
        return {
            "id": cursor.lastrowid,
            "asset_id": asset_id,
            "event_type": payload["event_type"],
            "owner_name": payload["owner_name"],
            "effective_at": effective_at,
            "location": payload.get("location", ""),
            "notes": payload.get("notes", ""),
            "created_at": now,
            "updated_at": now,
        }

    def list_ownership_events(self, asset_id: str, limit: int = 50) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT id, event_type, owner_name, effective_at, location, notes, created_at, updated_at
            FROM asset_ownership_events
            WHERE asset_id = ?
            ORDER BY effective_at DESC, id DESC
            LIMIT ?
            """,
            (asset_id, limit),
        )

    def upsert_signal_inventory_from_live(self, asset_id: str, signals: Iterable[Dict], profile_id: str = "live") -> None:
        now = utc_now()
        signal_list = list(signals)
        rows = []
        node_rows = []
        for signal in signal_list:
            rows.append(
                (
                    asset_id,
                    signal["signal_key"],
                    signal["signal"],
                    signal.get("display_name") or signal["signal"],
                    signal.get("category", "signal"),
                    signal.get("subsystem", "unknown"),
                    signal.get("unit", "unknown"),
                    signal.get("criticality", "medium"),
                    signal.get("path", ""),
                    signal.get("nodeid"),
                    signal.get("namespace"),
                    signal.get("value"),
                    profile_id,
                    now,
                    now,
                )
            )
            if signal.get("nodeid"):
                node_rows.append(
                    (
                        asset_id,
                        signal["nodeid"],
                        signal.get("display_name") or signal["signal"],
                        signal.get("path", ""),
                        signal.get("namespace", "unknown"),
                        signal.get("value"),
                        "numeric",
                        now,
                        now,
                    )
                )
        self.db.executemany(
            """
            INSERT INTO asset_signals (
                asset_id, signal_key, signal, display_name, category, subsystem, unit, criticality, path, nodeid,
                namespace, sample_value, source_profile, discovered_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_id, signal_key) DO UPDATE SET
                display_name=excluded.display_name,
                category=excluded.category,
                subsystem=excluded.subsystem,
                unit=excluded.unit,
                criticality=excluded.criticality,
                path=excluded.path,
                nodeid=excluded.nodeid,
                namespace=excluded.namespace,
                sample_value=excluded.sample_value,
                source_profile=excluded.source_profile,
                last_seen_at=excluded.last_seen_at
            """,
            rows,
        )
        if node_rows:
                self.db.executemany(
                    """
                    INSERT INTO asset_nodes (
                    asset_id, nodeid, browse_name, path, namespace, sample_value, sample_type, discovered_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id, nodeid) DO UPDATE SET
                    browse_name=excluded.browse_name,
                    path=excluded.path,
                    namespace=excluded.namespace,
                    sample_value=excluded.sample_value,
                    last_seen_at=excluded.last_seen_at
                    """,
                    node_rows,
                )
        self.sync_signal_mappings_from_live(asset_id, signal_list)

    def sync_signal_mappings_from_live(self, asset_id: str, signals: Iterable[Dict]) -> None:
        now = utc_now()
        primary_connection = self.get_primary_connection(asset_id)
        connection_id = primary_connection.get("id") if primary_connection else None
        existing_by_source = {row["source_ref"]: row for row in self.list_signal_mappings(asset_id)}

        for signal in signals:
            source_ref = signal.get("path", "")
            if not source_ref:
                continue
            signal_key = signal.get("signal_key") or f"{signal.get('signal', 'signal')}::{source_ref}"
            auto_payload = {
                "source_ref": source_ref,
                "signal_key": signal_key,
                "display_name": signal.get("display_name") or signal.get("signal") or source_ref,
                "category": signal.get("category", "signal"),
                "subsystem": signal.get("subsystem", "unknown"),
                "unit": signal.get("unit", "unknown"),
                "datatype": "boolean" if signal.get("unit") == "boolean" else "numeric",
                "criticality": signal.get("criticality", "medium"),
            }
            current = existing_by_source.get(source_ref)
            if current:
                if current.get("mapping_source") == "manual":
                    self._propagate_mapping_to_signal_inventory(asset_id, current, current)
                    continue
                self.db.execute(
                    """
                    UPDATE asset_signal_mappings
                    SET connection_id = ?, signal_key = ?, display_name = ?, category = ?, subsystem = ?, unit = ?,
                        datatype = ?, criticality = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        connection_id,
                        auto_payload["signal_key"],
                        auto_payload["display_name"],
                        auto_payload["category"],
                        auto_payload["subsystem"],
                        auto_payload["unit"],
                        auto_payload["datatype"],
                        auto_payload["criticality"],
                        now,
                        current["id"],
                    ),
                )
            else:
                self.db.execute(
                    """
                    INSERT INTO asset_signal_mappings (
                        asset_id, connection_id, source_ref, signal_key, display_name, category, subsystem, unit,
                        datatype, criticality, mapping_source, is_active, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        asset_id,
                        connection_id,
                        auto_payload["source_ref"],
                        auto_payload["signal_key"],
                        auto_payload["display_name"],
                        auto_payload["category"],
                        auto_payload["subsystem"],
                        auto_payload["unit"],
                        auto_payload["datatype"],
                        auto_payload["criticality"],
                        "auto",
                        1,
                        now,
                        now,
                    ),
                )

    def replace_baselines(self, asset_id: str, baselines: Iterable[Dict]) -> None:
        self.db.execute("DELETE FROM asset_baselines WHERE asset_id = ?", (asset_id,))
        now = utc_now()
        self.db.executemany(
            """
            INSERT INTO asset_baselines (
                asset_id, signal_key, min_value, max_value, avg_value, sample_count, confidence, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    asset_id,
                    item["signal_key"],
                    item.get("min_value"),
                    item.get("max_value"),
                    item.get("avg_value"),
                    item.get("sample_count", 0),
                    item.get("confidence", 0.0),
                    now,
                )
                for item in baselines
            ),
        )

    def list_baselines(self, asset_id: str) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT signal_key, min_value, max_value, avg_value, sample_count, confidence, updated_at
            FROM asset_baselines
            WHERE asset_id = ?
            ORDER BY signal_key ASC
            """,
            (asset_id,),
        )

    def replace_passport(self, asset_id: str, snapshot: Dict) -> None:
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO asset_passports (asset_id, snapshot_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(asset_id) DO UPDATE SET
                snapshot_json=excluded.snapshot_json,
                updated_at=excluded.updated_at
            """,
            (asset_id, json.dumps(snapshot), now),
        )

    def get_passport(self, asset_id: str) -> Optional[Dict]:
        row = self.db.query_one("SELECT snapshot_json, updated_at FROM asset_passports WHERE asset_id = ?", (asset_id,))
        if not row:
            return None
        snapshot = json.loads(row["snapshot_json"])
        snapshot["updated_at"] = row["updated_at"]
        return snapshot

    def add_event(self, asset_id: str, event_type: str, severity: str, title: str, detail: str = "", payload: Optional[Dict] = None) -> None:
        self.db.execute(
            """
            INSERT INTO asset_events (asset_id, event_type, severity, title, detail, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (asset_id, event_type, severity, title, detail, json.dumps(payload or {}), utc_now()),
        )

    def list_events(self, asset_id: str, limit: int = 50) -> List[Dict]:
        rows = self.db.query_all(
            """
            SELECT id, event_type, severity, title, detail, payload_json, created_at
            FROM asset_events
            WHERE asset_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (asset_id, limit),
        )
        for row in rows:
            row["payload"] = json.loads(row.pop("payload_json") or "{}")
        return rows

    def add_note(self, asset_id: str, note: str, author: str = "operator") -> Dict:
        created_at = utc_now()
        cursor = self.db.execute(
            """
            INSERT INTO asset_notes (asset_id, note, author, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (asset_id, note, author, created_at),
        )
        note_id = cursor.lastrowid
        return {
            "id": note_id,
            "asset_id": asset_id,
            "note": note,
            "author": author,
            "created_at": created_at,
        }

    def list_notes(self, asset_id: str, limit: int = 50) -> List[Dict]:
        return self.db.query_all(
            """
            SELECT id, note, author, created_at
            FROM asset_notes
            WHERE asset_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (asset_id, limit),
        )

    def add_snapshot(self, asset_id: str, snapshot: Dict) -> None:
        self.db.execute(
            """
            INSERT INTO asset_snapshots (asset_id, snapshot_json, created_at)
            VALUES (?, ?, ?)
            """,
            (asset_id, json.dumps(snapshot), utc_now()),
        )

    def list_snapshots(self, asset_id: str, limit: int = 20) -> List[Dict]:
        rows = self.db.query_all(
            """
            SELECT id, snapshot_json, created_at
            FROM asset_snapshots
            WHERE asset_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (asset_id, limit),
        )
        for row in rows:
            row["snapshot"] = json.loads(row.pop("snapshot_json") or "{}")
        return rows

    def _decode_asset(self, row: Dict) -> Dict:
        decoded = dict(row)
        decoded["tags"] = json.loads(decoded.pop("tags_json", "[]") or "[]")
        decoded["nameplate"] = self.get_nameplate(decoded["asset_id"]) or {}
        decoded["connections"] = self.list_connections(decoded["asset_id"])
        decoded["primary_connection"] = decoded["connections"][0] if decoded["connections"] else None
        return decoded
