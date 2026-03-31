import sqlite3
import threading
from pathlib import Path
from typing import Iterable, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS assets (
    asset_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    manufacturer TEXT,
    model TEXT,
    serial_number TEXT,
    location TEXT,
    description TEXT,
    opcua_endpoint TEXT,
    opcua_security_mode TEXT DEFAULT 'none',
    opcua_username TEXT,
    profile_id TEXT DEFAULT 'generic',
    status TEXT DEFAULT 'draft',
    connection_status TEXT DEFAULT 'unknown',
    last_connection_check_at TEXT,
    last_discovered_at TEXT,
    last_seen_at TEXT,
    tags_json TEXT DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_nodes (
    asset_id TEXT NOT NULL,
    nodeid TEXT NOT NULL,
    browse_name TEXT NOT NULL,
    path TEXT NOT NULL,
    namespace TEXT NOT NULL,
    sample_value REAL,
    sample_type TEXT DEFAULT 'numeric',
    discovered_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (asset_id, nodeid)
);

CREATE TABLE IF NOT EXISTS asset_connections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    connection_type TEXT NOT NULL,
    is_primary INTEGER DEFAULT 1,
    endpoint_or_host TEXT NOT NULL,
    config_json TEXT DEFAULT '{}',
    status TEXT DEFAULT 'unknown',
    last_connection_check_at TEXT,
    last_seen_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_signals (
    asset_id TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    signal TEXT NOT NULL,
    display_name TEXT NOT NULL,
    category TEXT NOT NULL,
    subsystem TEXT NOT NULL,
    unit TEXT NOT NULL,
    criticality TEXT NOT NULL,
    path TEXT NOT NULL,
    nodeid TEXT,
    namespace TEXT,
    sample_value REAL,
    source_profile TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (asset_id, signal_key)
);

CREATE TABLE IF NOT EXISTS asset_signal_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    connection_id INTEGER,
    source_ref TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    category TEXT NOT NULL,
    subsystem TEXT NOT NULL,
    unit TEXT NOT NULL,
    datatype TEXT DEFAULT 'numeric',
    criticality TEXT NOT NULL,
    mapping_source TEXT DEFAULT 'auto',
    is_active INTEGER DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_baselines (
    asset_id TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    min_value REAL,
    max_value REAL,
    avg_value REAL,
    sample_count INTEGER DEFAULT 0,
    confidence REAL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (asset_id, signal_key)
);

CREATE TABLE IF NOT EXISTS asset_passports (
    asset_id TEXT PRIMARY KEY,
    snapshot_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    detail TEXT,
    payload_json TEXT DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    note TEXT NOT NULL,
    author TEXT DEFAULT 'operator',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_nameplate (
    asset_id TEXT PRIMARY KEY,
    manufacture_date TEXT,
    country_of_origin TEXT,
    rated_power_kw REAL,
    interfaces_json TEXT DEFAULT '[]',
    operating_ranges_json TEXT DEFAULT '{}',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_components (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    name TEXT NOT NULL,
    part_number TEXT,
    supplier TEXT,
    is_replaceable INTEGER DEFAULT 1,
    criticality TEXT DEFAULT 'medium',
    notes TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_software_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    software_id TEXT NOT NULL,
    name TEXT NOT NULL,
    software_type TEXT DEFAULT 'firmware',
    version TEXT NOT NULL,
    hash TEXT DEFAULT '',
    update_channel TEXT DEFAULT '',
    support_start TEXT,
    support_end TEXT,
    sbom_ref TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    document_type TEXT NOT NULL,
    title TEXT NOT NULL,
    ref TEXT NOT NULL,
    issuer TEXT DEFAULT '',
    visibility TEXT DEFAULT 'internal',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_maintenance_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    event_at TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    result TEXT NOT NULL,
    notes TEXT DEFAULT '',
    parts_changed TEXT DEFAULT '',
    next_due TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_compliance_certificates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    certificate_type TEXT NOT NULL,
    title TEXT NOT NULL,
    ref TEXT NOT NULL,
    issuer TEXT DEFAULT '',
    valid_from TEXT,
    valid_until TEXT,
    status TEXT DEFAULT 'active',
    notes TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_access_policies (
    asset_id TEXT PRIMARY KEY,
    access_tier TEXT DEFAULT 'internal',
    audience TEXT DEFAULT 'operators',
    policy_ref TEXT DEFAULT '',
    justification TEXT DEFAULT '',
    contact TEXT DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_passport_integrity (
    asset_id TEXT PRIMARY KEY,
    revision TEXT DEFAULT '1',
    record_hash TEXT DEFAULT '',
    signature_ref TEXT DEFAULT '',
    signed_by TEXT DEFAULT '',
    last_verified_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_sustainability_records (
    asset_id TEXT PRIMARY KEY,
    pcf_kg_co2e REAL,
    energy_class TEXT DEFAULT '',
    recyclable_ratio REAL,
    takeback_available INTEGER DEFAULT 0,
    end_of_life_instructions TEXT DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_ownership_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    location TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class AssetDatabase:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(str(self.path), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._connection.executescript(SCHEMA)
            self._migrate()
            self._connection.commit()

    def _migrate(self) -> None:
        asset_columns = {row["name"] for row in self._connection.execute("PRAGMA table_info(assets)")}
        required_asset_columns = {
            "manufacturer": "ALTER TABLE assets ADD COLUMN manufacturer TEXT",
            "model": "ALTER TABLE assets ADD COLUMN model TEXT",
            "serial_number": "ALTER TABLE assets ADD COLUMN serial_number TEXT",
            "location": "ALTER TABLE assets ADD COLUMN location TEXT",
            "description": "ALTER TABLE assets ADD COLUMN description TEXT",
            "opcua_security_mode": "ALTER TABLE assets ADD COLUMN opcua_security_mode TEXT DEFAULT 'none'",
            "opcua_username": "ALTER TABLE assets ADD COLUMN opcua_username TEXT",
            "tags_json": "ALTER TABLE assets ADD COLUMN tags_json TEXT DEFAULT '[]'",
        }
        for column, statement in required_asset_columns.items():
            if column not in asset_columns:
                self._connection.execute(statement)

    def execute(self, sql: str, parameters: Iterable = ()) -> sqlite3.Cursor:
        with self._lock:
            cursor = self._connection.execute(sql, tuple(parameters))
            self._connection.commit()
        return cursor

    def executemany(self, sql: str, rows: Iterable[Iterable]) -> None:
        with self._lock:
            self._connection.executemany(sql, rows)
            self._connection.commit()

    def query_all(self, sql: str, parameters: Iterable = ()) -> list[dict]:
        with self._lock:
            cursor = self._connection.execute(sql, tuple(parameters))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def query_one(self, sql: str, parameters: Iterable = ()) -> Optional[dict]:
        with self._lock:
            cursor = self._connection.execute(sql, tuple(parameters))
            row = cursor.fetchone()
        return dict(row) if row else None
