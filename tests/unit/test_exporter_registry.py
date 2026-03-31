import sqlite3
import sys
import tempfile
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "exporter"))

from opcua_exporter.config import load_profile_config
from opcua_exporter.registry import load_registered_assets


class ExporterRegistryTest(unittest.TestCase):
    def test_load_registered_assets_reads_assets_with_endpoints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "assets.db"
            connection = sqlite3.connect(str(db_path))
            connection.executescript(
                """
                CREATE TABLE assets (
                    asset_id TEXT PRIMARY KEY,
                    asset_type TEXT,
                    opcua_endpoint TEXT,
                    profile_id TEXT,
                    status TEXT
                );
                INSERT INTO assets (asset_id, asset_type, opcua_endpoint, profile_id, status)
                VALUES ('cnc-01', 'cnc', 'opc.tcp://host:4840/freeopcua/assets/', 'cnc', 'active');
                """
            )
            connection.commit()
            connection.close()

            assets = load_registered_assets(str(db_path))
            self.assertEqual(len(assets), 1)
            self.assertEqual(assets[0]["asset_id"], "cnc-01")
            self.assertEqual(assets[0]["profile_id"], "cnc")
            self.assertEqual(assets[0]["connection_type"], "opcua")

    def test_load_registered_assets_prefers_primary_connection_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "assets.db"
            connection = sqlite3.connect(str(db_path))
            connection.executescript(
                """
                CREATE TABLE assets (
                    asset_id TEXT PRIMARY KEY,
                    asset_type TEXT,
                    opcua_endpoint TEXT,
                    profile_id TEXT,
                    status TEXT
                );
                CREATE TABLE asset_connections (
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
                INSERT INTO assets (asset_id, asset_type, opcua_endpoint, profile_id, status)
                VALUES ('cnc-02', 'cnc', '', 'cnc', 'active');
                INSERT INTO asset_connections (asset_id, connection_type, is_primary, endpoint_or_host, config_json, status, created_at, updated_at)
                VALUES ('cnc-02', 'mqtt', 1, 'mqtt://broker:1883', '{"topic_root":"factory/cnc-02"}', 'connected', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
                """
            )
            connection.commit()
            connection.close()

            assets = load_registered_assets(str(db_path))
            self.assertEqual(len(assets), 1)
            self.assertEqual(assets[0]["asset_id"], "cnc-02")
            self.assertEqual(assets[0]["connection_type"], "mqtt")
            self.assertEqual(assets[0]["connection_config"]["topic_root"], "factory/cnc-02")

    def test_load_profile_config_supports_profile_ids(self) -> None:
        config = load_profile_config(profile_id="robot")
        self.assertEqual(config["asset_type"], "robot_arm")
        self.assertGreaterEqual(len(config["rules"]), 1)


if __name__ == "__main__":
    unittest.main()
