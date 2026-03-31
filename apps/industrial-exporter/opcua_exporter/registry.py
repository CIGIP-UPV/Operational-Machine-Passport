import json
import sqlite3
from pathlib import Path
from typing import Dict, List


def load_registered_assets(database_path: str) -> List[Dict]:
    db_path = Path(database_path)
    if not db_path.exists():
        return []

    try:
        connection = sqlite3.connect(str(db_path))
        connection.row_factory = sqlite3.Row
        has_connections = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'asset_connections'"
        ).fetchone()
        rows = []
        if has_connections:
            cursor = connection.execute(
                """
                SELECT a.asset_id, a.asset_type, a.profile_id, a.status,
                       c.connection_type, c.endpoint_or_host, c.config_json
                FROM assets a
                JOIN asset_connections c ON c.asset_id = a.asset_id
                WHERE c.is_primary = 1
                  AND a.status != 'archived'
                ORDER BY a.asset_id ASC
                """
            )
            rows = cursor.fetchall()
        if not rows:
            cursor = connection.execute(
                """
                SELECT asset_id, asset_type, profile_id, status, 'opcua' AS connection_type, opcua_endpoint AS endpoint_or_host, '{}' AS config_json
                FROM assets
                WHERE opcua_endpoint IS NOT NULL
                  AND TRIM(opcua_endpoint) != ''
                  AND status != 'archived'
                ORDER BY asset_id ASC
                """
            )
            rows = cursor.fetchall()
        connection.close()
    except sqlite3.Error:
        return []

    return [
        {
            "asset_id": row["asset_id"],
            "asset_type": row["asset_type"] or "generic",
            "connection_type": row["connection_type"] or "opcua",
            "endpoint_or_host": row["endpoint_or_host"],
            "connection_config": json.loads(row["config_json"] or "{}"),
            "profile_id": row["profile_id"] or "generic",
            "status": row["status"] or "draft",
        }
        for row in rows
    ]
