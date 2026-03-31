import sys
import tempfile
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "analytics"))

from analytics.passport import build_passport
from analytics.repository import AssetRepository


class RegistryTest(unittest.TestCase):
    def test_repository_persists_asset_and_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo = AssetRepository(Path(tmp_dir) / "assets.db")
            repo.upsert_asset(
                {
                    "asset_id": "cnc-01",
                    "display_name": "CNC 01",
                    "asset_type": "cnc",
                    "opcua_endpoint": "opc.tcp://localhost:4840/freeopcua/assets/",
                    "profile_id": "cnc",
                }
            )
            created = repo.add_note("cnc-01", "Initial operator note")
            self.assertIsNotNone(created["id"])
            notes = repo.list_notes("cnc-01")
            self.assertEqual(len(notes), 1)
            self.assertEqual(notes[0]["note"], "Initial operator note")
            repo.upsert_nameplate(
                "cnc-01",
                {
                    "manufacture_date": "2026-01-15",
                    "country_of_origin": "ES",
                    "rated_power_kw": 12.5,
                    "interfaces": ["opcua", "mqtt"],
                    "operating_ranges": {"temperatureC": {"min": 0, "max": 45}},
                },
            )
            repo.add_component(
                "cnc-01",
                {
                    "component_id": "pump-01",
                    "name": "Hydraulic Pump",
                    "part_number": "HP-001",
                    "supplier": "Supplier",
                },
            )
            repo.add_software_item(
                "cnc-01",
                {
                    "software_id": "controller-fw",
                    "name": "Controller Firmware",
                    "version": "3.2.1",
                },
            )
            repo.add_document(
                "cnc-01",
                {
                    "document_type": "manual",
                    "title": "Operating Manual",
                    "ref": "doc://manuals/cnc-01.pdf",
                },
            )
            repo.add_maintenance_event(
                "cnc-01",
                {
                    "action": "commissioning",
                    "actor": "Integrator",
                    "result": "ok",
                    "next_due": "2026-09-01",
                },
            )
            repo.add_compliance_certificate(
                "cnc-01",
                {
                    "certificate_type": "ce",
                    "title": "CE Declaration",
                    "ref": "doc://ce/cnc-01.pdf",
                },
            )
            repo.upsert_access_policy(
                "cnc-01",
                {
                    "access_tier": "legitimate_interest",
                    "audience": "service",
                    "policy_ref": "doc://policies/access.pdf",
                },
            )
            repo.upsert_integrity_record(
                "cnc-01",
                {
                    "revision": "2",
                    "record_hash": "sha256:abc",
                    "signed_by": "Quality Manager",
                },
            )
            repo.upsert_sustainability_record(
                "cnc-01",
                {
                    "pcf_kg_co2e": 42.5,
                    "energy_class": "A",
                    "recyclable_ratio": 78,
                    "takeback_available": True,
                },
            )
            repo.add_ownership_event(
                "cnc-01",
                {
                    "event_type": "commissioned",
                    "owner_name": "Plant A",
                },
            )
            self.assertEqual(repo.get_nameplate("cnc-01")["country_of_origin"], "ES")
            self.assertEqual(len(repo.list_components("cnc-01")), 1)
            self.assertEqual(len(repo.list_software_inventory("cnc-01")), 1)
            self.assertEqual(len(repo.list_documents("cnc-01")), 1)
            self.assertEqual(len(repo.list_maintenance_events("cnc-01")), 1)
            self.assertEqual(repo.get_primary_connection("cnc-01")["connection_type"], "opcua")
            self.assertEqual(len(repo.list_compliance_certificates("cnc-01")), 1)
            self.assertEqual(repo.get_access_policy("cnc-01")["access_tier"], "legitimate_interest")
            self.assertEqual(repo.get_integrity_record("cnc-01")["revision"], "2")
            self.assertEqual(repo.get_sustainability_record("cnc-01")["energy_class"], "A")
            self.assertEqual(len(repo.list_ownership_events("cnc-01")), 1)

    def test_passport_builder_includes_semantic_and_observability_sections(self) -> None:
        asset = {
            "asset_id": "cnc-01",
            "display_name": "CNC 01",
            "asset_type": "cnc",
            "profile_id": "cnc",
            "opcua_endpoint": "opc.tcp://localhost:4840/freeopcua/assets/",
            "opcua_security_mode": "none",
            "connection_status": "connected",
        }
        stored_signals = [
            {
                "signal_key": "spindle_temperature::Objects/CNC/Sensors/SpindleTemperature",
                "signal": "spindle_temperature",
                "display_name": "Spindle Temperature",
                "category": "sensor",
                "subsystem": "spindle",
                "unit": "celsius",
                "criticality": "high",
                "path": "Objects/CNC/Sensors/SpindleTemperature",
                "sample_value": 72.0,
            }
        ]
        passport = build_passport(
            asset=asset,
            nameplate_data={"manufacture_date": "2026-01-15", "country_of_origin": "ES", "interfaces": ["opcua"]},
            stored_signals=stored_signals,
            signal_mappings=[
                {
                    "signal_key": "spindle_temperature::Objects/CNC/Sensors/SpindleTemperature",
                    "display_name": "Spindle Temperature",
                    "category": "sensor",
                    "subsystem": "spindle",
                    "unit": "celsius",
                    "criticality": "high",
                    "source_ref": "Objects/CNC/Sensors/SpindleTemperature",
                    "mapping_source": "auto",
                    "is_active": True,
                }
            ],
            stored_nodes=[{"nodeid": "ns=2;i=10"}],
            live_asset=None,
            pipeline={"exporter_up": True, "exporter_scrape_success": 1.0},
            events=[],
            notes=[],
            maintenance_events=[{"action": "commissioning", "next_due": "2026-09-01"}],
            software_inventory=[{"software_id": "fw", "version": "1.0.0"}],
            components=[{"component_id": "pump-1"}],
            documents=[{"document_type": "manual"}],
            compliance_certificates=[{"certificate_type": "ce"}],
            access_policy={"access_tier": "internal"},
            integrity_record={"revision": "2"},
            sustainability_record={"energy_class": "A"},
            ownership_events=[{"owner_name": "Plant A"}],
        )
        self.assertEqual(passport["semantic"]["signal_count"], 1)
        self.assertIn("observability", passport)
        self.assertEqual(passport["connectivity"]["connection_status"], "connected")
        self.assertEqual(passport["schema"], "machine-passport")
        self.assertEqual(passport["nameplate"]["country_of_origin"], "ES")
        self.assertEqual(passport["maintenance"]["next_due"], "2026-09-01")
        self.assertEqual(passport["software"]["inventory_count"], 1)
        self.assertEqual(passport["compliance"]["certificate_count"], 1)
        self.assertEqual(passport["access"]["tier"], "internal")
        self.assertEqual(passport["integrity"]["revision"], "2")
        self.assertEqual(passport["sustainability"]["energy_class"], "A")
        self.assertEqual(passport["custody"]["current_owner"], "Plant A")
        self.assertEqual(passport["semantic"]["mapping_count"], 1)
        self.assertEqual(passport["semantic"]["active_mapping_count"], 1)
        self.assertGreaterEqual(passport["semantic"]["mapping_confidence"], 90.0)

    def test_repository_persists_mqtt_primary_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo = AssetRepository(Path(tmp_dir) / "assets.db")
            repo.upsert_asset(
                {
                    "asset_id": "cnc-mqtt-01",
                    "display_name": "CNC MQTT 01",
                    "asset_type": "cnc",
                    "profile_id": "cnc",
                    "connection_type": "mqtt",
                    "mqtt_broker_url": "mqtt://mqtt-broker:1883",
                    "mqtt_topic_root": "factory/cnc-mqtt-01",
                    "mqtt_qos": 0,
                    "mqtt_client_id": "opc-observe-cnc-mqtt-01",
                }
            )
            connection = repo.get_primary_connection("cnc-mqtt-01")
            self.assertEqual(connection["connection_type"], "mqtt")
            self.assertEqual(connection["endpoint_or_host"], "mqtt://mqtt-broker:1883")
            self.assertEqual(connection["config"]["topic_root"], "factory/cnc-mqtt-01")

    def test_passport_builder_exposes_multi_protocol_connectivity_context(self) -> None:
        asset = {
            "asset_id": "cnc-mqtt-01",
            "display_name": "CNC MQTT 01",
            "asset_type": "cnc",
            "profile_id": "cnc",
            "connection_status": "connected",
            "created_at": "2026-03-31T10:00:00+00:00",
            "updated_at": "2026-03-31T10:10:00+00:00",
            "last_seen_at": "2026-03-31T10:10:05+00:00",
            "connections": [
                {
                    "connection_type": "mqtt",
                    "endpoint_or_host": "mqtt://mqtt-broker:1883",
                    "status": "connected",
                    "is_primary": True,
                    "config": {"broker_url": "mqtt://mqtt-broker:1883", "topic_root": "factory/cnc-mqtt-01", "client_id": "demo-client"},
                }
            ],
            "primary_connection": {
                "connection_type": "mqtt",
                "endpoint_or_host": "mqtt://mqtt-broker:1883",
                "status": "connected",
                "is_primary": True,
                "config": {"broker_url": "mqtt://mqtt-broker:1883", "topic_root": "factory/cnc-mqtt-01", "client_id": "demo-client"},
            },
        }
        passport = build_passport(
            asset=asset,
            nameplate_data={"interfaces": ["mqtt"]},
            stored_signals=[],
            signal_mappings=[],
            stored_nodes=[],
            live_asset={
                "observability": {
                    "exporter_reachable": True,
                    "scrape_success": 1.0,
                    "scrape_duration_seconds": 0.6,
                    "connector_type": "mqtt",
                    "connector_status": "connected",
                    "collection_mode": "subscription",
                    "last_seen_at": "2026-03-31T10:10:05+00:00",
                    "freshness_seconds": 0.0,
                    "continuity_score": 98.0,
                    "continuity_label": "message continuity",
                    "connector_health": "healthy",
                },
                "kpis": {"active_anomalies": 0},
                "diagnosis": {"monitoring_confidence": 0.95, "root_cause": "nominal"},
            },
            pipeline={"exporter_up": True, "exporter_scrape_success": 1.0, "exporter_scrape_duration_seconds": 0.6},
            events=[],
            notes=[],
            maintenance_events=[],
            software_inventory=[],
            components=[],
            documents=[],
            compliance_certificates=[],
            access_policy={},
            integrity_record={},
            sustainability_record={},
            ownership_events=[],
        )
        self.assertEqual(passport["connectivity"]["primary_connection_type"], "mqtt")
        self.assertEqual(passport["connectivity"]["topic_root"], "factory/cnc-mqtt-01")
        self.assertEqual(passport["connectivity"]["collection_mode"], "subscription")
        self.assertEqual(passport["observability"]["connector_type"], "mqtt")
        self.assertEqual(passport["observability"]["continuity_label"], "message continuity")
        self.assertEqual(passport["observability"]["connector_health"], "healthy")

    def test_manual_signal_mapping_updates_inventory_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo = AssetRepository(Path(tmp_dir) / "assets.db")
            repo.upsert_asset(
                {
                    "asset_id": "cnc-01",
                    "display_name": "CNC 01",
                    "asset_type": "cnc",
                    "opcua_endpoint": "opc.tcp://localhost:4840/freeopcua/assets/",
                    "profile_id": "cnc",
                }
            )
            repo.upsert_signal_inventory_from_live(
                "cnc-01",
                [
                    {
                        "signal_key": "spindle_temperature::Objects/CNC/Sensors/SpindleTemperature",
                        "signal": "spindle_temperature",
                        "display_name": "Spindle Temperature",
                        "category": "sensor",
                        "subsystem": "spindle",
                        "unit": "celsius",
                        "criticality": "high",
                        "path": "Objects/CNC/Sensors/SpindleTemperature",
                        "nodeid": "ns=2;i=10",
                        "namespace": "ns2",
                        "value": 71.0,
                    }
                ],
                profile_id="cnc",
            )
            mapping = repo.list_signal_mappings("cnc-01")[0]
            updated = repo.update_signal_mapping(
                "cnc-01",
                mapping["id"],
                {
                    "signal_key": "spindle_temp_main",
                    "display_name": "Spindle Temp Main",
                    "category": "sensor",
                    "subsystem": "main_spindle",
                    "unit": "celsius",
                    "criticality": "critical",
                    "is_active": True,
                },
            )
            self.assertEqual(base_signal_key := updated["signal_key"].split("::", 1)[0], "spindle_temp_main")
            signal = repo.list_signals("cnc-01")[0]
            self.assertEqual(signal["signal"], base_signal_key)
            self.assertEqual(signal["display_name"], "Spindle Temp Main")
            self.assertEqual(signal["subsystem"], "main_spindle")


if __name__ == "__main__":
    unittest.main()
