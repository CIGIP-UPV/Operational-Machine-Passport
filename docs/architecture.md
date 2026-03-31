# Architecture

The stack is organized around four layers:

1. **Asset simulation**
   - `opcua-demo/` runs a configurable OPC UA server.
   - Scenarios are defined as JSON files with asset metadata, signals, and event phases.
   - Each run writes a JSONL ground-truth trace.

2. **Semantic telemetry export**
   - `exporter/` discovers numeric and boolean nodes exposed by the asset.
   - Mapping profiles convert raw browse names and node paths into semantic Prometheus metrics such as `asset_sensor_value` or `asset_maintenance_value`.
   - The exporter can now read the shared asset registry and scrape multiple OPC UA machines concurrently, emitting per-asset scrape health metrics.

3. **Monitoring and visualization**
   - Prometheus scrapes infrastructure metrics and the exporter.
   - Grafana provisions dashboards for infrastructure, OPC UA signals and analytics outputs.
   - Caddy exposes the monitoring surface.

4. **Analytics and diagnosis**
   - `analytics/` reads Prometheus data and computes rule-based and z-score anomaly scores.
   - A simple correlation model distinguishes nominal state, asset faults, observability degradation, mixed causes and exporter outages.
   - The same service now persists a machine registry in SQLite, stores semantic discovery results and rebuilds digital passports per asset.
   - Asset-specific exporter scrape metrics are used to infer observability degradation per machine rather than as a single global state.

5. **Asset registry and passports**
   - The platform keeps a persistent catalog of OPC UA machines with connection metadata, semantic profiles and discovery timestamps.
   - Each asset has a digital passport that consolidates technical identity, semantic coverage, learned baseline, diagnostics, observability quality, events and operator notes.

6. **Operator-facing UI**
   - `ui/` exposes a multi-asset React workspace.
   - The UI supports machine onboarding, registry navigation, digital passport inspection and the original operational dashboards per selected asset.

This decomposition keeps the system reusable across asset types while preserving a concrete industrial validation case.
