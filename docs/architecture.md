# Architecture

The stack is organized around six layers:

1. **Asset simulation**
   - `simulators/opcua/` runs configurable OPC UA servers.
   - `simulators/mqtt/` publishes reproducible MQTT machine telemetry.
   - Scenarios are defined as JSON files with asset metadata, signals, and event phases.
   - Each run writes a JSONL ground-truth trace.

2. **Semantic telemetry export**
   - `apps/industrial-exporter/` discovers or subscribes to machine telemetry exposed by the asset.
   - Mapping profiles convert raw browse names and node paths into semantic Prometheus metrics such as `asset_sensor_value` or `asset_maintenance_value`.
   - The exporter reads the shared asset registry and ingests multiple assets concurrently, emitting per-asset scrape and connector health metrics.

3. **Monitoring and visualization**
   - `infra/prometheus/` scrapes infrastructure metrics and the exporter.
   - `infra/grafana/` provisions dashboards for infrastructure, industrial signals and analytics outputs.
   - `infra/caddy/` exposes the monitoring surface.

4. **Analytics and diagnosis**
   - `apps/analytics/` reads Prometheus data and computes rule-based and z-score anomaly scores.
   - A simple correlation model distinguishes nominal state, asset faults, observability degradation, mixed causes and exporter outages.
   - The same service now persists a machine registry in SQLite, stores semantic discovery results and rebuilds digital passports per asset.
   - Asset-specific exporter scrape metrics are used to infer observability degradation per machine rather than as a single global state.

5. **Asset registry and passports**
   - The platform keeps a persistent catalog of industrial machines with connection metadata, semantic profiles and discovery timestamps.
   - Each asset has a digital passport that consolidates technical identity, semantic coverage, learned baseline, diagnostics, observability quality, events and operator notes.

6. **Operator-facing UI**
   - `apps/ui/` exposes a multi-asset React workspace.
   - The UI supports machine onboarding, registry navigation, digital passport inspection and the original operational dashboards per selected asset.

This decomposition keeps the system reusable across asset types while preserving a concrete industrial validation case.
