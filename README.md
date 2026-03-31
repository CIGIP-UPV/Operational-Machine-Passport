# Machine Passport

Machine Passport is a multi-protocol industrial observability platform for machine onboarding, live monitoring, anomaly diagnosis, and digital machine passports. It is designed to work with heterogeneous industrial assets through a common semantic model, with **OPC UA** as the primary validation protocol and **MQTT** as the second supported connector. The platform combines machine telemetry, infrastructure observability, semantic normalization, anomaly detection, and a React-based user interface that lets operators register machines, inspect signals, review diagnosis, and maintain a living passport for each asset.

The project is built around three ideas. First, industrial data should be **interoperable**: the diagnosis and passport should not depend on the input protocol, but on normalized signals such as temperature, vibration, feed rate, machine state, or maintenance indicators. Second, observability should be **cross-layer**: the platform correlates process-side anomalies with monitoring-path degradation to distinguish between asset issues and observability issues. Third, machine information should be **persistent and operationally useful**: beyond dashboards, each asset has a digital passport with identity, connectivity, semantic mappings, baseline behavior, components, software, maintenance, compliance, sustainability, and custody records.

## What the application does

At runtime, the platform can:

- register industrial machines from the UI
- connect to machines through OPC UA or MQTT
- test connectivity and run discovery
- normalize discovered telemetry to a shared semantic model
- export live machine signals to Prometheus
- compute anomaly scores with rule-based, z-score, and MAD detectors
- provide basic explainable diagnosis and cross-layer IT/OT context
- build and update a living digital machine passport per asset
- manage multiple assets at once from a single dashboard

The result is one application serving five roles at the same time:

- **machine registry**
- **industrial connectivity layer**
- **live monitoring dashboard**
- **anomaly diagnosis workspace**
- **digital machine passport system**

## Main capabilities

### 1. Multi-machine registry

The UI includes a registry-first workflow. Users can browse the list of registered machines, add a new one, test its connection, and open its passport and monitoring views. Example assets are seeded automatically for demo purposes:

- `cnc-01` over OPC UA
- `cnc-02` over OPC UA
- `cnc-mqtt-01` over MQTT

### 2. Multi-protocol connectivity

The application currently supports:

- **OPC UA**
- **MQTT**

Each asset stores a generic connection model:

- connection type
- endpoint or broker
- protocol-specific configuration
- connection status
- last seen timestamp
- collection mode

This keeps the rest of the system protocol-agnostic.

### 3. Semantic normalization

Signals discovered from OPC UA nodes or MQTT topics are mapped into a common semantic inventory:

- signal key
- display name
- category
- subsystem
- unit
- datatype
- criticality
- source reference

Mappings can be reviewed and manually corrected from the UI. The passport tracks mapping coverage, active/inactive mappings, manual overrides, and mapping confidence.

### 4. Live monitoring

The `Monitor` workspace provides:

- a diagnosis banner with live confidence and current anomaly context
- KPI cards for anomaly count, tracked signals, detector votes, and health score
- `Signals at Risk`
- `Pipeline State`
- `Signals Explorer`
- `Operator Context`

The same UI works for OPC UA and MQTT assets because it consumes the normalized model instead of protocol-specific assumptions.

### 5. Anomaly detection and diagnosis

The analytics service runs three detectors:

- rule-based thresholds
- rolling z-score
- rolling MAD

It produces:

- anomaly score per signal
- detector vote totals
- monitoring confidence
- root cause hint
- evidences for diagnosis

The diagnosis layer also incorporates connector context such as:

- protocol
- collection mode
- connector health
- continuity score
- freshness

This helps separate:

- asset faults
- monitoring degradation
- mixed conditions
- observability outages

### 6. Digital machine passport

Each asset has a living machine passport with structured sections:

- **Overview**
- **Semantic**
- **Technical**
- **Maintenance**
- **Compliance**
- **Sustainability**

The passport currently covers:

- identity and technical nameplate
- protocol/connectivity context
- semantic inventory and mappings
- operational baseline
- health and diagnosis summary
- structured maintenance events
- software and firmware inventory
- critical components / light BoM
- linked technical documents
- compliance certificates
- access policy and integrity metadata
- sustainability records
- ownership / custody history

## Architecture overview

The stack is composed of these main services:

- `apps/analytics/`
  - registry, discovery, passport builder, anomaly detection, and HTTP API
- `apps/industrial-exporter/`
  - industrial semantic exporter for OPC UA and MQTT assets
- `apps/ui/`
  - React frontend for machine registry, passports, monitor, and diagnose
- `simulators/opcua/`
  - OPC UA simulators with reproducible scenarios and ground truth
- `simulators/mqtt/`
  - MQTT machine simulator with reproducible CNC scenarios
- `infra/mqtt-broker/`
  - local Mosquitto broker for the MQTT demo path
- `infra/prometheus/`
  - scraping and alerting rules
- `infra/grafana/`
  - dashboards for infrastructure and telemetry inspection
- `infra/caddy/`
  - reverse proxy and entrypoint for the web surface
- `experiments/`
  - repeatable experiment runner and evaluation scripts
- `tests/`
  - unit and integration checks

## User interface

The current UI is organized into three main areas:

### Machines

This is the parent view for the platform.

It lets you:

- see the list of registered assets
- add a new machine
- test connection
- run discovery
- review the machine passport

### Monitor

This is the operational view for the selected machine.

It lets you:

- inspect high-risk signals
- review pipeline health
- explore live signals
- open a detailed signal modal
- inspect connector context and operator notes

### Diagnose

This is the diagnosis and validation workspace.

It lets you:

- inspect evidences
- review detector outputs
- analyze diagnosis quality
- compare scenario ground truth against detected states

## Quick start

Run the complete stack:

```bash
docker compose up -d --build
```

On Linux hosts where cAdvisor is supported, you can enable the optional profile:

```bash
docker compose --profile linux-monitoring up -d --build
```

## Main endpoints

- UI: `http://localhost:4000`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9093`
- Pushgateway: `http://localhost:9091`
- OPC UA CNC 1: `opc.tcp://localhost:4840/freeopcua/assets/`
- OPC UA CNC 2: `opc.tcp://localhost:4841/freeopcua/assets/`
- MQTT broker on host: `mqtt://localhost:1884`
- MQTT topic root: `factory/cnc-mqtt-01/#`

Default credentials for protected HTTP endpoints:

- user: `admin`
- password: `admin`

## Built-in demo assets

The platform auto-registers demo assets so the UI is populated from the first start:

- `cnc-01`
  - OPC UA
  - `opc.tcp://opcua-simulator:4840/freeopcua/assets/`
- `cnc-02`
  - OPC UA
  - `opc.tcp://opcua-simulator-cnc-02:4840/freeopcua/assets/`
- `cnc-mqtt-01`
  - MQTT
  - `mqtt://mqtt-broker:1883`
  - topic root `factory/cnc-mqtt-01`

For external clients running on the host:

- use `opc.tcp://localhost:4840/freeopcua/assets/` and `opc.tcp://localhost:4841/freeopcua/assets/` for OPC UA
- use `mqtt://localhost:1884` for MQTT

## Development notes

Useful commands:

```bash
python3 -m unittest discover -s tests
```

```bash
docker compose ps
```

```bash
docker compose logs -f analytics opcua-exporter ui
```

```bash
npm run build --prefix apps/ui
```

## Research and publication angle

This repository is also structured as a research artifact for:

- unified IT/OT observability
- machine interoperability
- semantic industrial telemetry
- early anomaly detection
- explainable diagnosis
- digital machine passports

The same codebase can therefore be used as:

- a live demo platform
- an experimental benchmark environment
- a machine registry and passport prototype
- a multi-protocol observability testbed

## Current scope

The platform is already strong as an **operational machine passport** and **observability system**. It is especially useful for:

- onboarding assets
- validating semantic mappings
- monitoring industrial behavior
- demonstrating diagnosis over OPC UA and MQTT
- studying how machine telemetry and monitoring-path health interact

Future work can keep extending interoperability and lifecycle coverage, but the current repository already provides an end-to-end, running implementation of a multi-machine, multi-protocol monitoring and passport platform.
