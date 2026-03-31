# Reproducibility Notes

The repository is designed so the main artifact can be reproduced from source:

- local Docker builds are used for the simulator, exporter and analytics
- the baseline OPC UA scenarios are versioned in `simulators/opcua/config/scenarios/`
- the MQTT baseline scenarios are versioned in `simulators/mqtt/config/scenarios/`
- multiple asset scenarios are versioned, including CNC and robot-arm
- the exporter profile is versioned in `apps/industrial-exporter/opcua_exporter/config/profiles/`
- Prometheus and Grafana are provisioned from repository files
- experiment outputs are generated under `experiments/results/`

For a publishable artifact, keep these principles:

- avoid manual UI configuration
- version every scenario and mapping profile
- store raw traces and analysis scripts
- pin image and dependency versions
- run the same campaign multiple times with fixed seeds
- export summary tables and flattened datasets from the same raw results
