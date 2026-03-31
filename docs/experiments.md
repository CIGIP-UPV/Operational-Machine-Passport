# Experiment Workflow

An experiment is defined by:

- a simulator scenario
- environment overrides for the Docker stack
- a duration and warm-up window
- a set of Prometheus queries to export as results
- optional fault actions applied to services during the run
- a repetition count for statistical aggregation

Reference campaigns:

- `experiments/scenarios/cnc_cross_layer.json`
- `experiments/scenarios/robot_joint_wear.json`

## Execution flow

1. `docker compose up -d --build`
2. Wait for Prometheus readiness
3. Let the simulator advance through nominal and faulty phases
4. Inject optional IT faults such as exporter restart or pause
5. Export range queries from Prometheus
6. Persist raw run results and ground truth under `experiments/results/`
7. Aggregate repeated runs into summary CSV/JSON
8. Optionally flatten the dataset for downstream notebooks or papers

## Recommended experimental extensions

- add more simulator profiles for different asset classes
- inject infrastructure stress while OT faults are active
- compare detectors and thresholds on the same ground truth
- compute latency, precision and false positives offline from exported traces

## Current quantitative outputs

The repository now includes:

- `experiments/evaluate_results.py` for classification metrics and detection latency
- `experiments/export_dataset.py` for row-oriented datasets
- automatic capture of exporter and analytics CPU/memory overhead through Prometheus
