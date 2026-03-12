# Grafana Dashboards

This directory includes minimal provisioning scaffolding for local benchmark observability.

## Structure
- `dashboards/`: place exported dashboard JSON files here.
- `provisioning/datasources/prometheus.yaml`: default Prometheus datasource.
- `provisioning/dashboards/dashboards.yaml`: file-based dashboard provider.

## Notes
- Dashboards are intentionally lightweight in this phase.
- Prometheus is expected at `http://prometheus:9090` from the Grafana container.
