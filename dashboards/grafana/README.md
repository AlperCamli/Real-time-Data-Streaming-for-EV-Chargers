# Grafana Dashboards

This directory includes minimal provisioning scaffolding for local benchmark observability.

## Structure
- `dashboards/`: place exported dashboard JSON files here.
- `provisioning/datasources/prometheus.yaml`: default Prometheus datasource.
- `provisioning/dashboards/dashboards.yaml`: file-based dashboard provider.

## Notes
- Dashboards are intentionally lightweight in this phase.
- Prometheus is expected at `http://prometheus:9090` from the Grafana container.
- Provisioned dashboards:
  - `ChargeSquare Pipeline Overview`
  - `ChargeSquare ClickHouse + Sinks`

## Local Access
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`

## Troubleshooting
- If Grafana panels are empty, check Prometheus targets first:
  - `http://localhost:9090/targets`
- `simulator` and `processor` targets are only `UP` while Python services are running.
- `clickhouse` target is scraped from `clickhouse:9363` (container network).

ClickHouse HTTP endpoint note:
- `http://localhost:8123/` is not a UI page; it is an HTTP API endpoint.
- Run SQL over HTTP, for example:
  - `http://localhost:8123/?user=default&password=password&query=SHOW%20TABLES`
