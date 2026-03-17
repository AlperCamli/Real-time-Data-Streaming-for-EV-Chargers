# ChargeSquare Local Runbook

## Purpose
Quick operator guide to start, validate, benchmark, and troubleshoot the local EV pipeline:

`Simulator -> Kafka -> Processor -> Redis + ClickHouse -> Prometheus/Grafana`

## Prerequisites
- Docker Desktop (or Docker Engine + Compose plugin)
- Open ports: `3000`, `6379`, `8123`, `9000`, `9090`, `9100`, `9200`, `29092`
- Clone repo and enter project root
- Initialize env file:

```bash
cp .env.example .env
```

Optional (host benchmark runner):
- Python 3.12 + packages used by this repo (`kafka-python`, `redis`, `clickhouse-driver`, `pyyaml`, `prometheus-client`)

## Start the System
Start full stack:

```bash
docker compose up -d --build
docker compose ps
```

Expected core containers:
- `cs-zookeeper`, `cs-kafka`, `cs-kafka-init`
- `cs-redis`, `cs-clickhouse`
- `cs-processor`, `cs-simulator`
- `cs-prometheus`, `cs-grafana`

Tail critical logs:

```bash
docker logs -f cs-processor
docker logs -f cs-simulator
```

## Verify Ingestion Is Working
Check service endpoints:

```bash
curl -sSf http://localhost:9100/metrics >/dev/null && echo "processor metrics OK"
curl -sSf http://localhost:9200/metrics >/dev/null && echo "simulator metrics OK"
curl -sSf http://localhost:9090/-/healthy && echo
```

Check event counters are moving:

```bash
curl -s http://localhost:9200/metrics | grep '^events_generated_total'
curl -s http://localhost:9100/metrics | grep '^events_accepted_total'
```

## Inspect Kafka Flow
List and describe topics:

```bash
docker exec -it cs-kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec -it cs-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic cs.ev.events.raw
```

Inspect processor consumer lag:

```bash
docker exec -it cs-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group cs-processor
```

Peek raw topic payloads:

```bash
docker exec -it cs-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cs.ev.events.raw \
  --max-messages 5 --timeout-ms 10000
```

Peek DLQ payloads:

```bash
docker exec -it cs-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cs.ev.events.dlq \
  --max-messages 5 --timeout-ms 10000
```

## Verify Redis State
Scan key families:

```bash
docker exec -it cs-redis redis-cli --scan --pattern 'station:*:state' | head
docker exec -it cs-redis redis-cli --scan --pattern 'session:*:state' | head
docker exec -it cs-redis redis-cli --scan --pattern 'dedup:*' | head
```

Read one station hash:

```bash
STATION_KEY=$(docker exec cs-redis redis-cli --raw --scan --pattern 'station:*:state' | head -n 1)
echo "$STATION_KEY"
docker exec cs-redis redis-cli HGETALL "$STATION_KEY"
```

## Verify ClickHouse Writes
`sql/clickhouse/*.sql` is auto-applied on first startup of a fresh ClickHouse volume.

Check tables:

```bash
curl -s "http://localhost:8123/?user=default&password=password&query=SHOW%20TABLES"
```

Check row counts:

```bash
curl -s "http://localhost:8123/?user=default&password=password&query=SELECT%20count()%20FROM%20raw_events"
curl -s "http://localhost:8123/?user=default&password=password&query=SELECT%20count()%20FROM%20fact_sessions"
curl -s "http://localhost:8123/?user=default&password=password&query=SELECT%20count()%20FROM%20dead_letter_events"
curl -s "http://localhost:8123/?user=default&password=password&query=SELECT%20count()%20FROM%20late_events_rejected"
```

Quick analytical check:

```bash
curl -s "http://localhost:8123/?user=default&password=password&query=SELECT%20event_type,%20count()%20FROM%20raw_events%20GROUP%20BY%20event_type%20ORDER%20BY%20count()%20DESC"
```

## Run Benchmark Profiles
### 1k profile
Bring up 1k loadtest config:

```bash
docker compose -f docker-compose.yml -f docker-compose.loadtest.yml up -d --build
```

Run benchmark runner from container:

```bash
docker compose run --rm processor python -m src.benchmarks.run \
  --profile config/benchmarks/1k.yaml \
  --simulator-metrics-url http://simulator:9200/metrics \
  --processor-metrics-url http://processor:9100/metrics
```

### 10k profile (single + scaled options)
Single-instance override:

```bash
docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml up -d --build
```

Scaled experiment (multi-worker + second simulator):

```bash
docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml -f docker-compose.loadtest.scaled.yml up -d --build
```

Benchmark outputs (if runner is used):
- `benchmark_results/runs/<run_id>/result.json`
- `benchmark_results/runs/<run_id>/result.csv`
- `benchmark_results/runs/<run_id>/summary.md`

For long sustained validation (for example 25 minutes), keep the stack running and observe Grafana/Prometheus while counters and lag remain stable.

## Inspect Dashboards / Metrics
- Grafana: `http://localhost:3000` (`admin` / `Jm7TRdE@mZYZ98`)
- Prometheus: `http://localhost:9090`
- Processor metrics: `http://localhost:9100/metrics`
- Simulator metrics: `http://localhost:9200/metrics`

Grafana dashboards:
- `ChargeSquare Pipeline Overview`
- `ChargeSquare ClickHouse + Sinks`

Prometheus target health:

```bash
curl -s http://localhost:9090/api/v1/targets | grep -E '"health":"up"|"scrapeUrl"'
```

## Multi-Worker Processor Runs
Use scaled compose overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml -f docker-compose.loadtest.scaled.yml up -d --build
docker compose ps
```

Look for:
- `cs-processor`, `cs-processor-b`, `cs-processor-c`, `cs-processor-d`
- `cs-simulator`, `cs-simulator-b`

Check per-worker logs:

```bash
docker logs --tail 100 cs-processor
docker logs --tail 100 cs-processor-b
docker logs --tail 100 cs-processor-c
docker logs --tail 100 cs-processor-d
```

Consumer group lag remains the primary readiness signal under scaled load:

```bash
docker exec -it cs-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group cs-processor
```

## Troubleshooting
### 1) Kafka topics missing / no ingestion
- Check `cs-kafka-init` completion:

```bash
docker logs cs-kafka-init
```

- Re-run startup if init failed:

```bash
docker compose down --remove-orphans
docker compose up -d --build
```

### 2) Grafana panels empty
- Check Prometheus targets and metrics endpoints (`9100`, `9200`, `9363` scrape via Prometheus).
- Confirm processor/simulator are running and not crash-looping:

```bash
docker compose ps
docker logs --tail 200 cs-processor
docker logs --tail 200 cs-simulator
```

### 3) ClickHouse tables missing
- DDL init runs only on first startup of a fresh ClickHouse volume.
- Reset volumes to re-run init scripts:

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
```

### 4) Consumer lag grows continuously
- Scale processors (use scaled compose overlay).
- Reduce simulator pressure and verify simulator EPS control.
- Check processor hot-path logs/metrics and Redis/ClickHouse latency panels.

### 5) Benchmark runner fails on host
- Use containerized runner command instead of host Python dependency setup.

## Stop / Reset
Stop stack:

```bash
docker compose down --remove-orphans
```

Stop and remove volumes (full reset):

```bash
docker compose down -v --remove-orphans
```
