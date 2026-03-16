# ChargeSquare EV Data Engineering Case Study (Foundation)

## Purpose
This repository contains the foundation layer for an EV charging event pipeline case study.

The goal of this phase is to freeze shared contracts and local platform scaffolding so simulator, processor, and analytics implementation can proceed without structural rework.

## Frozen Architecture
Simulator -> Kafka -> Stream Processor -> Redis + ClickHouse -> Analytics/Reporting

## Technology Stack
- Python
- Docker Compose
- Kafka (transport)
- Redis (serving state only)
- ClickHouse (analytical/history system of record)
- JSON event serialization
- Optional: Prometheus + Grafana

## Frozen Contracts Included
- Kafka topics:
  - `cs.ev.events.raw`
  - `cs.ev.events.dlq`
  - `cs.ev.events.late`
- ClickHouse tables:
  - `raw_events`
  - `dead_letter_events`
  - `late_events_rejected`
  - `fact_sessions`
  - `agg_station_minute`
  - `agg_operator_hour`
  - `agg_city_day_faults`
- Redis key helpers:
  - `station:{station_id}:state`
  - `station:{station_id}:connector:{connector_id}:state`
  - `session:{session_id}:state`
  - `dedup:{event_id}`
- Canonical event envelope + payload models for:
  - `SESSION_START`
  - `METER_UPDATE`
  - `STATUS_CHANGE`
  - `SESSION_STOP`
  - `HEARTBEAT`
  - `FAULT_ALERT`

## Repository Structure
```text
.
├── Agents.md
├── Skills.md
├── README.md
├── docker-compose.yml
├── .env.example
├── config/
│   ├── simulator.default.yaml
│   ├── simulator.benchmark.yaml
│   ├── processor.default.yaml
│   ├── benchmarks/
│   │   ├── 1k.yaml
│   │   ├── 10k.yaml
│   │   ├── 50k.yaml
│   │   └── 100k.yaml
│   └── prometheus/prometheus.yml
├── src/
│   ├── benchmarks/
│   ├── common/
│   │   ├── settings.py
│   │   ├── logging.py
│   │   ├── metrics.py
│   │   ├── event_types.py
│   │   ├── topic_names.py
│   │   ├── table_names.py
│   │   ├── redis_keys.py
│   │   └── schemas/
│   │       ├── event_envelope.py
│   │       ├── event_payloads.py
│   │       └── validation.py
│   ├── simulator/main.py
│   └── processor/
├── sql/clickhouse/
│   ├── 001_create_raw_events.sql
│   ├── 002_create_dead_letter_events.sql
│   ├── 003_create_late_events_rejected.sql
│   ├── 004_create_fact_sessions.sql
│   ├── 005_create_agg_station_minute.sql
│   ├── 006_create_agg_operator_hour.sql
│   └── 007_create_agg_city_day_faults.sql
├── dashboards/grafana/
│   ├── dashboards/
│   ├── provisioning/
│   └── README.md
├── notebooks/README.md
└── tests/
    ├── unit/
    └── integration/
```

## Local Startup (Docker-First)
1. Copy env file:
   - `cp .env.example .env`
2. Start full pipeline (infra + processor + simulator):
   - `docker compose up -d --build`
3. Start the scaled 10k load-test stack (4 processor instances):
   - `docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml -f docker-compose.loadtest.scaled.yml up -d --build`
4. Start the scaled 100k load-test stack (4 processor instances):
   - `docker compose -f docker-compose.yml -f docker-compose.loadtest.100k.yml -f docker-compose.loadtest.scaled.yml up -d --build`
5. Start optional observability stack:
   - `docker compose --profile observability up -d prometheus grafana`

Runtime checks:
- `docker compose ps`
- `docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml -f docker-compose.loadtest.scaled.yml ps`
- `docker logs -f cs-processor`
- `docker logs -f cs-simulator`
- Prometheus targets: `http://localhost:9090/targets`

Shutdown commands:
- Default stack: `docker compose down --remove-orphans`
- Scaled 10k stack: `docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml -f docker-compose.loadtest.scaled.yml down --remove-orphans`
- Scaled 100k stack: `docker compose -f docker-compose.yml -f docker-compose.loadtest.100k.yml -f docker-compose.loadtest.scaled.yml down --remove-orphans`

## Python Dependencies (Optional for Host-Run)
- Recommended isolated env:
  - `python3 -m venv .venv && source .venv/bin/activate`
- `pip install kafka-python`
- `pip install redis`
- Optional for ClickHouse writes: `pip install clickhouse-driver`
- Optional for YAML-native config files: `pip install pyyaml`
- Optional for metrics endpoints: `pip install prometheus-client`

## Service Entry Points (Host-Run Fallback)
- Simulator:
  - `python -m src.simulator.main --config config/simulator.default.yaml`
  - smoke test: `python -m src.simulator.main --config config/simulator.default.yaml --max-runtime-seconds 20`
  - benchmark profile: `python -m src.simulator.main --config config/simulator.benchmark.yaml`
- Processor:
  - `python -m src.processor.main --config config/processor.default.yaml`
  - smoke loop: `python -m src.processor.main --config config/processor.default.yaml --max-loops 10`
- Metrics endpoints:
  - simulator: `http://localhost:9200/metrics`
  - processor: `http://localhost:9100/metrics`

## Benchmark Runner
- Run a tier profile:
  - `python -m src.benchmarks.run --profile config/benchmarks/1k.yaml`
- Run without query benchmark:
  - `python -m src.benchmarks.run --profile config/benchmarks/10k.yaml --skip-query-benchmark`
- Launch simulator+processor from the runner:
  - `python -m src.benchmarks.run --profile config/benchmarks/50k.yaml --launch-services`

## Heavy Load Profiles (1k / 10k / 100k EPS)
Available load profiles:

- 1k EPS sustained:
  - simulator: `config/simulator.loadtest.1k.yaml`
  - processor: `config/processor.loadtest.1k.yaml`
  - compose override: `docker-compose.loadtest.yml`
- 10k EPS sustained target:
  - simulator: `config/simulator.loadtest.10k.yaml`
  - processor: `config/processor.loadtest.10k.yaml`
  - compose override: `docker-compose.loadtest.10k.yml`
- 100k EPS stress target:
  - simulator: `config/simulator.loadtest.100k.yaml`
  - processor: `config/processor.loadtest.100k.yaml`
  - compose override: `docker-compose.loadtest.100k.yml`

Run commands:

- 1k: `docker compose -f docker-compose.yml -f docker-compose.loadtest.yml up -d --build`
- 10k: `docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml up -d --build`
- 100k: `docker compose -f docker-compose.yml -f docker-compose.loadtest.100k.yml up -d --build`
- 10k scaled: `docker compose -f docker-compose.yml -f docker-compose.loadtest.10k.yml -f docker-compose.loadtest.scaled.yml up -d --build`
- 100k scaled: `docker compose -f docker-compose.yml -f docker-compose.loadtest.100k.yml -f docker-compose.loadtest.scaled.yml up -d --build`

Scaling note:
- `docker-compose.loadtest.scaled.yml` adds `processor-b`, `processor-c`, and `processor-d`.
- Always combine the scaled override with the base compose file and one load-test override.
- The extra processor containers share the same consumer group and do not expose host ports.

How to configure/tune each tier:

- Simulator knobs:
  - `network.target_event_rate`: requested EPS target.
  - `network.station_count` and `network.connector_count_distribution`: capacity ceiling.
  - `network.tick_interval_seconds`: scheduling resolution.
  - `demand.base_session_start_rate_per_idle_connector_minute`: pressure for new sessions.
  - `session.meter_update_interval_seconds_min/max`: update frequency for active sessions.
- Processor knobs:
  - `consumer.max_poll_records`: records per poll.
  - `consumer.poll_timeout_ms`: loop wait time (lower = more responsive, higher CPU).
  - `sinks.clickhouse_batch_size` and `sinks.clickhouse_flush_interval_seconds`: sink throughput vs latency.
  - `sinks.kafka_batch_size`: throughput for DLQ/late topic writes.

How to decide if a load test is successful:

1. Let the system warm up 2 minutes, then observe at least 10 minutes.
2. Track Prometheus queries:
   - `rate(events_generated_total[1m])`
   - `rate(events_accepted_total[1m])`
   - `rate(events_accepted_total[1m]) / clamp_min(rate(events_generated_total[1m]), 1)`
   - `kafka_consumer_lag`
   - `deriv(kafka_consumer_lag[5m])`
   - `increase(clickhouse_insert_failures_total[10m])`
3. Judge result with this rubric:
   - PASS: accepted EPS is >=80% of tier target, acceptance ratio is >=0.90, lag derivative stays near 0 (or negative), and insert failures do not grow.
   - PARTIAL: accepted EPS is 50-80% of target or lag grows slowly but system remains stable (no crash loop).
   - FAIL: accepted EPS <50% of target, lag grows steeply for the full window, or services repeatedly restart.

Notes:
- 100k is a stress profile for bottleneck discovery and may not be fully reachable on a laptop.
- If lag grows persistently, increase processor sink batch sizes or reduce simulator intensity.

Quick data validation:
- `http://localhost:8123/?user=default&password=password&query=SELECT%20count()%20FROM%20raw_events`
- `http://localhost:8123/?user=default&password=password&query=SELECT%20count()%20FROM%20agg_station_minute`

Benchmark outputs are written under:
- `benchmark_results/runs/<run_id>/result.json`
- `benchmark_results/runs/<run_id>/result.csv`
- `benchmark_results/runs/<run_id>/summary.md`
- `benchmark_results/latest/`
- `benchmark_results/summary.json`
- `benchmark_results/summary.csv`
- `benchmark_results/summary.md`

## Implemented In This Phase
- Repository skeleton and modular package layout
- Frozen naming constants and shared contracts
- Canonical envelope/payload schema models
- Validation scaffolding (required fields, event type, timestamp parsing, semantic hooks)
- Docker Compose platform foundation
- ClickHouse DDL placeholders for all frozen tables
- Simulator implementation with:
  - modular station/connector/session domain model
  - configurable network generation (100+ stations, 1-4 connector distribution, operator/location/brand weights)
  - realistic lifecycle generation (`SESSION_START -> METER_UPDATE* -> SESSION_STOP`)
  - heartbeat and low-frequency fault behavior
  - duplicate/out-of-order/too-late injection hooks
  - Kafka publishing to `cs.ev.events.raw` with batching and structured error logging
  - internal metrics hooks for throughput, event counts, faults, injections, failures, active sessions
- Stream processor implementation with:
  - Kafka consume loop from `cs.ev.events.raw` with manual commit after sink flush
  - staged parse -> schema validation -> semantic validation -> dedup -> lateness classification -> routing
  - DLQ routing to Kafka `cs.ev.events.dlq` plus ClickHouse `dead_letter_events`
  - late rejection routing to ClickHouse `late_events_rejected` and optional Kafka late topic
  - Redis dedup (`dedup:{event_id}`) with in-memory fallback
  - Redis serving-state sink (`station`, `connector`, `session`) with timestamp guards and event-type lifecycle behavior
  - ClickHouse batched append sink for `raw_events`, `dead_letter_events`, `late_events_rejected`, `fact_sessions`, and frozen aggregate tables
  - processor-side session working state with deterministic finalization (`normal_stop`, `fault_termination`, `inactivity_timeout`)
  - timeout sweeper for abandoned sessions and Redis cleanup/expiry behavior for session working state
  - minimal in-memory aggregate windows flushed as finalized rows for:
    - `agg_station_minute`
    - `agg_operator_hour`
    - `agg_city_day_faults`
  - benchmark-friendly sink/finalization counters and latency/batch histograms

## Deferred To Later Phases
- Advanced retro-correction and backfill/recompute workflows
- Final polished dashboard design and submission narrative

## Troubleshooting
- `http://localhost:8123/` returning `OK` is expected. ClickHouse HTTP uses `/?query=...`.
- ClickHouse Prometheus metrics are exposed on `http://localhost:9363/metrics`.
- In Docker-first mode, Prometheus scrapes `simulator:9200` and `processor:9100` over the compose network.
- If targets are down, confirm the app containers are running:
  - `docker compose ps`
  - `docker logs --tail=100 cs-processor`
  - `docker logs --tail=100 cs-simulator`
- If processor consumes but accepts `0` with high `too_late_rejected_total`, you are likely replaying stale Kafka history.
  - test-only reset (deletes ClickHouse/Grafana volumes and restarts clean):
  - `docker compose down -v --remove-orphans`
  - `docker compose up -d --build`
