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
│   └── prometheus/prometheus.yml
├── src/
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
├── dashboards/grafana/README.md
├── notebooks/README.md
└── tests/
    ├── unit/
    └── integration/
```

## Local Infra Startup
1. Copy env file:
   - `cp .env.example .env`
2. Start core infra:
   - `docker compose up -d zookeeper kafka kafka-init redis clickhouse`
3. Start optional observability:
   - `docker compose --profile observability up -d prometheus grafana`

## Python Dependencies
- `pip install kafka-python`
- `pip install redis`
- Optional for ClickHouse writes: `pip install clickhouse-driver`
- Optional for YAML-native config files: `pip install pyyaml`

## Service Entry Points
- Simulator:
  - `python -m src.simulator.main --config config/simulator.default.yaml`
  - smoke test: `python -m src.simulator.main --config config/simulator.default.yaml --max-runtime-seconds 20`
  - benchmark profile: `python -m src.simulator.main --config config/simulator.benchmark.yaml`
- Processor:
  - `python -m src.processor.main --config config/processor.default.yaml`
  - smoke loop: `python -m src.processor.main --config config/processor.default.yaml --max-loops 10`

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
- Dashboard implementation and benchmark reporting
