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
│   └── processor/main.py
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

## Foundation Entry Points
- Simulator placeholder:
  - `python -m src.simulator.main --config config/simulator.default.yaml`
- Processor placeholder:
  - `python -m src.processor.main --config config/processor.default.yaml`

## Implemented In This Phase
- Repository skeleton and modular package layout
- Frozen naming constants and shared contracts
- Canonical envelope/payload schema models
- Validation scaffolding (required fields, event type, timestamp parsing, semantic hooks)
- Docker Compose platform foundation
- ClickHouse DDL placeholders for all frozen tables
- Simulator benchmark/default config scaffolding
- Minimal service boot entrypoints

## Deferred To Later Phases
- Full simulator behavior and lifecycle generation
- Kafka producer/consumer runtime logic
- Dedup, late-event, and stateful processing implementation
- Session reconstruction and aggregate materialization jobs
- Dashboard implementation and benchmark reporting
