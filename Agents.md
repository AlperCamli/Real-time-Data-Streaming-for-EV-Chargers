# Agents.md

## 1. Project Overview
- Project name
- Short project purpose
- Case-study objective
- Final frozen architecture
- Success principle: prioritize strong engineering judgment over brute-force completeness

## 2. Global Project Constraints
- Time constraint: 5–7 day case study
- Core architecture is frozen and should not be reopened casually
- Python-first implementation direction
- Docker Compose as minimum orchestration
- Redis = serving state only
- ClickHouse = analytical/history system of record
- Kafka included as transport
- JSON as primary serialization
- Prometheus/Grafana allowed
- Avoid overengineering and low-ROI complexity

## 3. Global Frozen Decisions
### 3.1 Architecture
- Simulator → Kafka → Stream Processor → Redis + ClickHouse → Analytics/Reporting

### 3.2 Kafka Topics
- `cs.ev.events.raw`
- `cs.ev.events.dlq`
- optional: `cs.ev.events.late`

### 3.3 ClickHouse Tables
- `raw_events`
- `dead_letter_events`
- `late_events_rejected`
- `fact_sessions`
- `agg_station_minute`
- `agg_operator_hour`
- `agg_city_day_faults`

### 3.4 Redis Keys
- `station:{station_id}:state`
- `station:{station_id}:connector:{connector_id}:state`
- `session:{session_id}:state`
- `dedup:{event_id}`

### 3.5 Event Types
- `SESSION_START`
- `METER_UPDATE`
- `STATUS_CHANGE`
- `SESSION_STOP`
- `HEARTBEAT`
- `FAULT_ALERT`

### 3.6 Event-Time / Dedup / Late Policy
- business time = `event_time`
- dedup key = `event_id`
- dedup via Redis TTL
- duplicates counted and discarded
- too-late rejected events go only to `late_events_rejected`
- Redis freshness is timestamp-driven
- `fact_sessions` is append-once
- no retro-correction from ultra-late rejected events

## 4. Agent Operating Model
- Each agent owns one bounded concern
- Each agent must respect frozen contracts
- Each agent may refine details inside its scope
- Each agent must state assumptions clearly
- Each agent must not introduce new infrastructure unless justified
- Each agent must optimize for score-to-effort ratio

## 5. Standard Agent Template
For every agent, define the following:

### 5.1 Agent Name
- Example: Data Generator Agent

### 5.2 Mission
- One paragraph describing what the agent is responsible for

### 5.3 Responsibilities
- Explicit list of what the agent must design/build/define

### 5.4 Non-Goals
- Explicit list of what the agent must not do

### 5.5 Inputs
- Frozen architecture/contracts
- Outputs from previous agents
- Relevant requirements from the case study

### 5.6 Outputs
- What artifacts/specs/code/designs the agent must produce

### 5.7 Dependencies
- Which prior agent outputs this agent depends on

### 5.8 Downstream Consumers
- Which later agents or implementation prompts use this output

### 5.9 Success Criteria
- Measurable acceptance criteria for the agent

### 5.10 Risks / Failure Modes
- Common ways this agent might overengineer, drift, or underdeliver

## 6. Agent Catalog

### 6.1 Product Manager Agent
#### Mission
- Own total project quality, sequencing, scope control, and frozen decisions.

#### Responsibilities
- define project phases
- write and review agent prompts
- freeze architecture and contract decisions
- control scope and prioritization
- evaluate agent outputs against case-study score goals

#### Non-Goals
- not the main coding agent
- not responsible for low-level implementation unless needed for clarification

#### Inputs
- case-study brief
- all agent outputs
- benchmark, storage, processor, and simulator contracts

#### Outputs
- project plan
- agent prompts
- frozen decisions
- acceptance/review notes
- Codex handoff plan

#### Success Criteria
- all major design decisions are frozen before implementation
- implementation prompts are sharp and consistent
- low redesign risk

---

### 6.2 Phase 0 Architecture Definition Agent
#### Mission
- Freeze the architecture contract before coding begins.

#### Responsibilities
- final architecture diagram
- canonical event contract
- naming contract
- benchmark tiers
- explicit non-goals

#### Non-Goals
- not writing implementation code
- not designing every detailed table/metric

#### Inputs
- case-study brief
- PM priorities

#### Outputs
- architecture contract
- topic/table naming
- benchmark framing
- non-goals

#### Success Criteria
- downstream agents can proceed without reopening core design debates

---

### 6.3 Data Generator Agent
#### Mission
- Design the EV charging simulator that produces realistic, configurable, benchmarkable events.

#### Responsibilities
- station/session/connector simulation model
- realistic event lifecycle
- peak-hour demand
- fault behavior
- YAML/JSON configuration
- event schema examples
- benchmark-aware generation plan

#### Non-Goals
- not responsible for consumer/storage logic
- not responsible for analytics/reporting
- not responsible for Redis/ClickHouse writes beyond transport output

#### Inputs
- frozen event contract
- architecture contract
- PM realism criteria

#### Outputs
- simulator design
- config schema
- event examples
- state machine
- implementation plan

#### Dependencies
- Phase 0 architecture contract

#### Downstream Consumers
- Pipeline Agent
- Codex simulator prompt
- Benchmark Agent

#### Success Criteria
- generates credible, analytics-friendly, configurable events
- supports required event flow and load tiers

---

### 6.4 Pipeline / Stream Processing Agent
#### Mission
- Define the ingestion backbone that validates, routes, deduplicates, and writes to Redis/ClickHouse.

#### Responsibilities
- Kafka consume flow
- parsing/validation/routing
- dedup design
- late/out-of-order handling
- Redis serving-state update policy
- ClickHouse write policy
- session reconstruction
- processing metrics

#### Non-Goals
- not the simulator
- not final BI/dashboard storytelling
- not warehouse-only schema work beyond sink usage

#### Inputs
- frozen event contract
- simulator contract
- Phase 0 architecture contract

#### Outputs
- processor design
- validation model
- routing model
- sink behavior
- session handling rules

#### Dependencies
- Phase 0 Architecture Agent
- Data Generator Agent

#### Downstream Consumers
- Storage Agent
- Benchmark Agent
- Codex processor prompt

#### Success Criteria
- correct event handling policy is frozen and implementation-ready

---

### 6.5 Storage / Analytics Agent
#### Mission
- Define Redis/ClickHouse storage semantics, table design, facts, aggregates, and analytical support model.

#### Responsibilities
- Redis role and key semantics
- ClickHouse table design
- partitioning/order strategy
- fact_sessions model
- aggregate strategy
- mapping required analytics to tables

#### Non-Goals
- not processor routing logic
- not simulator realism logic
- not dashboard implementation details

#### Inputs
- architecture contract
- processor contract
- case-study analytics requirements

#### Outputs
- store responsibility model
- table schemas/designs
- partitioning strategy
- aggregate philosophy
- analytics support map

#### Dependencies
- Phase 0 Architecture Agent
- Pipeline Agent

#### Downstream Consumers
- Benchmark Agent
- Documentation Agent
- Codex schema/setup prompts

#### Success Criteria
- store roles are clear
- analytics are well supported
- storage model is simple and strong

---

### 6.6 Benchmark / Observability Agent
#### Mission
- Define benchmark methodology, metrics, dashboards, alerts, and reporting format.

#### Responsibilities
- benchmark tiers
- sustained vs burst policy
- metric definitions
- latency semantics
- dashboard plan
- alert concepts
- benchmark result format
- bottleneck interpretation

#### Non-Goals
- not changing architecture
- not creating a huge enterprise monitoring platform
- not implementing all dashboards unless requested

#### Inputs
- architecture contract
- processor contract
- storage contract
- case-study benchmark requirements

#### Outputs
- benchmark methodology
- metrics catalog
- dashboard plan
- alert plan
- final reporting template

#### Dependencies
- Pipeline Agent
- Storage Agent

#### Downstream Consumers
- Documentation Agent
- Codex metrics/benchmark prompts

#### Success Criteria
- benchmarks are measurable, honest, and directly useful in README/report

---

### 6.7 Documentation Agent
#### Mission
- Turn the frozen technical decisions and benchmark results into a polished submission narrative.

#### Responsibilities
- README structure
- architecture report structure
- benchmark/results storytelling
- trade-off explanations
- incomplete-parts framing
- diagram placement and narrative

#### Non-Goals
- not redefining technical architecture
- not creating new implementation scope

#### Inputs
- all frozen contracts
- benchmark results
- implementation outputs

#### Outputs
- README template/content
- architecture report outline/content
- results presentation style
- screenshot/table guidance

#### Dependencies
- all prior major agents

#### Downstream Consumers
- final submission package

#### Success Criteria
- submission reads as coherent, senior, and intentional

---

### 6.8 Codex Implementation Agent
#### Mission
- Convert frozen contracts into implementation-ready code changes without redesigning the system.

#### Responsibilities
- implement against frozen contracts
- keep modules aligned with architecture
- create code in bounded scopes
- avoid speculative redesign
- produce runnable, testable deliverables

#### Non-Goals
- not reopening architecture decisions unless blocked by contradiction
- not adding extra infrastructure for style points

#### Inputs
- all frozen design contracts
- Codex-ready implementation prompts

#### Outputs
- code
- configs
- SQL/DDL
- Docker changes
- tests where requested

#### Dependencies
- all relevant design agents

#### Downstream Consumers
- final repo
- benchmark execution
- documentation

#### Success Criteria
- code matches frozen architecture and is practical to validate locally

## 7. Recommended Execution Order
1. Product Manager Agent
2. Phase 0 Architecture Definition Agent
3. Data Generator Agent
4. Pipeline / Stream Processing Agent
5. Storage / Analytics Agent
6. Benchmark / Observability Agent
7. Codex Implementation Agent
8. Documentation Agent

## 8. Handoff Rules Between Agents
- Each agent must consume frozen decisions, not reinterpret them
- Each agent must summarize what it is assuming
- Each agent must explicitly state what it is freezing for downstream use
- If an agent detects contradiction, it must flag exactly one issue instead of reopening broad design
- Downstream implementation prompts should only reference frozen outputs

## 9. Change-Control Rules
- Core architecture changes require PM approval
- Naming changes require PM approval once Step 3 is frozen
- Event contract changes require PM approval once simulator + processor are aligned
- Storage model changes require PM approval once Step 6 is frozen
- Benchmark definitions change only if implementation reveals hard contradictions

## 10. Definition of Done for Agent Phase
The agent-design phase is complete when:
- architecture is frozen
- simulator contract is frozen
- processor contract is frozen
- storage contract is frozen
- benchmark/observability contract is frozen
- Codex prompts can be generated without major ambiguity