# Benchmark Results and Performance Narrative

## Benchmark Objective
This document reports benchmark outcomes for the ChargeSquare EV pipeline using only available evidence from:
- repository benchmark profiles and runner logic
- Grafana dashboard query definitions
- provided dashboard/Docker/ClickHouse screenshots
- implementation chronology provided by the author

The goal is to show honest local-machine performance and engineering decisions, not to over-claim cloud-scale throughput.

## Test Environment
- Hardware: Apple Silicon M1 MacBook, 8 CPU cores, 16 GB RAM (author-provided).
- Runtime: Docker Compose local stack.
- Core services: Kafka (single broker), Redis, ClickHouse, Python simulator, Python processor, Prometheus, Grafana.
- Scrape cadence: Prometheus `scrape_interval: 5s`.
- Kafka topics: `cs.ev.events.raw`, `cs.ev.events.dlq`, `cs.ev.events.late`.

## Benchmark Tiers and Methodology
Executed benchmark tiers in this report:

| Tier | Target EPS | Test Type | Configured Duration | Intent |
|---|---:|---|---:|---|
| 1k | 1,000 | sustained | 300s | baseline credibility tier |
| 10k | 10,000 | sustained | 300s | primary local sustained target |

Notes:
- 50k and 100k profiles exist in the repository configuration, but they are intentionally out of scope for this report.

Benchmark runner outcome rubric (from `src/benchmarks/run.py`):
- `pass`: achieved EPS >= 90% of target
- `partial`: achieved EPS >= 70% of target
- `fail`: achieved EPS < 70% of target

Execution approach used in this narrative:
1. Run tier profile with configured simulator/processor settings.
2. Observe pipeline dashboard (`chargesquare-overview`) and sink dashboard (`chargesquare-clickhouse-prometheus`).
3. Interpret generated EPS vs accepted EPS, lag growth, and latency behavior.
4. Cross-check infrastructure pressure from Docker CPU/memory snapshots.
5. Cross-check analytical responsiveness with ClickHouse query evidence.

## Metrics Collected
Primary metrics and how they are defined in this repo:

| Metric | Definition |
|---|---|
| generated EPS | `sum(rate(events_generated_total[1m]))` |
| accepted EPS | `sum(rate(events_accepted_total[1m]))` |
| consumer lag | `max(kafka_consumer_lag)` |
| ingest lag p95 | `max(ingest_lag_ms_p95)` |
| e2e latency p95/p99 | `max(end_to_end_latency_ms_p95)`, `max(end_to_end_latency_ms_p99)` |
| quality rates | `rate(dead_letter_routed_total[5m])`, `rate(too_late_rejected_total[5m])`, `rate(duplicates_detected_total[5m])` |
| Redis sink latency metric available | `max(redis_write_latency_ms_p95)` |
| ClickHouse insert latency metric available | `max(clickhouse_insert_latency_ms_p95)` |
| ClickHouse query pressure | `rate(ClickHouseProfileEvents_Query[1m])`, `SelectQuery`, `InsertQuery` |
| ClickHouse memory/IO | `ClickHouseMetrics_MemoryTracking`, `ClickHouseAsyncMetrics_MemoryResident`, OS read/write byte rates |

## Results Summary
The table below uses exact values only where they are directly visible. Chart-derived values are marked as approximate (`~`). Missing persisted run artifacts are marked `N/A`.

| target EPS | achieved EPS | test type | duration | ingest lag p95/p99 | end-to-end latency p95/p99 | Redis latency | ClickHouse insert/query latency | consumer lag | bottleneck | outcome note |
|---:|---:|---|---|---|---|---|---|---|---|---|
| 1,000 | >=1,000 (author-confirmed) | sustained | 300s configured | N/A | N/A | N/A | N/A | limited (author-confirmed) | none material | Baseline tier reached; details intentionally not expanded. |
| 10,000 | ~1,600 | sustained | 300s configured; panel window ~12:46-12:53 | ~0 -> ~430k+ ms | ~0 -> ~440k+ ms | N/A | N/A | up to ~6.1M | processor hot path saturation | Early 10k attempt failed; accepted EPS far below generated EPS (~13k-15k). |
| 10,000 | ~5,400 | sustained | 300s configured | N/A | N/A | N/A | N/A | rising (exact run artifact unavailable) | CPU-heavy per-event processing | After hot-path refactor, single processor improved from ~1.6k to ~5.4k. |
| 10,000 | ~11k-12k | sustained | final sustained run: 25 minutes; panel windows include ~19:03-19:15 and ~21:24-21:46 | ~2k-6k ms | p95 ~4k-8k ms, p99 ~5k-9k ms | N/A | Query evidence: 42.27M-row group-by in 0.162s | spikes mostly in low-thousands to ~11k | mixed (throughput + sink pressure) | Multi-processor pipeline reached and exceeded 10k target on accepted EPS in final 25-minute validation. |
| 10,000 | ~11k-12k with instability periods | sustained | 300s configured; panel window ~15:49-15:53 | up to ~70k ms | up to ~80k ms | N/A | N/A | up to ~500k | simulator control instability + backlog growth | Multi-simulator scaling increased generation but produced unstable lag behavior before control rework. |

### Exact Snapshot Counters (Screenshot-Visible)
These are included as exact observed counters without over-interpreting them as full-run outcomes.

| Snapshot | Exact visible counters |
|---|---|
| Pipeline stats snapshot A | Generated Total: 7,386,261; Accepted Total: 628,387; DLQ Total: 27; Too-Late Rejected Total: 1,819 |
| Pipeline stats snapshot B | Generated Total: 14,838,320; Accepted Total: 14,746,427; DLQ Total: 5,331; Too-Late Rejected Total: 44,331 |
| Multi-instance accepted totals | processor: 1,756,321; processor-b: 3,677,218; processor-c: 3,608,235; processor-d: 1,755,068 (sum: 10,796,842) |
| Multi-instance quality totals | DLQ sum: 126; Too-late sum: 32,291 |
| ClickHouse query snapshot | `SELECT event_type, count(*) FROM raw_events GROUP BY event_type` -> elapsed 0.162 sec, read 42.27M rows |

## 10k Journey: Bottlenecks and Improvements
### Phase 1: Initial 10k attempt (~1.6k accepted EPS)
Observed behavior:
- Generated throughput was much higher than accepted throughput.
- Consumer lag climbed rapidly (multi-million backlog).
- Ingest and end-to-end latencies exploded.

Interpretation:
- Kafka transport was healthy enough to accept load, but processor hot-path cost dominated throughput.

### Phase 2: Hot-path optimization wave (~1.6k -> ~5.4k)
Implemented changes (author-provided):
- `src/processor/state/session_state.py`
  - Added `_station_sessions: dict[str, set[str]]` secondary index.
  - `active_sessions_for_station` moved from full scan to `len(set)` O(1).
  - Index maintenance added to start/close/expire paths.
- `src/processor/dedup.py`
  - Added batch dedup API (`check_and_mark_batch`).
  - Redis dedup uses pipelined `SET NX` batch round-trip.
  - In-memory dedup uses single lock per batch.
- `src/processor/sinks/redis_sink.py`
  - Heartbeat thinning: HEARTBEAT updates return after station key update (skip connector/session writes).
  - Batch Redis writes via `redis.pipeline(transaction=False)` (eval + expire phases).
- `src/processor/main.py`
  - Multi-pass batch flow:
    - parse/schema for full batch
    - batch dedup for all event IDs
    - semantic/lateness/session-state pass
    - batch Redis writes for accepted events
  - `force=True -> force=False` to avoid over-forced ClickHouse flushes.

Result:
- Single-processor accepted EPS improved to ~5.4k.

### Phase 3: Horizontal processor scaling (~12k accepted EPS)
Observed behavior:
- Multi-processor topology increased accepted EPS into ~11k-12k range.
- Dashboard periods show generated and accepted curves close to each other in this range.

Interpretation:
- Processor parallelism closed most of the gap between generated and accepted rates for 10k target workloads.

### Phase 4: Multi-simulator pressure and lag instability
Observed behavior:
- Single simulator was generation-limited (~15k ceiling), so multi-simulator was introduced.
- Docker snapshots show severe CPU pressure during this period (example: ~846.74% / 800%).
- Consumer lag became unstable in some runs; one window rose to ~500k, and earlier failed windows reached multi-million lag.

Interpretation:
- Simulator-side rate control and downstream acceptance drifted out of balance under high local CPU contention.

### Phase 5: Simulator control rework and stabilized real-time operation
Observed behavior:
- Final runs show sustained ~12k accepted EPS with bounded lag compared to unstable periods.
- Final validation window was run for 25 minutes at 10k-tier target conditions.
- Latency stayed in second-level range instead of runaway backlog behavior in the failed runs.

Interpretation:
- With improved simulator control and existing processor optimizations, the local system reached a stable real-time envelope around ~12k accepted EPS.

## Redis and ClickHouse Performance Observations
### Redis
- Redis is effective as serving-state storage but can dominate hot-path cost under heartbeat-heavy traffic if each event writes too many keys.
- Heartbeat thinning and batched pipelines were high-impact optimizations because `METER_UPDATE`/`HEARTBEAT` dominate event volume.

### ClickHouse
Observed from dashboard and query evidence:
- ClickHouse query rate generally around ~6-8 queries/sec in the shown window.
- Memory remained around ~1.3-1.45 GB in the shown window.
- Write IO showed sustained throughput with spikes (roughly up to ~25 MB/s in screenshot windows).
- Manual query evidence: `SELECT event_type, count(*) FROM raw_events GROUP BY event_type` scanned 42.27M rows in 0.162s.

Interpretation:
- ClickHouse handled analytical reads efficiently on loaded local data.
- Insert and flush policy still matters for ingestion stability, but no evidence here suggests ClickHouse as the first hard ceiling during the final stable 10k+ runs.

## Throughput and Latency Interpretation by Tier
- 1k tier:
  - Achieved baseline and considered operationally straightforward on local hardware.
- 10k tier:
  - Initially failed due to processor hot path.
  - Reached with optimization + parallel processors.
  - Final local sustained posture: ~12k accepted EPS with limited lag under tuned simulator control, validated over 25 minutes.

## Architecture-Level Interpretation
- Kafka transport choice:
  - Correct for decoupling producer and consumer speeds; lag metric gave immediate visibility into imbalance.
- Redis serving-state strategy:
  - Correct for low-latency serving state; requires aggressive write-thinning/batching to avoid becoming hot-path tax.
- ClickHouse batching strategy:
  - Better when processor avoids over-forced flushes and allows batch size/timer controls to work.
- Processor hot-path design:
  - Main throughput determinant in Python-based local pipeline; algorithmic complexity and batch boundaries are decisive.
- Scaling path:
  - Current pragmatic path: parallel Python processors/simulators with careful control loops.
  - Future high-scale path: distributed stream frameworks (for example Spark/Flink class) when objective shifts beyond local 10k-tier objectives.

## Limitations and Honesty Notes
- No committed `benchmark_results/` run artifacts were found in repository state at authoring time.
- Several values are inferred from chart visuals and intentionally marked as approximate.
- Redis p95 write and ClickHouse insert p95 numeric panel values are defined in dashboards but not numerically readable in the provided screenshots.
- Exact calendar dates for each screenshot sequence were not available in repository artifacts; this report uses phase-based ordering.

## Next Tuning Steps
1. Persist benchmark outputs (`result.json`, `result.csv`, `summary.md`) for every tier run and commit them with hardware metadata.
2. Add explicit run IDs/annotations in Grafana to tie each screenshot to config hash and code version.
3. Tighten simulator EPS controller further for multi-simulator runs to reduce overshoot and lag oscillation.
4. Profile Redis sink and semantic stage CPU under final 12k workload to identify next optimization increment.
5. Keep replayable evidence packaging (screenshots + exported metrics + config hash) for each future tuning milestone.

## Image Mapping Guide (Google Docs Placement)
Use the mapping below to place your screenshots in the right sections with evaluator-friendly captions.

| Image to use | Place in section | Suggested caption |
|---|---|---|
| Throughput/Lag/Latency panel where accepted EPS is ~1.6k and lag climbs to ~6.1M (window around ~12:46-12:53) | `## 10k Journey: Bottlenecks and Improvements` -> `### Phase 1` | Initial 10k attempt: generator outpaced processor, causing runaway lag and latency growth. |
| Throughput panel where accepted EPS improves to ~5.4k after code changes | `## 10k Journey: Bottlenecks and Improvements` -> `### Phase 2` | Hot-path optimization wave raised single-processor throughput from ~1.6k to ~5.4k accepted EPS. |
| Throughput/Lag/Latency panel with accepted EPS around ~11k-12k and bounded lag (windows around ~19:03-19:15 or ~21:24-21:46) | `## 10k Journey: Bottlenecks and Improvements` -> `### Phase 3` and `### Phase 5` | Multi-processor + simulator-control rework enabled sustained ~12k accepted EPS in stable operation. |
| Throughput/Lag/Latency panel with instability (window around ~15:49-15:53), lag rising toward ~500k, p95/p99 increasing sharply | `## 10k Journey: Bottlenecks and Improvements` -> `### Phase 4` | Multi-simulator phase exposed EPS-control instability and backlog amplification. |
| Docker screenshot showing very high total CPU (e.g., ~846.74% / 800%) | `### Phase 4` and `## Test Environment` | Local CPU saturation under multi-simulator pressure constrained controllability. |
| Docker screenshot showing processor replicas (`cs-processor`, `cs-processor-b/c/d`) | `### Phase 3` | Horizontal processor scaling was the key step to exceed 10k accepted EPS. |
| Pipeline stat-card snapshot with `Generated Total 14,838,320` and `Accepted Total 14,746,427` | `### Exact Snapshot Counters (Screenshot-Visible)` | High acceptance ratio in stabilized run. |
| Multi-instance stat-card snapshot with per-processor accepted totals summing to `10,796,842` | `### Exact Snapshot Counters (Screenshot-Visible)` and `### Phase 3` | Accepted throughput distribution across processor replicas. |
| ClickHouse dashboard panel (query rates, memory, IO) | `## Redis and ClickHouse Performance Observations` -> `### ClickHouse` | ClickHouse sustained analytical and ingest activity without becoming first hard ceiling. |
| ClickHouse SQL UI screenshot showing `read 42.27 million rows` in `0.162 sec` | `### Exact Snapshot Counters (Screenshot-Visible)` and `### ClickHouse` | Point-in-time analytical query evidence on loaded raw_events data. |

## Evidence Used
- Benchmark and tier configs: `config/benchmarks/*.yaml`, loadtest simulator/processor configs.
- Benchmark runner logic and rubric: `src/benchmarks/run.py`.
- Dashboard metric definitions: `dashboards/grafana/dashboards/chargesquare-overview.json`, `dashboards/grafana/dashboards/chargesquare-clickhouse-prometheus.json`.
- Prometheus scrape topology: `config/prometheus/prometheus.yml`.
- Author-provided screenshots: pipeline trends, stat totals, Docker CPU/memory snapshots, ClickHouse query screenshot.
- Author-provided optimization chronology and code-level changes.

## Missing Evidence
- No committed `benchmark_results/runs/*/result.json` artifacts for direct numeric extraction.
- No timestamped run log bundle mapping each screenshot to exact profile invocation.
- No full exported Prometheus time series snapshots for post-hoc exact percentile extraction.
