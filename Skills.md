# Skills.md

## 1. Purpose of This File
This file defines the working rules, quality bar, output style, and decision principles that all project agents must follow.

Its purpose is to keep all agents aligned with the same architecture, constraints, and case-study scoring logic.

## 2. Core Working Principle
Optimize for the strongest case-study submission within 5–7 days, not for maximum theoretical completeness.

Agents should prefer:
- strong architecture
- clean schema
- realistic implementation scope
- measurable performance reasoning
- polished documentation value
over unnecessary complexity.

## 3. Frozen Architectural Skills
All agents must respect these frozen skills:

### 3.1 Architecture Respect
- Do not casually reopen the final architecture
- Assume: Simulator → Kafka → Stream Processor → Redis + ClickHouse → Analytics/Reporting

### 3.2 Store Role Discipline
- Redis is only for serving state and dedup state
- ClickHouse is for raw history, audit, session facts, and limited aggregates

### 3.3 Event-Time Discipline
- Use `event_time` as business truth
- Treat ingest/processing times as operational metrics

### 3.4 Simplicity Bias
- Prefer append-oriented designs
- Prefer small numbers of strong tables/metrics/dashboards
- Avoid distributed-systems complexity unless it clearly improves score

## 4. Case-Study Scoring Skills
Every agent should optimize for these evaluation surfaces:

- pipeline design quality
- schema/data model quality
- store selection and justification
- analytical usefulness
- benchmark and scalability reasoning
- code quality readiness
- documentation clarity
- prioritization maturity

Agents must explicitly prefer high-score items over low-ROI extras.

## 5. Prioritization Skills
Agents must classify work into three levels:

### 5.1 Must-Have
Core items that materially affect case-study score.

### 5.2 Should-Have
Differentiators that improve quality but are not foundational.

### 5.3 Bonus-Only
Items to include only if time remains.

Agents must state which category their recommendations belong to.

## 6. Decision-Making Skills
When choosing between alternatives, agents must optimize in this order:

1. strongest score-to-effort ratio
2. strongest clarity for README/report
3. strongest fit with frozen architecture
4. strongest local implementation realism
5. strongest extensibility only if it does not increase risk much

Agents should briefly evaluate alternatives, then choose one clear recommendation.

## 7. Scope-Control Skills
Agents must avoid:
- overengineering
- cloud-only assumptions
- large framework additions with weak ROI
- enterprise-grade features that do not improve scoring enough
- redesigning other agents’ responsibilities

Examples of low-ROI complexity:
- exactly-once claims
- schema registry
- Avro migration
- Airflow orchestration
- Kubernetes deployment
- full anomaly detection productization
- distributed tracing platform
- warehouse-side correction loops

## 8. Output Quality Skills
All agent outputs should be:

- concrete
- opinionated
- implementation-oriented
- readable
- aligned with frozen contracts
- explicit about assumptions
- explicit about trade-offs

Avoid:
- vague “it depends” answers
- giant theoretical essays
- broad tool comparisons without final choice
- ambiguous ownership boundaries

## 9. Prompt-Following Skills
When responding, each agent should:

1. restate its mission briefly
2. respect the frozen contracts
3. produce the requested structure exactly
4. define measurable success criteria
5. call out risks and simplifications
6. end with one clear recommendation

## 10. Engineering-Design Skills
### 10.1 Naming Consistency
- Use frozen topic/table/key names exactly unless PM approves change

### 10.2 Contract Respect
- Build on the canonical event contract
- Do not invent conflicting field semantics

### 10.3 Modularity Bias
- Prefer modular components/services/files over one large script
- But do not decompose into too many services

### 10.4 Auditability
- Preserve explicit invalid-event and late-event audit paths
- Keep raw vs fact vs aggregate boundaries clear

### 10.5 Benchmarkability
- Designs should be measurable
- Metrics and benchmark implications should be considered early

## 11. Analytics Skills
Agents must design with the required analytical outputs in mind:

- hourly energy delivered
- station uptime/downtime
- charging duration by vehicle brand
- revenue analysis
- geographic fault distribution

Optional analytics must never weaken support for these five core outputs.

## 12. Observability Skills
When metrics or monitoring are involved, agents must prefer:

- throughput
- lag
- latency percentiles
- error class counts
- data quality counters
- sink health
- bottleneck diagnosis signals

Avoid vanity-heavy instrumentation.

## 13. Documentation Skills
Even when documentation is deferred, agents must produce outputs that are:

- easy to copy into README/report
- easy to explain to evaluators
- rich in rationale
- strong in trade-off language
- explicit about non-goals

## 14. Codex-Handoff Skills
When preparing work for Codex:

- freeze assumptions first
- separate design from implementation clearly
- give bounded coding scopes
- avoid asking Codex to redesign architecture
- specify file/module boundaries when possible
- specify acceptance criteria clearly

## 15. Conflict-Handling Skills
If an agent sees a contradiction:
- identify the contradiction precisely
- propose the smallest fix
- do not reopen unrelated decisions
- escalate only the blocked decision to PM

## 16. Success-State Skills
Each agent must define success in measurable terms:
- requirement coverage
- correctness
- usefulness to downstream agents
- realistic implementation fit
- contribution to final case-study score

## 17. Final-Answer Style Skills
Every agent answer should:
- be concrete and direct
- prefer bullets/sections over long loose prose when needed
- close with a final recommendation
- avoid filler
- avoid speculative expansion beyond scope

## 18. Definition of Good Output
A good agent output:
- reduces ambiguity
- reduces rework
- improves Codex implementation quality
- improves final README/report quality
- makes the submission feel senior and deliberate