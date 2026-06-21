# Architectural Record: Local Fleet & Quota Observability Daemon

> **Status:** Proposed  
> **Date:** 2026-06-16  

## 1. Context and Problem Statement

The fleet operates behind Traefik and communicates via Protobuf over Kafka. However, **no services are currently emitting to Kafka** (with the possible exception of `paling`). Transitioning to this event-driven observability model requires an enormous, cross-fleet diff. We will have to instrument `traefik`, `fleet-svc`, `delightd`, `comfy`, `transparent`, `la-domestique`, `paling`, `antigravity` (agy/gemini), and `odysseus` to natively consume and emit Kafka payloads. 

Simultaneously, LLM agent operations introduce a critical constraint: token quota and burn rate. Token burn rate serves both as a hard cost constraint and a fundamental metric of developer activity.

We require a small, unobtrusive, floating observability widget (analogous to the macOS Activity Monitor's floating CPU window) that unifies fleet health and token runway. 

## 2. Architectural Invariants

1. **State Segregation**: The UI must be completely stateless. All metric aggregation, buffering, and threshold calculations are handled by a local background daemon.
2. **Language Constraints & Security**: The core stack is strictly **Golang and Rust**. Any inclusion of JavaScript, Python, or unverified libraries (especially JSON parsers vulnerable to agent-injection) are severe tier-0 security risks.
3. **Idempotent Telemetry & Backoff**: If the daemon reconnects, state transitions must be strictly idempotent. Network retries must utilize standard exponential backoff.
4. **Post-Meteor Discovery**: Hardcoding endpoints/ports is forbidden. The daemon must present itself as strictly **UNHEALTHY** upon a cold boot until it successfully polls Traefik and registers.
5. **Coverage Enforcement**: A hard floor of **87%** is strictly enforced for the new `obs-svc` repository. For existing fleet services modified during the cross-fleet diff, the strict rule is **non-regression**: subagents must not decrease coverage below the pre-diff baseline (tracked in handoff state).
6. **Schema Governance**: Confluent Schema Registry compatibility must be locked to `FULL_TRANSITIVE` to prevent forward/backward breaks across all historical versions. Fields are **never removed, only deprecated** (retained for a minimum of 3 release cycles). The `Taskfile.yml` must enforce this via `buf lint` and `buf breaking` pre-commit gates.

## 3. System Design

The architecture is self-contained within `~/work/obs-svc`. The build system will utilize **Task** (`Taskfile.yml`), enforcing `-dev` and `-prod` deployment groups. 

> **Documentation Mandate:** The exhaustive mechanics of the internal state machines will *not* be derived from this conversation log. They must be codified in explicit detail within `obs-svc/docs/design.md`. `obs-svc`, `fleet-svc`, and `delightd` must all contain an `overview.md`.

### 3.1. The Aggregator Daemon (`obs-svc-agg` in Go)
A local Go service responsible for data ingestion and state machine execution.

*   **Cold Boot Buffering**: The daemon will *not* buffer in memory during an `UNHEALTHY` boot state. It will rely strictly on **Kafka replay**. When the daemon becomes healthy, it will seek back to committed offsets. Offsets are strictly committed *after* a successful state transition to prevent data loss mid-crash.
*   **Widget Cadence**: `TokenBurnEvent` ingestion operates continuously. However, pushing updates to the UI at this rate wastes CPU and causes UI flicker. `obs-svc-agg` will utilize an internal `time.Ticker` (configurable, default 2 seconds) to accumulate events and push batched snapshots to the widget.
*   **State Machine Mechanics**: Transitions must be codified in `design.md` utilizing strict hysteresis to prevent flapping (e.g., requiring 3 consecutive healthy heartbeats to snap back to GREEN). Minimum transition logic:
    ```text
    UNSPECIFIED → GREEN   : first healthy heartbeat received
    GREEN → YELLOW        : >0 degraded nodes OR burn_rate exceeds threshold_1
    YELLOW → RED          : >N degraded nodes OR burn_rate exceeds threshold_2
    ANY → EXHAUSTED       : absolute_quota_remaining_cents == 0
    EXHAUSTED → *         : not permitted (terminal, requires manual reset)
    ANY → UNHEALTHY       : Traefik endpoint lost (daemon-internal, not broadcast)
    ```

### 3.2. Secrets Backend (Kube Secrets & RBAC)
Given the tier-0 security posture, API keys cannot reside in plaintext. Because the fleet already runs on Kubernetes, we will natively utilize **Kube Secrets**. To maintain strict least-privilege isolation, we will implement Kubernetes Role-Based Access Control (RBAC):
1. Create a dedicated `ServiceAccount` for `obs-svc-agg`.
2. Provision a `Role` with `get` and `watch` verbs restricted exclusively to the specific `obs-svc-secrets` object.
3. Bind the `Role` to the `ServiceAccount` via a `RoleBinding`.

### 3.3. The Presentation Layer (`obs-svc-apple` in Rust)
A thin, dumb client written in Rust. It lives within `obs-svc` but acts strictly as `obs-svc-apple`, consuming the 2-second snapshot gRPC feed from `obs-svc-agg`. It runs as a frameless macOS application with `NSWindowLevelFloating`.

## 4. Protobuf Schemas

### 4.1. Global Fleet Observability (`observability.v1`)

```protobuf
syntax = "proto3";

package observability.v1;

import "google/protobuf/timestamp.proto";

option go_package = "github.com/janearc/obs-svc/api/v1;observabilityv1";

enum HealthState {
  HEALTH_STATE_UNSPECIFIED = 0;
  HEALTH_STATE_GREEN = 1;
  HEALTH_STATE_YELLOW = 2;
  HEALTH_STATE_RED = 3;
  HEALTH_STATE_EXHAUSTED = 4; // 💀 Terminal state
}

message ServiceHealthHeartbeat {
  string service_name = 1;
  HealthState current_state = 2;
  uint32 uptime_seconds = 3;
  uint32 internal_load_metric = 4; 
  google.protobuf.Timestamp timestamp = 5;
  
  // Unique identifier for idempotent processing (UUID or service+timestamp)
  string idempotency_key = 6;
}

message TokenBurnEvent {
  string agent_id = 1;
  string action_context = 2;
  uint32 tokens_consumed = 3;
  optional uint32 cost_estimated_micro_usd = 4; 
  google.protobuf.Timestamp timestamp = 5;
  
  // Unique identifier for idempotent processing
  string idempotency_key = 6;
}

message WidgetStatePayload {
  google.protobuf.Timestamp calculated_at = 1;
  FleetMetrics fleet = 2;
  QuotaMetrics quota = 3;
}

message FleetMetrics {
  HealthState overall_health = 1;
  uint32 active_nodes = 2;
  uint32 degraded_nodes = 3;
  string active_discovery_endpoint = 4; 
}

message QuotaMetrics {
  HealthState runway_state = 1;
  uint32 runway_minutes_remaining = 2;
  uint32 burn_rate_tokens_per_minute = 3;
  uint32 absolute_quota_remaining_cents = 4; 
}
```

### 4.2. Delight Daemon (`delight.v1`)
```protobuf
syntax = "proto3";

package delight.v1;

import "google/protobuf/timestamp.proto";

option go_package = "github.com/janearc/delightd/api/v1;delightv1";

message BackupEvent {
  string project_name = 1;
  bool success = 2;
  uint64 bytes_before = 3;
  uint64 bytes_after = 4;
  uint32 duration_milliseconds = 5;
  google.protobuf.Timestamp timestamp = 6;
}

message ServiceBackupStatus {
  string service_name = 1;
  bool is_known_to_daemon = 2;
  bool is_actively_backing_up = 3;
  bool has_bash_fragment = 4;
}
```

## 5. Concrete Execution Plan

**Core Directive: Handoff State Management**
We explicitly **expect agent/model failure and resume due to laptop environment/status volatility**. The primary agent and *all* subagents must maintain persistent `handoff_state.md` files.

### The Sequence

0.  **`paling` Pre-Flight Audit**:
    -   Dispatch an agent to audit the `paling` repository's existing Kafka emission logic. Generate a report. If it conforms to the new idempotent `observability.v1` schema, omit it from the cross-fleet diff.
1.  **`delightd` API & Wrappers**:
    -   Update `delightd` to respond to service introspection queries.
    -   Write explicit unit tests proving we can query `delightd` state logic.
2.  **`delightd` Telemetry Proof-of-Work (Dry Run)**:
    -   Instrument `delightd` to emit the standardized telemetry. Run in `--dry-run`.
    -   Consume and verify the Kafka logs to prove correctness.
3.  **`delightd` Production Cutover**:
    -   Update launch configuration to perform genuine backups.
4.  **Cross-Fleet Diff Preparation (Dependency Map)**:
    -   Produce a dependency map identifying shared modules across the remaining 8 services to prevent subagent merge conflicts.
    -   Record baseline test coverage for every service in the root `handoff_state.md`.
5.  **The Cross-Fleet Diff (Subagent Orchestration)**:
    -   **Wave 1**: Dispatch subagents to instrument services with *no* shared dependencies.
    -   **Wave 2**: Dispatch subagents sequentially to instrument services touching shared code.
    -   Subagents will optimize hot paths and strictly enforce non-regression coverage rules.
6.  **Bootstrap `obs-svc`**:
    -   Write `overview.md` and `design.md` (detailing timeouts, hysteresis, etc.).
    -   Implement `obs-svc-agg` utilizing Kafka replay and `time.Ticker` decoupling.
    -   Implement `obs-svc-apple` (Rust).
    -   Validate the 87% coverage floor pipeline.
</content>
</invoke>
