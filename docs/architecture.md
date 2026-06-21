# obs-svc — fleet health and token runway, at a glance

> Architecture for **obs-svc**, the observability layer of
> [blm](https://github.com/janearc/blm). Status: proposed. Date: 2026-06-16.

## What it is

obs-svc answers one question without you having to ask it: **is the fleet okay,
and how much token runway is left?** A small frameless widget floats on your
desktop — think the macOS Activity Monitor CPU window — and a local daemon does
all the thinking behind it. Glance at it; if it's green, get back to work.

It watches two things at once:

- **fleet health** — which services are up, degraded, or down.
- **token runway** — how fast agents are burning tokens, and how long the budget
  lasts at that rate. (Burn rate isn't a deep engineering metric; it's the number
  nontechnical folks find interesting — *how much is this costing right now.*)

## The two pieces

obs-svc is split deliberately, so the part you look at holds no state and can't
quietly lie to you:

- **obs-svc-agg** (Go) — the daemon. It ingests events, runs the health and quota
  state machine, and pushes a finished snapshot to the widget. All the buffering,
  thresholds, and judgment live here.
- **obs-svc-apple** (Rust) — the widget. A thin, frameless macOS client
  (`NSWindowLevelFloating`) that renders the latest snapshot over a gRPC feed
  (default every 2 seconds). It computes nothing.

## How it knows what's true

The daemon doesn't hardcode where the fleet is. On a cold boot it reports itself
**UNHEALTHY** and stays that way until it has discovered the live endpoints — the
same discipline [delightd](https://github.com/janearc/delightd) uses to be the
fleet's source of truth: present nothing until you actually know it.

Health and token events arrive over Kafka. The daemon does not buffer in memory
during an unhealthy boot; it relies on **Kafka replay**, seeking back to its
committed offsets once healthy, and commits offsets only *after* a successful
state transition — so a crash mid-process loses nothing.

### The health state machine

The widget shows one of a few colors, and the daemon moves between them with
hysteresis (it takes a few consecutive healthy heartbeats to return to green, so
the light doesn't flap):

```text
UNSPECIFIED → GREEN    : first healthy heartbeat
GREEN → YELLOW         : a degraded node, or burn rate over threshold_1
YELLOW → RED           : several degraded nodes, or burn rate over threshold_2
ANY → EXHAUSTED        : token budget hits zero (terminal — manual reset)
ANY → UNHEALTHY        : discovery endpoint lost (daemon-internal, not shown)
```

The exact thresholds and timer mechanics live with the implementation, not here.

## Secrets

API keys never sit in plaintext. obs-svc-agg reads them from **Kube Secrets**
under a least-privilege RBAC binding: a dedicated `ServiceAccount`, a `Role`
scoped to `get`/`watch` on exactly the `obs-svc-secrets` object, and a
`RoleBinding` tying them together. Nothing else in the cluster can read them, and
the widget never sees them at all.

## The contracts

obs-svc speaks Protobuf over Kafka. Schema compatibility is locked to
`FULL_TRANSITIVE` in the Confluent Schema Registry, and fields are never removed,
only deprecated — so an old consumer and a new one can always read the same
stream. The keywords **MUST**, **SHALL**, and **SHOULD** carry their
[RFC 2119 / BCP 14](https://www.rfc-editor.org/rfc/rfc2119) meanings.

### observability.v1 — fleet health + token burn

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
  HEALTH_STATE_EXHAUSTED = 4; // terminal
}

message ServiceHealthHeartbeat {
  string service_name = 1;
  HealthState current_state = 2;
  uint32 uptime_seconds = 3;
  uint32 internal_load_metric = 4;
  google.protobuf.Timestamp timestamp = 5;
  string idempotency_key = 6; // for idempotent processing (UUID or service+timestamp)
}

message TokenBurnEvent {
  string agent_id = 1;
  string action_context = 2;
  uint32 tokens_consumed = 3;
  optional uint32 cost_estimated_micro_usd = 4;
  google.protobuf.Timestamp timestamp = 5;
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

### delight.v1 — backup events from delightd

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
