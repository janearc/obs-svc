# obs-svc-agg — design

Authoritative architecture: `../../observability_architecture_v1.md`. This
document records the *mechanics* that doc mandates be written down explicitly.

## 1. Components and data flow

```
delightd ──delight.v1.BackupEvent──┐
paling   ──observability.v1.*──────┤
…fleet   ──observability.v1.*──────┴─▶ Kafka (delight.events, …)
                                          │ franz-go consumer (Confluent SR framing)
                                          ▼
                                   obs-svc-agg (this service)
                                     · health state machine
                                     · per-project / fleet rollups
                                     · HTTP: /health /metrics /state
                                     · (future) 2s gRPC snapshot feed
                                          │
                                          ▼
                                   obs-svc-apple (Rust widget, not built)
```

## 2. Wire format (the read side)

Producers frame events in the Confluent Schema Registry protobuf format:
`[magic 0x00][schema id, 4B big-endian][message-index][payload]`. The
message-index for a file's first message is the single byte `0x00`. `pkg/sr`
decodes this (the general `[count][index…]` form too) and hands back the raw
protobuf. We own this decode — there is no Confluent deserializer — so it is
unit-tested against producer-shaped frames.

obs-svc-agg does not need to call Schema Registry to deserialize: the topic
determines the message type (`delight.events` → `delight.v1.BackupEvent`). A
registry-driven type lookup is a future generalization (see §6).

## 3. Offset & replay semantics

- Auto-commit is **disabled**; offsets are committed only **after** a fetch is
  folded into state (`pkg/consumer`). A crash mid-batch replays those records
  rather than losing them — the architecture's data-loss-avoidance rule.
- Cold boot relies on **Kafka replay**, not in-memory buffering: the daemon
  joins the consumer group and reads from its committed offsets.
- A record that cannot be decoded/parsed is logged and skipped (and committed
  past) so a poison pill does not stall the partition. Durable dead-lettering is
  a follow-up (§6).

## 4. Health state machine

The daemon boots **UNHEALTHY** and refuses to claim readiness until it has
connected to its inputs.

```
(cold boot) UNHEALTHY ──consumer connects to Kafka──▶ HEALTHY
```

`/health` returns `503` while UNHEALTHY and `200` once HEALTHY. The consumer
reconnects with exponential backoff (1s → ×2 → 30s cap); a dropped connection
returns the daemon to retrying.

**Planned (per architecture record, not yet implemented):** the richer fleet
HealthState machine (`UNSPECIFIED → GREEN → YELLOW → RED`, `ANY → EXHAUSTED`
terminal) driven by `observability.v1.ServiceHealthHeartbeat` + `QuotaMetrics`,
with hysteresis (e.g. 3 consecutive green heartbeats to snap back to GREEN) to
prevent flapping, and the Traefik-poll readiness gate that replaces the simple
"connected to Kafka" signal. Tracked in §6.

## 5. Aggregation

`pkg/agg` keeps per-project `BackupStat` (total / successes / failures / last
outcome / last size / last duration / last seen) plus a total event count.
`Snapshot()` returns a deep copy so the HTTP layer never races the writer. The
widget cadence (a 2s `time.Ticker` batching snapshots to the gRPC feed) is
specified by the architecture and will sit on top of `Snapshot()`.

## 6. Known gaps / follow-ups

- `observability.v1.ServiceHealthHeartbeat` ingestion + the full HealthState
  machine with hysteresis, and `QuotaMetrics`/`TokenBurnEvent` → token runway.
- The 2s gRPC snapshot feed and `obs-svc-apple` (Rust widget).
- Traefik-based discovery as the true post-meteor readiness gate (no hardcoded
  endpoints); today brokers/topic come from env with dev-fleet defaults.
- Kube Secrets + RBAC for API keys (token-cost data) per the security posture.
- Durable dead-letter for undecodable records.
- Schema-Registry-driven type resolution to consume multiple record types per
  topic via RecordNameStrategy.
