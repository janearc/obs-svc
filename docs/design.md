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
                                   tiny-monitor (Rust widget, separate repo)
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

## 4. Health

Two distinct mechanisms; do not conflate them.

### 4.1 Daemon readiness (process liveness)

The daemon boots **UNHEALTHY** and refuses to claim readiness until it has
connected to its inputs.

```
(cold boot) UNHEALTHY ──consumer connects to Kafka──▶ HEALTHY
```

`/health` returns `503` while UNHEALTHY and `200` once HEALTHY. The consumer
reconnects with exponential backoff (1s → ×2 → 30s cap); a dropped connection
returns the daemon to retrying. Replacing this "connected to Kafka" signal with
the architecture's Traefik-poll readiness gate is a documented follow-up (§6).

### 4.2 Per-service fleet health state machine (`pkg/health`)

Each fleet service self-reports a `HealthState` on every
`observability.v1.ServiceHealthHeartbeat`. obs-svc-agg does **not** trust that
raw value directly: it folds it through an explicit per-service state machine
that debounces flapping. One `health.Machine` exists per service, owned by the
aggregator behind its lock.

States mirror the contract enum:

```
UNSPECIFIED ──▶ GREEN ──▶ YELLOW ──▶ RED        (degradation, immediate)
   ▲             ▲           │          │
   └──── hysteresis ─────────┘          │        (N consecutive greens to recover)
              (snap back to GREEN)      │
ANY ─────────────────────────────────▶ EXHAUSTED (terminal)
```

The transition rules, in evaluation order (one heartbeat = one `Observe` call):

1. **EXHAUSTED is terminal.** Once reached, every later heartbeat is a no-op.
   A fully-consumed quota does not recover on its own.
2. **Reported EXHAUSTED snaps from any state.** Quota is gone; nothing to
   debounce.
3. **Reported UNSPECIFIED is missing data.** It holds the current state and
   *breaks* any in-progress recovery streak, so a gap in healthy reports cannot
   silently count toward recovery.
4. **GREEN from clean state** (already GREEN, or cold UNSPECIFIED) snaps to
   GREEN — there is nothing to debounce on the way up from clean.
5. **GREEN from a degraded state** (YELLOW/RED) increments a green streak;
   recovery to GREEN happens only once the streak reaches the threshold
   (`DefaultGreenStreak` = 3). The streak resets on recovery, on degradation,
   and on an UNSPECIFIED gap.
6. **YELLOW or RED degrades immediately** and resets the green streak — bad news
   is believed at once.

The asymmetry is deliberate: degradation is responsive (a single bad heartbeat
moves the state), recovery is conservative (a run of consecutive healthy
heartbeats), so a service oscillating around a threshold does not flap the fleet
view. The threshold is configurable via `agg.NewWithHysteresis`.

The aggregator stores the **debounced** state per service plus the raw
`last_reported` value (for observability) and a heartbeat count. The fleet
rollup (`/state.fleet`, `obs_svc_fleet_*`) is worst-wins over the debounced
states: any EXHAUSTED drives overall to EXHAUSTED (terminal outranks RED), else
the max of GREEN/YELLOW/RED, else UNSPECIFIED when no service has reported.

`QuotaMetrics`/`TokenBurnEvent` → token runway (the EXHAUSTED-on-zero-runway
driver) remains a follow-up (§6); EXHAUSTED is wired here only via a service's
self-reported state.

## 5. Aggregation

`pkg/agg` keeps per-project `BackupStat` (total / successes / failures / last
outcome / last size / last duration / last seen), per-service `ServiceHealth`
(debounced state / last reported / uptime / load / heartbeat count / last seen),
and a `FleetHealth` rollup, plus a total event count. `Snapshot()` returns a
deep copy of every map so the HTTP layer never races the writer. The widget
cadence (a 2s `time.Ticker` batching snapshots to the gRPC feed) is specified by
the architecture and will sit on top of `Snapshot()`.

The consumer subscribes to multiple topics and dispatches each record by topic
to the right message type (`delight.events` → `delight.v1.BackupEvent`,
`observability.events` → `observability.v1.ServiceHealthHeartbeat`); a record
from an unmodeled topic is skipped rather than mis-parsed.

## 6. Known gaps / follow-ups

- `QuotaMetrics`/`TokenBurnEvent` → token runway (the EXHAUSTED-on-zero-runway
  driver). `observability.v1.ServiceHealthHeartbeat` ingestion and the full
  HealthState machine with hysteresis are **implemented** (§4.2).
- The 2s gRPC snapshot feed and `tiny-monitor` (Rust widget, in its own repo:
  https://github.com/janearc/tiny-monitor).
- Traefik-based discovery as the true post-meteor readiness gate (no hardcoded
  endpoints); today brokers/topic come from env with dev-fleet defaults.
- Kube Secrets + RBAC for API keys (token-cost data) per the security posture.
- Durable dead-letter for undecodable records.
- Schema-Registry-driven type resolution to consume multiple record types per
  topic via RecordNameStrategy.
