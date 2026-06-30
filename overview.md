# obs-svc — overview

obs-svc is the fleet's observability daemon: it turns the Protobuf-over-Kafka
events the fleet emits into a single, glanceable picture of fleet health and LLM
token runway.

Two components (architecture of record: `docs/architecture.md`):

- **`obs-svc-agg`** (Go) — the brain. Consumes fleet events from Kafka, holds
  *all* aggregated state, runs the health state machine, and serves snapshots.
  This is what's built today.
- **`tiny-monitor`** (Rust) — a thin, stateless floating macOS widget that
  renders a 2-second snapshot feed from the aggregator. Lives in its own repo
  ([janearc/tiny-monitor](https://github.com/janearc/tiny-monitor)).

## What works today

`obs-svc-agg` consumes `delight.events` and folds delightd's live
`delight.v1.BackupEvent`s into per-project rolling state. It exposes:

- `GET /health` — `200` once connected to Kafka, `503` during the cold-boot
  UNHEALTHY window.
- `GET /metrics` — Prometheus exposition (healthy flag, event/backup counters).
- `GET /state` — the aggregated JSON snapshot.

## Why it's small

Everything upstream already exists: the contracts (`observability.v1`,
`delight.v1`) live in kafka-svc, Schema Registry + the flat `dev-fleet` network
are up, and delightd already produces real events with the fleet's Confluent-SR
protobuf framing. obs-svc-agg is "just" the consumer + aggregation + the health
state machine.

See `docs/design.md` for the state machine, offset/replay semantics, and the
gaps tracked toward the full architecture.
