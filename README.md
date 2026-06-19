# obs-svc

Local fleet & quota observability daemon. See `observability_architecture_v1.md` (design of record) and `docs/design.md`.

- **obs-svc-agg** (Go): consumes Protobuf-over-Kafka fleet events, aggregates state, serves snapshots.
- **obs-svc-apple** (Rust): frameless floating macOS widget; polls `obs-svc-agg`'s `GET /state` at ~2s and renders fleet health + token runway. See `obs-svc-apple/README.md`.

---

## author

max toegang <max.toegang@ftml.net>
🤖 claude · claude-opus-4-8
🤖 bespoke locally trained models
