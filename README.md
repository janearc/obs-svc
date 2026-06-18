# obs-svc

Local fleet & quota observability daemon. See `observability_architecture_v1.md` (design of record) and `docs/design.md`.

- **obs-svc-agg** (Go): consumes Protobuf-over-Kafka fleet events, aggregates state, serves snapshots.
- **obs-svc-apple** (Rust): frameless floating macOS widget (not yet built).
