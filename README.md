# obs-svc

Local fleet & quota observability daemon. See `docs/architecture.md` (design of record) and `docs/design.md`.

- **obs-svc-agg** (Go): consumes Protobuf-over-Kafka fleet events, aggregates state, serves snapshots.
- **tiny-monitor** (Rust): frameless floating macOS widget; polls `obs-svc-agg`'s `GET /state` at ~2s and renders fleet health + token runway. Lives in its own repo: [janearc/tiny-monitor](https://github.com/janearc/tiny-monitor).

---

## author

max toegang <max.toegang@ftml.net>
🤖 claude · claude-opus-4-8
🤖 bespoke locally trained models
