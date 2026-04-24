# Changelog

## [0.1.0] — 2026-04-24

### Added
- Kafka consumer via `confluent-kafka` (librdkafka)
- JSON message parsing: merges Kafka metadata into existing JSON rows
- Plain string fallback: wraps non-JSON values as `{"message": "..."}`
- Kafka metadata injected per message: `_topic`, `_partition`, `_offset`, `_kafka_timestamp`
- Existing `timestamp` / `@timestamp` fields preserved (not overwritten)
- Buffer management: size-based and time-based rotation
- PFC compression via `pfc_jsonl` binary after each rotation
- Manual offset commit **only after successful PFC compression** — no data loss on crash
- Optional S3 upload after compression
- SASL/SSL authentication support (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- Multi-topic and multi-partition support
- TOML configuration file
- Graceful shutdown on SIGTERM/SIGINT with buffer flush
- Background watchdog thread for time-based rotation
- `--version`, `--config` CLI flags
- Unit + resilience tests: all passing
- E2E integration test against Redpanda (Kafka-compatible)
