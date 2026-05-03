# pfc-kafka-consumer

**Kafka consumer for PFC-JSONL log compression** — consume messages from Kafka topics and compress them directly to `.pfc` format.

Commits Kafka offsets **only after successful PFC compression** — no data loss if the process crashes mid-flight.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Part of PFC-JSONL Ecosystem](https://img.shields.io/badge/ecosystem-PFC--JSONL-brightgreen)](https://github.com/ImpossibleForge/pfc-jsonl)

---

## How it fits in your pipeline

```
Kafka / Redpanda
      │  topic: app-logs, access-logs, ...
      ▼
pfc-kafka-consumer          ← this service
      │  pfc_jsonl compress  (after each rotation)
      │  commit offsets      (only on success)
      ▼
kafka_20260115_100000.pfc   →  local disk or S3
      │
      ▼
Query with DuckDB / pfc-gateway
```

---

## Quickstart

### 1. Install

```bash
pip install confluent-kafka toml
# Optional S3 upload:
pip install boto3
```

### 2. Download pfc_jsonl binary

```bash
# Linux x86_64
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x86_64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS ARM64
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

### 3. Configure

```bash
cp config/config.toml ./config.toml
# Edit brokers, topics, group_id
```

### 4. Start

```bash
python pfc_kafka_consumer.py --config config.toml
# 2026-01-15T10:00:00 [pfc-kafka] INFO pfc-kafka-consumer v0.1.0 started
# 2026-01-15T10:00:00 [pfc-kafka] INFO Topics: ['app-logs'] | Group: pfc-consumer
```

---

## Configuration

```toml
[kafka]
brokers           = ["localhost:9092"]
topics            = ["app-logs", "access-logs"]
group_id          = "pfc-consumer"
auto_offset_reset = "earliest"   # or "latest"
poll_timeout_sec  = 1.0
batch_size        = 500

# Optional auth
security_protocol = "PLAINTEXT"  # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
sasl_mechanism    = ""           # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
sasl_username     = ""
sasl_password     = ""
ssl_ca_location   = ""

[buffer]
rotate_mb             = 64
rotate_sec            = 3600
output_dir            = "/tmp/pfc-kafka"
prefix                = "kafka"
commit_after_compress = true     # safe default — commit only after successful compress

[pfc]
binary = "/usr/local/bin/pfc_jsonl"

[s3]
enabled = false
bucket  = "my-log-archive"
prefix  = "kafka-logs/"
region  = "us-east-1"
```

---

## Output format

Each Kafka message becomes one flat JSONL line. JSON messages are merged; plain strings are wrapped.

**JSON message:**
```json
{"timestamp": "2026-01-15T10:00:00.123Z", "level": "ERROR", "service": "payment"}
```
→ becomes:
```json
{
  "timestamp": "2026-01-15T10:00:00.123Z",
  "level": "ERROR",
  "service": "payment",
  "_topic": "app-logs",
  "_partition": 2,
  "_offset": 84712,
  "_kafka_timestamp": "2026-01-15T10:00:00.123Z"
}
```

**Plain string message:**
```
2026-01-15T10:00:00 ERROR payment failed
```
→ becomes:
```json
{
  "message": "2026-01-15T10:00:00 ERROR payment failed",
  "timestamp": "2026-01-15T10:00:00.123Z",
  "_topic": "app-logs",
  "_partition": 0,
  "_offset": 12345,
  "_kafka_timestamp": "2026-01-15T10:00:00.123Z"
}
```

---

## Offset commit safety

`commit_after_compress = true` (default):
- Messages are **not** committed to Kafka until the PFC file is written successfully
- If the process crashes before compression completes, messages are re-consumed on restart
- No data loss — at-least-once delivery guarantee

`commit_after_compress = false`:
- Offsets committed immediately after polling
- Higher throughput, but messages may be lost if compression fails

---

## Confluent Cloud / MSK / Redpanda Cloud

```toml
[kafka]
brokers           = ["pkc-xxxx.us-east-1.aws.confluent.cloud:9092"]
security_protocol = "SASL_SSL"
sasl_mechanism    = "PLAIN"
sasl_username     = "YOUR_API_KEY"
sasl_password     = "YOUR_API_SECRET"
```

---

## Querying compressed logs

```sql
-- DuckDB
INSTALL pfc FROM community;
LOAD pfc;

SELECT level, service, count(*)
FROM read_pfc_jsonl('kafka_20260115_100000.pfc',
                    ts_from=1768471200::BIGINT,
                    ts_to=1768471500::BIGINT)
WHERE line LIKE '%ERROR%'
GROUP BY level, service
ORDER BY 3 DESC;
```

---

## Running tests

```bash
pip install pytest confluent-kafka toml
pytest tests/test_kafka_consumer.py tests/test_resilience.py -v

# Full E2E (requires Docker):
python3 tests/e2e_integration_test.py
```

---

## Part of the PFC Ecosystem

**[→ View all PFC tools & integrations](https://github.com/ImpossibleForge/pfc-jsonl#ecosystem)**

| Direct integration | Why |
|---|---|
| [pfc-gateway](https://github.com/ImpossibleForge/pfc-gateway) | Query the archives pfc-kafka-consumer creates — HTTP REST, no DuckDB required |
| [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) | Alternative ingest — log pipeline instead of message queue |
| [pfc-vector](https://github.com/ImpossibleForge/pfc-vector) | Alternative ingest — high-performance HTTP sink |

---


---

## Disclaimer

PFC-Kafka-Consumer is an independent open-source project and is not affiliated with, endorsed by, or associated with the Apache Software Foundation, Apache Kafka, or Confluent.
## License

pfc-kafka-consumer (this repository) is released under the MIT License — see [LICENSE](LICENSE).

The PFC-JSONL binary (`pfc_jsonl`) is proprietary software — free for personal and open-source use. Commercial use requires a license: [info@impossibleforge.com](mailto:info@impossibleforge.com)