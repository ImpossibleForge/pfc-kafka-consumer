#!/usr/bin/env python3
"""
pfc-kafka-consumer — End-to-End Integration Test
Spins up Redpanda (Kafka-compatible) via Docker, produces messages,
runs pfc-kafka-consumer, verifies PFC output is correct and queryable.

Requirements: Docker, pfc_jsonl binary, DuckDB binary
Run: python3 tests/e2e_integration_test.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from pfc_kafka_consumer import PfcBuffer, PfcKafkaConsumer, load_config, message_to_jsonl

PFC_BINARY = os.environ.get("PFC_BINARY", "/usr/local/bin/pfc_jsonl")
DUCKDB_BINARY = os.environ.get("DUCKDB_BINARY", "/usr/local/bin/duckdb")
KAFKA_PORT = 9092
KAFKA_CONTAINER = "pfc-kafka-e2e"
KAFKA_BIN = "/opt/kafka/bin"

PASS = 0
FAIL = 0


def ok(msg):
    global PASS; PASS += 1; print(f"  OK   {msg}")


def fail(msg):
    global FAIL; FAIL += 1; print(f"  FAIL {msg}")


# ─── Redpanda Lifecycle ───────────────────────────────────────────────────────

def start_kafka():
    subprocess.run(["docker", "rm", "-f", KAFKA_CONTAINER], capture_output=True)
    r = subprocess.run([
        "docker", "run", "-d",
        "--name", KAFKA_CONTAINER,
        "--network", "host",
        "-e", "KAFKA_NODE_ID=1",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{KAFKA_PORT}",
        "-e", f"KAFKA_LISTENERS=PLAINTEXT://localhost:{KAFKA_PORT},CONTROLLER://localhost:9093",
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", f"KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
        "apache/kafka:latest",
    ], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  FAIL Docker start failed: {r.stderr}")
        return False
    # Wait until broker is ready
    for _ in range(20):
        time.sleep(2)
        check = subprocess.run([
            "docker", "exec", KAFKA_CONTAINER,
            f"{KAFKA_BIN}/kafka-broker-api-versions.sh",
            "--bootstrap-server", f"localhost:{KAFKA_PORT}",
        ], capture_output=True, text=True)
        if check.returncode == 0:
            return True
    print("  FAIL Kafka broker did not become ready in time")
    return False


def stop_kafka():
    subprocess.run(["docker", "rm", "-f", KAFKA_CONTAINER], capture_output=True)


def create_topic(topic: str):
    r = subprocess.run([
        "docker", "exec", KAFKA_CONTAINER,
        f"{KAFKA_BIN}/kafka-topics.sh",
        "--create", "--topic", topic,
        "--partitions", "3",
        "--replication-factor", "1",
        "--bootstrap-server", f"localhost:{KAFKA_PORT}",
    ], capture_output=True, text=True)
    return r.returncode == 0


def produce_messages(topic: str, messages: list[str]) -> bool:
    input_data = "\n".join(messages)
    r = subprocess.run([
        "docker", "exec", "-i", KAFKA_CONTAINER,
        f"{KAFKA_BIN}/kafka-console-producer.sh",
        "--topic", topic,
        "--bootstrap-server", f"localhost:{KAFKA_PORT}",
    ], input=input_data, capture_output=True, text=True)
    return r.returncode == 0


# ─── Main E2E ─────────────────────────────────────────────────────────────────

def run():
    tmpdir = Path(tempfile.mkdtemp(prefix="pfc_kafka_e2e_"))
    try:
        # ── Step 1: Start Redpanda ────────────────────────────────────────
        print("\n[1] Start Apache Kafka (KRaft mode)")
        if not start_kafka():
            fail("Kafka failed to start — aborting")
            return
        ok("Kafka started on port " + str(KAFKA_PORT))

        # ── Step 2: Create topic + produce messages ───────────────────────
        print("\n[2] Create topic + produce messages")
        TOPIC = "pfc-e2e-test"
        if not create_topic(TOPIC):
            fail("Topic creation failed")
            return
        ok(f"Topic '{TOPIC}' created (3 partitions)")

        messages = []
        for i in range(500):
            ts_ms = 1768471200000 + i * 30
            dt_s = ts_ms // 1000
            dt_ms = ts_ms % 1000
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(dt_s, tz=timezone.utc)
            ts = dt.strftime(f"%Y-%m-%dT%H:%M:%S.{dt_ms:03d}Z")
            level = ["INFO", "INFO", "WARN", "INFO", "ERROR"][i % 5]
            msg = json.dumps({
                "timestamp": ts,
                "level": level,
                "service": ["api-gateway", "auth", "payment"][i % 3],
                "message": f"request processed id={i}",
                "http_status": 200 + (i % 3) * 100,
            })
            messages.append(msg)

        if produce_messages(TOPIC, messages):
            ok(f"Produced {len(messages)} messages to '{TOPIC}'")
        else:
            fail("Message production failed")
            return

        # ── Step 3: Run consumer ──────────────────────────────────────────
        print("\n[3] Run pfc-kafka-consumer")
        cfg = {
            "kafka": {
                "brokers": [f"localhost:{KAFKA_PORT}"],
                "topics": [TOPIC],
                "group_id": "pfc-e2e-group",
                "auto_offset_reset": "earliest",
                "poll_timeout_sec": 1.0,
                "batch_size": 500,
                "security_protocol": "PLAINTEXT",
                "sasl_mechanism": "", "sasl_username": "",
                "sasl_password": "", "ssl_ca_location": "",
            },
            "buffer": {
                "rotate_mb": 64,  # buffer all messages, flush on stop
                "rotate_sec": 9999,
                "output_dir": str(tmpdir),
                "prefix": "e2e",
                "commit_after_compress": True,
            },
            "pfc": {"binary": PFC_BINARY},
            "s3": {"enabled": False, "bucket": "", "prefix": "", "region": ""},
        }

        consumer = PfcKafkaConsumer(cfg)
        consumed_count = [0]
        compressed_count = [0]
        committed_offsets = []

        original_write = consumer.buffer.write
        def counting_write(line, topic, partition, offset):
            consumed_count[0] += 1
            return original_write(line, topic, partition, offset)
        consumer.buffer.write = counting_write

        original_callback = None
        def track_callback(offsets):
            compressed_count[0] += 1
            committed_offsets.append(offsets)
        consumer.buffer.set_compress_callback(track_callback)

        # Run consumer in thread, stop after consuming all messages
        def run_consumer():
            consumer.start()

        t = threading.Thread(target=run_consumer, daemon=True)
        t.start()

        # Wait until all 500 messages consumed (max 45s)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if consumed_count[0] >= 500:
                break
            time.sleep(0.5)
        consumer.stop()
        t.join(timeout=15)  # allow flush + compression to finish

        if consumed_count[0] >= 500:
            ok(f"Consumer ingested {consumed_count[0]} messages")
        else:
            fail(f"Only consumed {consumed_count[0]}/500 messages")
            return

        # ── Step 4: Verify PFC files created ─────────────────────────────
        print("\n[4] Verify PFC output files")
        pfc_files = list(tmpdir.glob("*.pfc"))
        if pfc_files:
            ok(f"{len(pfc_files)} PFC file(s) created")
        else:
            fail("No PFC files created")
            return

        total_pfc_bytes = sum(f.stat().st_size for f in pfc_files)
        ok(f"Total PFC size: {total_pfc_bytes // 1024} KB")

        # ── Step 5: pfc_jsonl info on each file ───────────────────────────
        print("\n[5] pfc_jsonl info")
        for pfc in pfc_files[:2]:
            r = subprocess.run([PFC_BINARY, "info", str(pfc)],
                               capture_output=True, text=True)
            if r.returncode == 0:
                ok(f"pfc_jsonl info OK: {pfc.name}")
                for line in r.stdout.strip().splitlines():
                    print(f"     {line}")
            else:
                fail(f"pfc_jsonl info failed: {r.stderr}")

        # ── Step 6: Decompress + verify structure ─────────────────────────
        print("\n[6] Decompress roundtrip")
        total_rows = 0
        for pfc in pfc_files:
            out = tmpdir / (pfc.stem + "_roundtrip.jsonl")
            r = subprocess.run([PFC_BINARY, "decompress", str(pfc), str(out)],
                               capture_output=True, text=True)
            if r.returncode == 0:
                rows = out.read_text().splitlines()
                total_rows += len(rows)
                # Verify structure of first row
                if rows:
                    first = json.loads(rows[0])
                    required = {"timestamp", "_topic", "_partition", "_offset", "_kafka_timestamp"}
                    missing = required - set(first.keys())
                    if not missing:
                        ok(f"{pfc.name}: {len(rows)} rows, structure OK")
                    else:
                        fail(f"{pfc.name}: missing fields {missing}")
            else:
                fail(f"Decompress failed: {r.stderr}")

        if total_rows >= 500:
            ok(f"Total decompressed rows: {total_rows} (expected >= 500)")
        else:
            fail(f"Total decompressed rows: {total_rows} (expected >= 500)")

        # ── Step 7: pfc_jsonl query (timestamp filter) ────────────────────
        print("\n[7] pfc_jsonl query — timestamp range")
        for pfc in pfc_files[:1]:
            r = subprocess.run([
                PFC_BINARY, "query", str(pfc),
                "--from", "2026-01-15T10:00",
                "--to", "2026-01-15T10:01",
            ], capture_output=True, text=True)
            if r.returncode == 0:
                ok(f"pfc_jsonl query OK on {pfc.name}")
                for line in r.stdout.strip().splitlines():
                    print(f"     {line}")
                if "Matching blocks" in r.stdout:
                    ok("Block index used for filtering")
            else:
                fail(f"Query failed: {r.stderr}")

        # ── Step 8: DuckDB query ──────────────────────────────────────────
        print("\n[8] DuckDB read_pfc_jsonl")
        for pfc in pfc_files[:1]:
            sql = (
                "LOAD pfc;\n"
                "SELECT json_extract_string(line, '$.level') AS level, count(*) AS count "
                f"FROM read_pfc_jsonl('{pfc}', "
                "ts_from=CAST(1768471200 AS BIGINT), ts_to=CAST(1768471500 AS BIGINT)) "
                "WHERE line LIKE '%T10:0%' GROUP BY 1 ORDER BY 2 DESC;"
            )
            r = subprocess.run([DUCKDB_BINARY], input=sql, capture_output=True, text=True)
            if r.returncode == 0:
                ok(f"DuckDB query OK on {pfc.name}")
                for line in r.stdout.strip().splitlines():
                    print(f"     {line}")
                if "INFO" in r.stdout:
                    ok("DuckDB: INFO rows found")
            else:
                fail(f"DuckDB failed: {r.stderr}")

        # ── Step 9: Verify Kafka metadata in rows ─────────────────────────
        print("\n[9] Kafka metadata verification")
        for pfc in pfc_files[:1]:
            out = tmpdir / "meta_check.jsonl"
            r = subprocess.run([PFC_BINARY, "decompress", str(pfc), str(out)],
                               capture_output=True, text=True)
            if r.returncode == 0:
                sample = [json.loads(l) for l in out.read_text().splitlines()[:10]]
                topics_found = {r.get("_topic") for r in sample}
                if TOPIC in topics_found:
                    ok(f"_topic field correctly set to '{TOPIC}'")
                else:
                    fail(f"_topic not found in rows: {topics_found}")
                partitions = {r.get("_partition") for r in sample}
                if len(partitions) > 0:
                    ok(f"Partition data present: {sorted(partitions)}")
                offsets = [r.get("_offset") for r in sample if r.get("_offset") is not None]
                if offsets:
                    ok(f"Offset data present (sample: {offsets[:3]})")
                services = {r.get("service") for r in sample}
                if len(services) > 1:
                    ok(f"Multiple services correctly preserved: {services}")

        # ── Step 10: .bidx index for pfc-gateway ─────────────────────────
        print("\n[10] .bidx index (pfc-gateway compatibility)")
        for pfc in pfc_files:
            bidx = Path(str(pfc) + ".bidx")
            if bidx.exists():
                ok(f".bidx present → {pfc.name} queryable by pfc-gateway")
            else:
                fail(f".bidx missing for {pfc.name}")

    finally:
        print("\n[cleanup] Stopping Kafka…")
        stop_kafka()
        shutil.rmtree(tmpdir, ignore_errors=True)
        sep = "=" * 52
        print(f"\n{sep}")
        print(f"  RESULT: {PASS} passed, {FAIL} failed")
        print(sep)
        sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    run()
