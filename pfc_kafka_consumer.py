#!/usr/bin/env python3
"""
pfc-kafka-consumer v0.1.0
Kafka consumer that compresses log messages directly to PFC format.

Consumes messages from Kafka topics → buffers as JSONL → pfc_jsonl compress → optional S3 upload.
Commits offsets only after successful PFC compression — no data loss on crash.
"""

import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import toml
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

VERSION = "0.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pfc-kafka] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("pfc-kafka")

# ─── Config ───────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "kafka": {
        "brokers": ["localhost:9092"],
        "topics": ["app-logs"],
        "group_id": "pfc-consumer",
        "auto_offset_reset": "earliest",
        "poll_timeout_sec": 1.0,
        "batch_size": 500,
        "security_protocol": "PLAINTEXT",
        "sasl_mechanism": "",
        "sasl_username": "",
        "sasl_password": "",
        "ssl_ca_location": "",
    },
    "buffer": {
        "rotate_mb": 64,
        "rotate_sec": 3600,
        "output_dir": "/tmp/pfc-kafka",
        "prefix": "kafka",
        "commit_after_compress": True,
    },
    "pfc": {
        "binary": "/usr/local/bin/pfc_jsonl",
    },
    "s3": {
        "enabled": False,
        "bucket": "",
        "prefix": "kafka-logs/",
        "region": "us-east-1",
    },
}


def deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(path: Optional[str] = None) -> dict:
    cfg = DEFAULT_CONFIG.copy()
    cfg["kafka"] = DEFAULT_CONFIG["kafka"].copy()
    cfg["buffer"] = DEFAULT_CONFIG["buffer"].copy()
    cfg["pfc"] = DEFAULT_CONFIG["pfc"].copy()
    cfg["s3"] = DEFAULT_CONFIG["s3"].copy()
    if path and Path(path).exists():
        with open(path) as f:
            user_cfg = toml.load(f)
        cfg = deep_merge(cfg, user_cfg)
    return cfg


# ─── Message → JSONL ──────────────────────────────────────────────────────────

def message_to_jsonl(msg) -> Optional[str]:
    """
    Convert a Kafka message to a JSONL line.
    - If value is valid JSON object: merge kafka metadata into it.
    - If value is plain string or bytes: wrap as {"message": ...} + metadata.
    - Returns None if message has no value (tombstone).
    """
    value = msg.value()
    if value is None:
        return None

    # Decode bytes
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            value = value.decode("utf-8", errors="replace")

    # Try parse as JSON
    row = {}
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            row = parsed
        else:
            row["message"] = value
    except (json.JSONDecodeError, ValueError):
        row["message"] = value

    # Kafka metadata (prefixed with _ to avoid collision)
    ts_type, ts_ms = msg.timestamp()
    if ts_ms and ts_ms > 0:
        ts_sec = ts_ms / 1000.0
        dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
        kafka_ts = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ts_ms % 1000):03d}Z"
    else:
        kafka_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Only inject timestamp if not already present
    if "timestamp" not in row and "@timestamp" not in row:
        row["timestamp"] = kafka_ts

    row["_topic"] = msg.topic()
    row["_partition"] = msg.partition()
    row["_offset"] = msg.offset()
    row["_kafka_timestamp"] = kafka_ts

    # Include message key if present
    key = msg.key()
    if key is not None:
        try:
            row["_key"] = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        except Exception:
            pass

    return json.dumps(row, ensure_ascii=False)


# ─── Buffer & Compression ─────────────────────────────────────────────────────

class PfcBuffer:
    """
    Thread-safe write buffer with size + time-based rotation.
    Tracks pending offsets per partition for safe Kafka commit after compression.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.buf_cfg = cfg["buffer"]
        self.pfc_cfg = cfg["pfc"]
        self.s3_cfg = cfg["s3"]

        self.output_dir = Path(self.buf_cfg["output_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.rotate_bytes = int(self.buf_cfg["rotate_mb"]) * 1024 * 1024
        self.rotate_sec = int(self.buf_cfg["rotate_sec"])
        self.prefix = self.buf_cfg["prefix"]
        self.binary = self.pfc_cfg["binary"]
        self.commit_after_compress = self.buf_cfg.get("commit_after_compress", True)

        self._lock = threading.Lock()
        self._lines: list[str] = []
        self._size: int = 0
        self._last_rotate = time.monotonic()

        # Pending offsets: {(topic, partition): max_offset}
        self._pending_offsets: dict[tuple, int] = {}
        # Offsets ready to commit (after successful compression)
        self._committed_offsets: list[dict] = []

        self._total_ingested = 0
        self._total_compressed = 0
        self._total_failed = 0

        self._on_compress_callback = None  # called with offsets after compress

    def set_compress_callback(self, fn):
        self._on_compress_callback = fn

    def write(self, line: str, topic: str, partition: int, offset: int) -> bool:
        """Write a line and track its offset. Returns True if rotation triggered."""
        with self._lock:
            encoded = line + "\n"
            self._lines.append(encoded)
            self._size += len(encoded.encode())
            self._total_ingested += 1
            key = (topic, partition)
            self._pending_offsets[key] = max(
                self._pending_offsets.get(key, -1), offset
            )
            should_rotate = (
                self._size >= self.rotate_bytes
                or (time.monotonic() - self._last_rotate) >= self.rotate_sec
            )
        if should_rotate:
            self.rotate()
            return True
        return False

    def flush(self) -> None:
        with self._lock:
            has_data = bool(self._lines)
        if has_data:
            self.rotate()

    def rotate(self) -> None:
        with self._lock:
            if not self._lines:
                return
            lines_snapshot = self._lines[:]
            offsets_snapshot = dict(self._pending_offsets)
            self._lines = []
            self._size = 0
            self._pending_offsets = {}
            self._last_rotate = time.monotonic()

        slug = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        jsonl_path = self.output_dir / f"{self.prefix}_{slug}.jsonl"
        pfc_path = self.output_dir / f"{self.prefix}_{slug}.pfc"

        try:
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.writelines(lines_snapshot)

            jsonl_size = jsonl_path.stat().st_size

            result = subprocess.run(
                [self.binary, "compress", str(jsonl_path), str(pfc_path)],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                log.error("pfc_jsonl compress failed: %s", result.stderr)
                self._total_failed += 1
                return

            pfc_size = pfc_path.stat().st_size
            ratio = (pfc_size / jsonl_size * 100) if jsonl_size > 0 else 0
            log.info(
                "Compressed %d lines → %s (%.1f%% ratio)",
                len(lines_snapshot),
                pfc_path.name,
                ratio,
            )
            self._total_compressed += 1

            jsonl_path.unlink(missing_ok=True)

            # S3 upload
            if self.s3_cfg.get("enabled"):
                self._upload_s3(pfc_path)

            # Signal that these offsets can now be committed
            if self._on_compress_callback and offsets_snapshot:
                self._on_compress_callback(offsets_snapshot)

        except subprocess.TimeoutExpired:
            log.error("pfc_jsonl compress timed out")
            self._total_failed += 1
        except FileNotFoundError:
            log.error("pfc_jsonl binary not found: %s", self.binary)
            self._total_failed += 1
        except Exception as exc:
            log.error("Rotation error: %s", exc)
            jsonl_path.unlink(missing_ok=True)
            self._total_failed += 1

    def _upload_s3(self, pfc_path: Path) -> None:
        try:
            import boto3
            s3 = boto3.client("s3", region_name=self.s3_cfg["region"])
            key = self.s3_cfg["prefix"].rstrip("/") + "/" + pfc_path.name
            s3.upload_file(str(pfc_path), self.s3_cfg["bucket"], key)
            log.info("Uploaded → s3://%s/%s", self.s3_cfg["bucket"], key)
            pfc_path.unlink(missing_ok=True)
        except Exception as exc:
            log.error("S3 upload failed: %s", exc)

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "buffered_lines": len(self._lines),
                "buffered_bytes": self._size,
                "total_ingested": self._total_ingested,
                "total_compressed": self._total_compressed,
                "total_failed": self._total_failed,
                "pending_partitions": len(self._pending_offsets),
            }


# ─── Consumer ─────────────────────────────────────────────────────────────────

def build_kafka_config(cfg: dict) -> dict:
    k = cfg["kafka"]
    conf = {
        "bootstrap.servers": ",".join(k["brokers"]) if isinstance(k["brokers"], list) else k["brokers"],
        "group.id": k["group_id"],
        "auto.offset.reset": k["auto_offset_reset"],
        "enable.auto.commit": False,  # always manual commit
        "security.protocol": k.get("security_protocol", "PLAINTEXT"),
    }
    if k.get("sasl_mechanism"):
        conf["sasl.mechanism"] = k["sasl_mechanism"]
    if k.get("sasl_username"):
        conf["sasl.username"] = k["sasl_username"]
    if k.get("sasl_password"):
        conf["sasl.password"] = k["sasl_password"]
    if k.get("ssl_ca_location"):
        conf["ssl.ca.location"] = k["ssl_ca_location"]
    return conf


class PfcKafkaConsumer:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.buffer = PfcBuffer(cfg)
        self._consumer: Optional[Consumer] = None
        self._running = False
        self._committed_offsets: dict[tuple, int] = {}
        self._lock = threading.Lock()

        self.buffer.set_compress_callback(self._on_compressed)

    def _on_compressed(self, offsets: dict) -> None:
        """Called after successful PFC compression — commit these offsets."""
        with self._lock:
            for (topic, partition), offset in offsets.items():
                self._committed_offsets[(topic, partition)] = offset
        if self._consumer and self.cfg["buffer"].get("commit_after_compress", True):
            try:
                tps = [
                    TopicPartition(topic, partition, offset + 1)
                    for (topic, partition), offset in offsets.items()
                ]
                self._consumer.commit(offsets=tps, asynchronous=True)
                log.info("Committed %d partition offset(s)", len(tps))
            except Exception as exc:
                log.warning("Offset commit failed: %s", exc)

    def start(self) -> None:
        k_cfg = self.cfg["kafka"]
        topics = k_cfg["topics"] if isinstance(k_cfg["topics"], list) else [k_cfg["topics"]]
        poll_timeout = float(k_cfg.get("poll_timeout_sec", 1.0))
        batch_size = int(k_cfg.get("batch_size", 500))

        conf = build_kafka_config(self.cfg)
        self._consumer = Consumer(conf)
        self._consumer.subscribe(topics)
        self._running = True

        log.info("pfc-kafka-consumer v%s started", VERSION)
        log.info("Brokers: %s", conf["bootstrap.servers"])
        log.info("Topics: %s | Group: %s", topics, conf["group.id"])
        log.info("Output: %s | rotate_mb=%d rotate_sec=%d",
                 self.cfg["buffer"]["output_dir"],
                 self.cfg["buffer"]["rotate_mb"],
                 self.cfg["buffer"]["rotate_sec"])

        # Watchdog thread for time-based rotation
        watchdog = threading.Thread(target=self._watchdog, daemon=True)
        watchdog.start()

        try:
            batch = []
            while self._running:
                msg = self._consumer.poll(poll_timeout)
                if msg is None:
                    if batch:
                        self._flush_batch(batch)
                        batch = []
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("Kafka error: %s", msg.error())
                    continue

                line = message_to_jsonl(msg)
                if line:
                    batch.append((line, msg.topic(), msg.partition(), msg.offset()))

                if len(batch) >= batch_size:
                    self._flush_batch(batch)
                    batch = []

        except KafkaException as exc:
            log.error("Kafka exception: %s", exc)
        finally:
            if batch:
                self._flush_batch(batch)
            log.info("Flushing buffer on shutdown…")
            self.buffer.flush()
            if self._consumer:
                self._consumer.close()
            log.info("pfc-kafka-consumer stopped. Stats: %s", self.buffer.stats)

    def _flush_batch(self, batch: list) -> None:
        for line, topic, partition, offset in batch:
            self.buffer.write(line, topic, partition, offset)

    def _watchdog(self) -> None:
        while self._running:
            time.sleep(30)
            elapsed = time.monotonic() - self.buffer._last_rotate
            if elapsed >= self.buffer.rotate_sec and self.buffer._lines:
                log.info("Watchdog: time-based rotation (%.0fs elapsed)", elapsed)
                self.buffer.flush()

    def stop(self) -> None:
        self._running = False

    @property
    def stats(self) -> dict:
        return {
            **self.buffer.stats,
            "running": self._running,
            "committed_partitions": len(self._committed_offsets),
        }


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description=f"pfc-kafka-consumer v{VERSION}")
    parser.add_argument("--config", "-c", default="", help="Path to TOML config file")
    parser.add_argument("--version", action="store_true", help="Print version and exit")
    args = parser.parse_args()

    if args.version:
        print(VERSION)
        sys.exit(0)

    cfg = load_config(args.config or None)

    if not Path(cfg["pfc"]["binary"]).exists():
        log.error("pfc_jsonl binary not found: %s", cfg["pfc"]["binary"])
        sys.exit(1)

    consumer = PfcKafkaConsumer(cfg)

    def _shutdown(signum, frame):
        log.info("Signal %d received — stopping consumer", signum)
        consumer.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    consumer.start()


if __name__ == "__main__":
    main()
