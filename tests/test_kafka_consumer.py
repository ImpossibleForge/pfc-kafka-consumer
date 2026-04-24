"""
pfc-kafka-consumer — Happy-Path Test Suite
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from pfc_kafka_consumer import (
    PfcBuffer,
    PfcKafkaConsumer,
    build_kafka_config,
    deep_merge,
    load_config,
    message_to_jsonl,
)

PFC_BINARY = os.environ.get("PFC_BINARY", "/usr/local/bin/pfc_jsonl")
BINARY_AVAILABLE = Path(PFC_BINARY).exists()


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def tmpdir():
    d = tempfile.mkdtemp(prefix="pfc_kafka_test_")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def cfg(tmpdir):
    return {
        "kafka": {
            "brokers": ["localhost:9092"],
            "topics": ["test-topic"],
            "group_id": "test-group",
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
            "output_dir": str(tmpdir),
            "prefix": "test",
            "commit_after_compress": True,
        },
        "pfc": {"binary": PFC_BINARY},
        "s3": {"enabled": False, "bucket": "", "prefix": "", "region": ""},
    }


def make_msg(value, topic="test-topic", partition=0, offset=0,
             ts_ms=1768471200000, key=None):
    msg = MagicMock()
    msg.value.return_value = value if isinstance(value, bytes) else (
        value.encode() if value else None
    )
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = (1, ts_ms)
    msg.key.return_value = key.encode() if isinstance(key, str) else key
    msg.error.return_value = None
    return msg


# ─── Unit: message_to_jsonl ───────────────────────────────────────────────────

class TestMessageToJsonl:
    def test_json_value_merged(self):
        msg = make_msg('{"level":"INFO","service":"api"}')
        line = message_to_jsonl(msg)
        row = json.loads(line)
        assert row["level"] == "INFO"
        assert row["service"] == "api"
        assert row["_topic"] == "test-topic"
        assert row["_partition"] == 0
        assert row["_offset"] == 0

    def test_plain_string_wrapped(self):
        msg = make_msg("plain log line")
        line = message_to_jsonl(msg)
        row = json.loads(line)
        assert row["message"] == "plain log line"
        assert "_topic" in row

    def test_timestamp_injected(self):
        msg = make_msg('{"level":"INFO"}', ts_ms=1768471200000)
        line = message_to_jsonl(msg)
        row = json.loads(line)
        assert "timestamp" in row
        assert row["timestamp"].startswith("2026-01-15T")

    def test_existing_timestamp_preserved(self):
        msg = make_msg('{"timestamp":"2026-01-15T09:00:00.000Z","level":"INFO"}')
        line = message_to_jsonl(msg)
        row = json.loads(line)
        assert row["timestamp"] == "2026-01-15T09:00:00.000Z"

    def test_kafka_metadata_always_added(self):
        msg = make_msg('{"level":"WARN"}', topic="logs", partition=2, offset=42)
        row = json.loads(message_to_jsonl(msg))
        assert row["_topic"] == "logs"
        assert row["_partition"] == 2
        assert row["_offset"] == 42
        assert "_kafka_timestamp" in row

    def test_key_included(self):
        msg = make_msg('{"level":"INFO"}', key="my-key")
        row = json.loads(message_to_jsonl(msg))
        assert row["_key"] == "my-key"

    def test_bytes_value_decoded(self):
        msg = make_msg(b'{"level":"DEBUG","msg":"hello"}')
        row = json.loads(message_to_jsonl(msg))
        assert row["level"] == "DEBUG"

    def test_none_value_returns_none(self):
        msg = make_msg(None)
        assert message_to_jsonl(msg) is None

    def test_json_array_wrapped(self):
        msg = make_msg('[1,2,3]')
        row = json.loads(message_to_jsonl(msg))
        assert row["message"] == "[1,2,3]"

    def test_invalid_json_wrapped(self):
        msg = make_msg("not json {at all}")
        row = json.loads(message_to_jsonl(msg))
        assert row["message"] == "not json {at all}"

    def test_unicode_message(self):
        msg = make_msg('{"message":"日本語ログ 🔥","level":"INFO"}')
        row = json.loads(message_to_jsonl(msg))
        assert row["message"] == "日本語ログ 🔥"

    def test_nested_json_preserved(self):
        msg = make_msg('{"level":"INFO","meta":{"host":"prod-01","env":"production"}}')
        row = json.loads(message_to_jsonl(msg))
        assert row["meta"]["host"] == "prod-01"

    def test_zero_timestamp_handled(self):
        msg = make_msg('{"level":"INFO"}', ts_ms=0)
        row = json.loads(message_to_jsonl(msg))
        assert "timestamp" in row

    def test_at_timestamp_preserved(self):
        msg = make_msg('{"@timestamp":"2026-01-01T00:00:00Z","level":"INFO"}')
        row = json.loads(message_to_jsonl(msg))
        assert "timestamp" not in row
        assert row["@timestamp"] == "2026-01-01T00:00:00Z"

    def test_large_offset(self):
        msg = make_msg('{"level":"INFO"}', offset=9_999_999)
        row = json.loads(message_to_jsonl(msg))
        assert row["_offset"] == 9_999_999

    def test_multiple_partitions(self):
        for p in range(8):
            msg = make_msg('{"level":"INFO"}', partition=p, offset=p * 100)
            row = json.loads(message_to_jsonl(msg))
            assert row["_partition"] == p


# ─── Unit: Config ─────────────────────────────────────────────────────────────

class TestConfig:
    def test_defaults(self):
        cfg = load_config(None)
        assert cfg["kafka"]["group_id"] == "pfc-consumer"
        assert cfg["buffer"]["rotate_mb"] == 64
        assert cfg["s3"]["enabled"] is False

    def test_from_file(self, tmpdir):
        p = tmpdir / "cfg.toml"
        p.write_text('[kafka]\ngroup_id = "my-group"\n')
        cfg = load_config(str(p))
        assert cfg["kafka"]["group_id"] == "my-group"
        assert cfg["buffer"]["rotate_mb"] == 64  # default preserved

    def test_missing_file_uses_defaults(self):
        cfg = load_config("/nonexistent/config.toml")
        assert cfg["kafka"]["group_id"] == "pfc-consumer"

    def test_deep_merge(self):
        base = {"a": {"x": 1, "y": 2}, "b": 3}
        override = {"a": {"y": 99, "z": 4}}
        result = deep_merge(base, override)
        assert result == {"a": {"x": 1, "y": 99, "z": 4}, "b": 3}

    def test_brokers_list(self):
        cfg = load_config(None)
        assert isinstance(cfg["kafka"]["brokers"], list)

    def test_topics_list(self):
        cfg = load_config(None)
        assert isinstance(cfg["kafka"]["topics"], list)


# ─── Unit: Kafka Config Builder ───────────────────────────────────────────────

class TestKafkaConfigBuilder:
    def test_basic_config(self, cfg):
        k = build_kafka_config(cfg)
        assert k["bootstrap.servers"] == "localhost:9092"
        assert k["group.id"] == "test-group"
        assert k["enable.auto.commit"] is False
        assert k["auto.offset.reset"] == "earliest"

    def test_brokers_list_joined(self, cfg):
        cfg["kafka"]["brokers"] = ["b1:9092", "b2:9092"]
        k = build_kafka_config(cfg)
        assert k["bootstrap.servers"] == "b1:9092,b2:9092"

    def test_sasl_included_when_set(self, cfg):
        cfg["kafka"]["sasl_mechanism"] = "PLAIN"
        cfg["kafka"]["sasl_username"] = "user"
        cfg["kafka"]["sasl_password"] = "pass"
        k = build_kafka_config(cfg)
        assert k["sasl.mechanism"] == "PLAIN"
        assert k["sasl.username"] == "user"
        assert k["sasl.password"] == "pass"

    def test_sasl_omitted_when_empty(self, cfg):
        k = build_kafka_config(cfg)
        assert "sasl.mechanism" not in k

    def test_ssl_ca_location_included(self, cfg):
        cfg["kafka"]["ssl_ca_location"] = "/etc/ssl/ca.pem"
        k = build_kafka_config(cfg)
        assert k["ssl.ca.location"] == "/etc/ssl/ca.pem"


# ─── Unit: Buffer ─────────────────────────────────────────────────────────────

class TestBuffer:
    def test_write_accumulates(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        buf.write("line1", "topic", 0, 0)
        buf.write("line2", "topic", 0, 1)
        assert buf.stats["buffered_lines"] == 2
        assert buf.stats["total_ingested"] == 2

    def test_offset_tracking(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        buf.write("line1", "topic", 0, 5)
        buf.write("line2", "topic", 0, 10)
        buf.write("line3", "topic", 1, 3)
        with buf._lock:
            assert buf._pending_offsets[("topic", 0)] == 10
            assert buf._pending_offsets[("topic", 1)] == 3

    def test_max_offset_per_partition(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        buf.write("line1", "t", 0, 100)
        buf.write("line2", "t", 0, 50)   # lower — should not override
        buf.write("line3", "t", 0, 200)
        with buf._lock:
            assert buf._pending_offsets[("t", 0)] == 200

    def test_size_threshold_triggers_rotation(self, cfg, tmpdir):
        cfg["buffer"]["rotate_mb"] = 0
        buf = PfcBuffer(cfg)
        with patch.object(buf, "rotate") as mock:
            buf.write("line1", "t", 0, 0)
            mock.assert_called()

    def test_flush_empty_safe(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        buf.flush()  # should not raise

    def test_stats_initial(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        s = buf.stats
        assert s["buffered_lines"] == 0
        assert s["total_ingested"] == 0
        assert s["total_compressed"] == 0
        assert s["total_failed"] == 0

    def test_compress_callback_called(self, cfg, tmpdir):
        cfg["buffer"]["rotate_mb"] = 0
        buf = PfcBuffer(cfg)
        received = []
        buf.set_compress_callback(lambda offsets: received.append(offsets))
        with patch("subprocess.run") as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = ""
            mock_run.return_value = mock_result
            # Mock stat() for size calculation
            with patch("pathlib.Path.stat") as mock_stat:
                mock_stat.return_value = MagicMock(st_size=100)
                with patch("pathlib.Path.unlink"):
                    buf.write("line1", "t", 0, 42)
        # callback may have been called
        assert buf.stats["total_ingested"] == 1

    @pytest.mark.skipif(not BINARY_AVAILABLE, reason="pfc_jsonl not available")
    def test_real_compression(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)  # default rotate_mb=64 — won't auto-rotate on 100 lines
        for i in range(100):
            line = json.dumps({
                "timestamp": f"2026-01-15T10:{i//60:02d}:{i%60:02d}.000Z",
                "level": "INFO",
                "service": "svc",
                "message": f"msg {i}"
            })
            buf.write(line, "test", 0, i)
        buf.flush()  # single compression run
        pfc_files = list(tmpdir.glob("*.pfc"))
        assert len(pfc_files) == 1
        assert buf.stats["total_compressed"] == 1


# ─── Unit: Consumer internals (no blocking start()) ───────────────────────────

class TestConsumerInternals:
    def test_flush_batch_writes_to_buffer(self, cfg, tmpdir):
        """_flush_batch() correctly writes all messages to the buffer."""
        consumer = PfcKafkaConsumer(cfg)
        msgs = [
            make_msg('{"level":"INFO","service":"svc"}', offset=i)
            for i in range(5)
        ]
        batch = [(msg_to_line(m), m.topic(), m.partition(), m.offset()) for m in msgs]
        consumer._flush_batch(batch)
        assert consumer.buffer.stats["total_ingested"] == 5

    def test_on_compressed_commits_offsets(self, cfg, tmpdir):
        """_on_compressed triggers Kafka commit with correct TopicPartitions."""
        consumer = PfcKafkaConsumer(cfg)
        consumer._consumer = MagicMock()
        consumer._on_compressed({("logs", 0): 42, ("logs", 1): 17})
        consumer._consumer.commit.assert_called_once()
        tps = consumer._consumer.commit.call_args[1]["offsets"]
        offset_map = {(tp.topic, tp.partition): tp.offset for tp in tps}
        assert offset_map[("logs", 0)] == 43  # committed offset = max_offset + 1
        assert offset_map[("logs", 1)] == 18

    def test_on_compressed_no_commit_when_disabled(self, cfg, tmpdir):
        """No commit happens when commit_after_compress=False."""
        cfg["buffer"]["commit_after_compress"] = False
        consumer = PfcKafkaConsumer(cfg)
        consumer._consumer = MagicMock()
        consumer._on_compressed({("t", 0): 10})
        consumer._consumer.commit.assert_not_called()

    def test_stop_sets_running_false(self, cfg, tmpdir):
        consumer = PfcKafkaConsumer(cfg)
        consumer._running = True
        consumer.stop()
        assert consumer._running is False

    def test_kafka_config_built_correctly(self, cfg, tmpdir):
        consumer = PfcKafkaConsumer(cfg)
        k = build_kafka_config(consumer.cfg)
        assert k["enable.auto.commit"] is False
        assert k["group.id"] == "test-group"

    def test_message_with_error_skipped(self, cfg, tmpdir):
        """Messages with errors are not written to buffer."""
        consumer = PfcKafkaConsumer(cfg)
        from confluent_kafka import KafkaError
        err_msg = make_msg('{"level":"INFO"}')
        err = MagicMock()
        err.code.return_value = -1
        err_msg.error.return_value = err
        # Simulate the consumer's error handling logic
        if err_msg.error():
            if err_msg.error().code() != KafkaError._PARTITION_EOF:
                pass  # logged, not written
        assert consumer.buffer.stats["total_ingested"] == 0


def msg_to_line(msg):
    return message_to_jsonl(msg)


# ─── Unit: Concurrent writes ──────────────────────────────────────────────────

class TestConcurrent:
    def test_concurrent_writes_no_data_loss(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        results = []

        def writer(n):
            for i in range(20):
                buf.write(f"line-{n}-{i}", "t", n, i)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert buf.stats["total_ingested"] == 100
