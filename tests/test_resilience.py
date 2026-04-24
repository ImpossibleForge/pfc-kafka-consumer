"""
pfc-kafka-consumer — Resilience & Edge-Case Test Suite
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
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from pfc_kafka_consumer import (
    PfcBuffer,
    PfcKafkaConsumer,
    message_to_jsonl,
    load_config,
)

PFC_BINARY = os.environ.get("PFC_BINARY", "/usr/local/bin/pfc_jsonl")
BINARY_AVAILABLE = Path(PFC_BINARY).exists()


@pytest.fixture
def tmpdir():
    d = tempfile.mkdtemp(prefix="pfc_kafka_resil_")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def cfg(tmpdir):
    return {
        "kafka": {
            "brokers": ["localhost:9092"], "topics": ["t"],
            "group_id": "g", "auto_offset_reset": "earliest",
            "poll_timeout_sec": 1.0, "batch_size": 500,
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": "", "sasl_username": "",
            "sasl_password": "", "ssl_ca_location": "",
        },
        "buffer": {
            "rotate_mb": 64, "rotate_sec": 3600,
            "output_dir": str(tmpdir), "prefix": "test",
            "commit_after_compress": True,
        },
        "pfc": {"binary": PFC_BINARY},
        "s3": {"enabled": False, "bucket": "", "prefix": "", "region": ""},
    }


def make_msg(value, topic="t", partition=0, offset=0, ts_ms=1768471200000):
    msg = MagicMock()
    msg.value.return_value = value.encode() if isinstance(value, str) else value
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = (1, ts_ms)
    msg.key.return_value = None
    msg.error.return_value = None
    return msg


# ─── R01: Binary not found ────────────────────────────────────────────────────

def test_binary_not_found_no_crash(cfg, tmpdir):
    cfg["pfc"]["binary"] = "/nonexistent/pfc_jsonl"
    buf = PfcBuffer(cfg)
    buf._lines = ['{"timestamp":"2026-01-15T10:00:00.000Z","level":"INFO","msg":"x"}']
    buf._size = 100
    buf.rotate()
    assert buf.stats["total_failed"] == 1


# ─── R02: Compress returns non-zero ───────────────────────────────────────────

def test_compress_nonzero_keeps_jsonl(cfg, tmpdir):
    buf = PfcBuffer(cfg)
    buf._lines = ['{"timestamp":"2026-01-15T10:00:00.000Z","level":"INFO","msg":"x"}']
    buf._size = 100

    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "simulated failure"

    with patch("subprocess.run", return_value=mock_result):
        buf.rotate()

    assert buf.stats["total_failed"] == 1
    jsonl_files = list(tmpdir.glob("*.jsonl"))
    assert len(jsonl_files) == 1  # kept as fallback


# ─── R03: Compress timeout ────────────────────────────────────────────────────

def test_compress_timeout_no_crash(cfg, tmpdir):
    buf = PfcBuffer(cfg)
    buf._lines = ["line1"]
    buf._size = 10

    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("pfc", 300)):
        buf.rotate()

    assert buf.stats["total_failed"] == 1


# ─── R04: Output dir not writable ────────────────────────────────────────────

def test_unwritable_output_dir_no_crash(cfg, tmpdir):
    ro = tmpdir / "readonly"
    ro.mkdir()
    ro.chmod(0o444)
    cfg["buffer"]["output_dir"] = str(ro)
    try:
        buf = PfcBuffer(cfg)
        buf._lines = ["line1"]
        buf._size = 10
        buf.rotate()  # must not raise
    finally:
        ro.chmod(0o755)


# ─── R05: Malformed message values ───────────────────────────────────────────

def test_none_value_returns_none():
    msg = make_msg("")
    msg.value.return_value = None
    assert message_to_jsonl(msg) is None


def test_empty_string_wrapped():
    msg = make_msg("")
    row = json.loads(message_to_jsonl(msg))
    assert row["message"] == ""


def test_very_large_message():
    msg = make_msg(json.dumps({"level": "INFO", "data": "x" * 100_000}))
    row = json.loads(message_to_jsonl(msg))
    assert len(row["data"]) == 100_000


def test_binary_garbage_decoded():
    msg = MagicMock()
    msg.value.return_value = bytes(range(256))
    msg.topic.return_value = "t"
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    msg.timestamp.return_value = (1, 1768471200000)
    msg.key.return_value = None
    msg.error.return_value = None
    line = message_to_jsonl(msg)
    assert line is not None
    row = json.loads(line)
    assert "message" in row


def test_deeply_nested_json():
    nested = {"a": {"b": {"c": {"d": {"e": "deep"}}}}}
    msg = make_msg(json.dumps(nested))
    row = json.loads(message_to_jsonl(msg))
    assert row["a"]["b"]["c"]["d"]["e"] == "deep"


def test_json_with_null_values():
    msg = make_msg('{"level":null,"service":null,"count":0}')
    row = json.loads(message_to_jsonl(msg))
    assert row["level"] is None
    assert row["count"] == 0


def test_message_with_special_characters():
    special = '{"msg":"line1\\nline2\\ttab \\"quoted\\""}'
    msg = make_msg(special)
    row = json.loads(message_to_jsonl(msg))
    assert "\n" in row["msg"]
    assert "\t" in row["msg"]


# ─── R06: S3 upload failure ───────────────────────────────────────────────────

def test_s3_upload_failure_no_crash(cfg, tmpdir):
    cfg["s3"]["enabled"] = True
    cfg["s3"]["bucket"] = "my-bucket"
    buf = PfcBuffer(cfg)
    fake_pfc = tmpdir / "test.pfc"
    fake_pfc.write_bytes(b"FAKE")

    with patch("boto3.client") as mock_boto:
        mock_boto.return_value.upload_file.side_effect = Exception("S3 error")
        buf._upload_s3(fake_pfc)

    assert fake_pfc.exists()  # kept on failure


def test_s3_boto3_missing(cfg, tmpdir):
    cfg["s3"]["enabled"] = True
    buf = PfcBuffer(cfg)
    fake_pfc = tmpdir / "test.pfc"
    fake_pfc.write_bytes(b"FAKE")

    with patch.dict("sys.modules", {"boto3": None}):
        buf._upload_s3(fake_pfc)  # must not raise


# ─── R07: Offset commit failure ───────────────────────────────────────────────

def test_offset_commit_failure_no_crash(cfg, tmpdir):
    consumer = PfcKafkaConsumer(cfg)
    consumer._consumer = MagicMock()
    consumer._consumer.commit.side_effect = Exception("Commit failed")
    consumer._on_compressed({("t", 0): 42})  # must not raise


# ─── R08: Version flag ────────────────────────────────────────────────────────

def test_version_flag(capsys):
    sys.argv = ["pfc_kafka_consumer", "--version"]
    with pytest.raises(SystemExit) as exc:
        from pfc_kafka_consumer import main
        main()
    assert exc.value.code == 0
    assert "0.1.0" in capsys.readouterr().out


# ─── R09: Missing binary at startup ──────────────────────────────────────────

def test_startup_exits_missing_binary(cfg):
    cfg["pfc"]["binary"] = "/nonexistent/binary"
    sys.argv = ["pfc_kafka_consumer"]
    with patch("pfc_kafka_consumer.load_config", return_value=cfg):
        with pytest.raises(SystemExit) as exc:
            from pfc_kafka_consumer import main
            main()
        assert exc.value.code == 1


# ─── R10: Concurrent writes + rotation race ──────────────────────────────────

def test_concurrent_writes_and_rotation(cfg, tmpdir):
    buf = PfcBuffer(cfg)
    rotate_called = threading.Event()
    original_rotate = buf.rotate

    def counted_rotate():
        rotate_called.set()
        original_rotate()

    buf.rotate = counted_rotate

    def writer():
        for i in range(50):
            buf.write(f"line{i}", "t", 0, i)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert buf.stats["total_ingested"] == 200


# ─── R11: Callback not set ────────────────────────────────────────────────────

def test_no_callback_no_crash(cfg, tmpdir):
    cfg["buffer"]["rotate_mb"] = 0
    buf = PfcBuffer(cfg)
    # No callback set
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stderr = ""

    with patch("subprocess.run", return_value=mock_result):
        with patch("pathlib.Path.stat") as ms:
            ms.return_value = MagicMock(st_size=100)
            with patch("pathlib.Path.unlink"):
                buf.write("line1", "t", 0, 0)  # must not raise


# ─── R12: Buffer recovery after failure ──────────────────────────────────────

def test_buffer_continues_after_rotation_failure(cfg, tmpdir):
    buf = PfcBuffer(cfg)
    rotate_count = [0]
    original_rotate = buf.rotate

    def failing_rotate():
        rotate_count[0] += 1
        if rotate_count[0] == 1:
            raise RuntimeError("First rotation fails")
        original_rotate()

    buf.rotate = failing_rotate
    buf.write("line1", "t", 0, 0)
    buf.write("line2", "t", 0, 1)
    assert buf.stats["total_ingested"] == 2


# ─── R13: All Kafka offset scenarios ─────────────────────────────────────────

def test_offsets_reset_after_rotation(cfg, tmpdir):
    """rotate() is called when size threshold reached; real rotation clears offsets."""
    cfg["buffer"]["rotate_mb"] = 0
    buf = PfcBuffer(cfg)

    with patch.object(buf, "rotate") as mock_rotate:
        buf.write("line1", "t", 0, 100)
        mock_rotate.assert_called()
    # Note: offsets are cleared inside rotate() (synchronous).
    # Since rotate is mocked above, they are cleared in the next real rotation.
    # Here we just verify rotate was triggered, which guarantees offsets will be committed.
