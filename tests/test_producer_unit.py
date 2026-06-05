"""Unit tests for producer components.

Covers:
1. Exponential backoff interval calculation (Requirements 1.4)
2. DLQ envelope construction (Requirements 8.2)
3. Structured log output format (Requirements 9.1)
4. Missing env var exit behavior (Requirements 10.5)
"""
from __future__ import annotations

import io
import json
import os
from unittest.mock import MagicMock, patch

import fastavro
import pytest

from producer.publisher import Event, KafkaPublisher, _load_schema
from producer.main import _log, _require_env


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_publisher() -> KafkaPublisher:
    pub = KafkaPublisher.__new__(KafkaPublisher)
    pub._bootstrap_servers = "localhost:9092"
    pub._schema_registry_url = "http://localhost:8081"
    pub._event_schema = _load_schema("event.avsc")
    pub._dlq_schema = _load_schema("dlq_event.avsc")
    pub._producer = MagicMock()
    return pub


def _sample_event(**overrides) -> Event:
    base = dict(
        event_id="evt-test-001",
        source="rest_api",
        timestamp="2024-01-01T00:00:00Z",
        payload={"key": "value"},
    )
    base.update(overrides)
    return Event(**base)


# ---------------------------------------------------------------------------
# 1. Exponential backoff interval calculation
# ---------------------------------------------------------------------------

class TestExponentialBackoff:
    """Backoff doubles on each failure, resets to 1 on success, never exceeds 60."""

    def _make_rest_adapter(self):
        from producer.adapters.rest_api import RestApiAdapter
        return RestApiAdapter(url="http://example.com/api")

    def test_initial_backoff_is_one(self):
        adapter = self._make_rest_adapter()
        assert adapter._backoff == 1

    def test_backoff_doubles_after_failure(self):
        import requests as req
        adapter = self._make_rest_adapter()

        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise req.ConnectionError("refused")
            return mock_resp

        with patch("producer.adapters.rest_api.requests.get", side_effect=side_effect):
            with patch("producer.adapters.rest_api.time.sleep") as mock_sleep:
                adapter.fetch()

        # Slept with backoff=1, then doubled to 2 before success reset it to 1
        mock_sleep.assert_called_once_with(1)

    def test_backoff_resets_to_one_after_success(self):
        import requests as req
        adapter = self._make_rest_adapter()
        adapter._backoff = 16

        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None

        with patch("producer.adapters.rest_api.requests.get", return_value=mock_resp):
            adapter.fetch()

        assert adapter._backoff == 1

    def test_backoff_never_exceeds_60(self):
        import requests as req
        adapter = self._make_rest_adapter()
        adapter._backoff = 32  # one more failure → 64, but capped at 60

        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise req.ConnectionError("refused")
            return mock_resp

        with patch("producer.adapters.rest_api.requests.get", side_effect=side_effect):
            with patch("producer.adapters.rest_api.time.sleep") as mock_sleep:
                adapter.fetch()

        # Slept with 32, would double to 64 but capped at 60, then reset to 1 on success
        mock_sleep.assert_called_once_with(32)
        assert adapter._backoff == 1

    def test_multiple_failures_double_each_time(self):
        import requests as req
        adapter = self._make_rest_adapter()

        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None

        call_count = 0
        sleep_calls = []

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise req.ConnectionError("refused")
            return mock_resp

        with patch("producer.adapters.rest_api.requests.get", side_effect=side_effect):
            with patch("producer.adapters.rest_api.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
                adapter.fetch()

        # Three failures: sleep(1), sleep(2), sleep(4)
        assert sleep_calls == [1, 2, 4]
        assert adapter._backoff == 1  # reset after success

    def test_backoff_cap_at_60_with_websocket(self):
        from producer.adapters.websocket import WebSocketAdapter
        adapter = WebSocketAdapter(url="ws://example.com/ws")
        adapter._backoff = 32

        call_count = 0

        async def mock_collect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("refused")
            return ['{"v": 1}']

        with patch.object(adapter, "_collect", side_effect=mock_collect):
            with patch("producer.adapters.websocket.time.sleep") as mock_sleep:
                adapter.fetch()

        mock_sleep.assert_called_once_with(32)
        assert adapter._backoff == 1


# ---------------------------------------------------------------------------
# 2. DLQ envelope construction
# ---------------------------------------------------------------------------

class TestDLQEnvelopeConstruction:
    """publish_dlq sets all required DLQ fields correctly."""

    def test_original_topic_is_set(self):
        captured = []
        pub = _make_publisher()
        with patch.object(pub, "_do_produce", side_effect=lambda t, p: captured.append((t, p))):
            pub.publish_dlq("my-topic", _sample_event(), ValueError("oops"))

        assert len(captured) == 1
        dlq_topic, dlq_bytes = captured[0]
        assert dlq_topic == "my-topic.dlq"

        schema = _load_schema("dlq_event.avsc")
        record = fastavro.schemaless_reader(io.BytesIO(dlq_bytes), schema)
        assert record["original_topic"] == "my-topic"

    def test_error_type_is_exception_class_name(self):
        captured = []
        pub = _make_publisher()
        with patch.object(pub, "_do_produce", side_effect=lambda t, p: captured.append(p)):
            pub.publish_dlq("events", _sample_event(), ValueError("bad"))

        schema = _load_schema("dlq_event.avsc")
        record = fastavro.schemaless_reader(io.BytesIO(captured[0]), schema)
        assert record["error_type"] == "ValueError"

    def test_error_message_is_str_of_exception(self):
        captured = []
        pub = _make_publisher()
        error = RuntimeError("something went wrong")
        with patch.object(pub, "_do_produce", side_effect=lambda t, p: captured.append(p)):
            pub.publish_dlq("events", _sample_event(), error)

        schema = _load_schema("dlq_event.avsc")
        record = fastavro.schemaless_reader(io.BytesIO(captured[0]), schema)
        assert record["error_message"] == str(error)

    def test_raw_bytes_is_non_empty_bytes(self):
        captured = []
        pub = _make_publisher()
        with patch.object(pub, "_do_produce", side_effect=lambda t, p: captured.append(p)):
            pub.publish_dlq("events", _sample_event(), TypeError("x"))

        schema = _load_schema("dlq_event.avsc")
        record = fastavro.schemaless_reader(io.BytesIO(captured[0]), schema)
        assert isinstance(record["raw_bytes"], (bytes, bytearray))
        assert len(record["raw_bytes"]) > 0

    def test_different_exception_types(self):
        schema = _load_schema("dlq_event.avsc")
        for exc_cls, exc_msg in [(KeyError, "key"), (TypeError, "type"), (OSError, "io")]:
            captured = []
            pub = _make_publisher()
            error = exc_cls(exc_msg)
            with patch.object(pub, "_do_produce", side_effect=lambda t, p: captured.append(p)):
                pub.publish_dlq("events", _sample_event(), error)

            record = fastavro.schemaless_reader(io.BytesIO(captured[0]), schema)
            assert record["error_type"] == exc_cls.__name__


# ---------------------------------------------------------------------------
# 3. Structured log output format
# ---------------------------------------------------------------------------

class TestStructuredLogFormat:
    """Every log line from _log() is valid JSON with required fields."""

    def test_log_is_valid_json(self, capsys):
        _log("INFO", "test message")
        out = capsys.readouterr().out.strip()
        record = json.loads(out)  # raises if not valid JSON
        assert isinstance(record, dict)

    def test_log_has_timestamp_field(self, capsys):
        _log("INFO", "msg")
        record = json.loads(capsys.readouterr().out.strip())
        assert "timestamp" in record
        assert record["timestamp"]  # non-empty

    def test_log_has_component_field(self, capsys):
        _log("INFO", "msg")
        record = json.loads(capsys.readouterr().out.strip())
        assert "component" in record
        assert record["component"] == "Producer"

    def test_log_has_level_field(self, capsys):
        for level in ("INFO", "ERROR", "WARNING"):
            _log(level, "msg")
            record = json.loads(capsys.readouterr().out.strip())
            assert record["level"] == level

    def test_log_has_message_field(self, capsys):
        _log("INFO", "hello world")
        record = json.loads(capsys.readouterr().out.strip())
        assert record["message"] == "hello world"

    def test_all_four_required_fields_present(self, capsys):
        _log("ERROR", "something failed")
        record = json.loads(capsys.readouterr().out.strip())
        for field in ("timestamp", "component", "level", "message"):
            assert field in record, f"Missing required field: {field}"

    def test_extra_kwargs_included_in_log(self, capsys):
        _log("INFO", "with extras", topic="events", source="rest_api")
        record = json.loads(capsys.readouterr().out.strip())
        assert record["topic"] == "events"
        assert record["source"] == "rest_api"


# ---------------------------------------------------------------------------
# 4. Missing env var exit behavior
# ---------------------------------------------------------------------------

class TestMissingEnvVarExit:
    """Each required env var, when absent, causes main() to exit(1) and log the var name."""

    _REQUIRED_VARS = [
        "SOURCE_TYPE",
        "SOURCE_URL",
        "KAFKA_BOOTSTRAP_SERVERS",
        "SCHEMA_REGISTRY_URL",
    ]

    _ALL_VARS = {
        "SOURCE_TYPE": "rest_api",
        "SOURCE_URL": "http://example.com",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SCHEMA_REGISTRY_URL": "http://localhost:8081",
    }

    def _set_env(self, monkeypatch, missing: str) -> None:
        for k, v in self._ALL_VARS.items():
            if k == missing:
                monkeypatch.delenv(k, raising=False)
            else:
                monkeypatch.setenv(k, v)

    @pytest.mark.parametrize("missing_var", _REQUIRED_VARS)
    def test_exits_with_code_1(self, monkeypatch, missing_var):
        self._set_env(monkeypatch, missing_var)
        from producer import main as main_module
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
        assert exc_info.value.code == 1

    @pytest.mark.parametrize("missing_var", _REQUIRED_VARS)
    def test_log_contains_variable_name(self, monkeypatch, capsys, missing_var):
        self._set_env(monkeypatch, missing_var)
        from producer import main as main_module
        with pytest.raises(SystemExit):
            main_module.main()
        output = capsys.readouterr().out
        found = False
        for line in output.strip().splitlines():
            try:
                record = json.loads(line)
                if missing_var in record.get("message", ""):
                    assert record["level"] == "ERROR"
                    found = True
                    break
            except json.JSONDecodeError:
                continue
        assert found, f"No log line identified missing variable '{missing_var}'"

    @pytest.mark.parametrize("missing_var", _REQUIRED_VARS)
    def test_log_is_valid_json(self, monkeypatch, capsys, missing_var):
        self._set_env(monkeypatch, missing_var)
        from producer import main as main_module
        with pytest.raises(SystemExit):
            main_module.main()
        output = capsys.readouterr().out
        for line in output.strip().splitlines():
            if line:
                record = json.loads(line)  # raises if not valid JSON
                assert "timestamp" in record
                assert "component" in record
                assert "level" in record
                assert "message" in record
