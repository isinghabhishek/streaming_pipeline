"""Unit tests for producer/main.py."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, call, patch

import pytest

from producer.adapters import IoTSimulatorAdapter, RestApiAdapter, WebSocketAdapter
from producer.adapters.base import RawDataPoint
from producer.main import _log, _require_env, build_adapter, run_loop
from producer.publisher import Event, KafkaPublisher


# ---------------------------------------------------------------------------
# _require_env
# ---------------------------------------------------------------------------

class TestRequireEnv:
    def test_returns_value_when_set(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "hello")
        assert _require_env("MY_VAR") == "hello"

    def test_exits_with_code_1_when_missing(self, monkeypatch):
        monkeypatch.delenv("MY_VAR", raising=False)
        with pytest.raises(SystemExit) as exc_info:
            _require_env("MY_VAR")
        assert exc_info.value.code == 1

    def test_exits_with_code_1_when_empty(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "")
        with pytest.raises(SystemExit) as exc_info:
            _require_env("MY_VAR")
        assert exc_info.value.code == 1

    def test_log_contains_variable_name(self, monkeypatch, capsys):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(SystemExit):
            _require_env("MISSING_VAR")
        captured = capsys.readouterr()
        log_line = json.loads(captured.out.strip())
        assert "MISSING_VAR" in log_line["message"]
        assert log_line["level"] == "ERROR"

    def test_structured_log_has_required_fields(self, monkeypatch, capsys):
        monkeypatch.delenv("SOME_VAR", raising=False)
        with pytest.raises(SystemExit):
            _require_env("SOME_VAR")
        captured = capsys.readouterr()
        log_line = json.loads(captured.out.strip())
        for field in ("timestamp", "component", "level", "message"):
            assert field in log_line


# ---------------------------------------------------------------------------
# build_adapter
# ---------------------------------------------------------------------------

class TestBuildAdapter:
    def test_rest_api_returns_rest_adapter(self):
        adapter = build_adapter("rest_api", "http://example.com/api")
        assert isinstance(adapter, RestApiAdapter)

    def test_iot_simulator_returns_iot_adapter(self):
        adapter = build_adapter("iot_simulator", "ignored")
        assert isinstance(adapter, IoTSimulatorAdapter)

    def test_websocket_returns_websocket_adapter(self):
        adapter = build_adapter("websocket", "ws://example.com/ws")
        assert isinstance(adapter, WebSocketAdapter)

    def test_rest_api_url_is_set(self):
        adapter = build_adapter("rest_api", "http://example.com/data")
        assert adapter.url == "http://example.com/data"

    def test_websocket_url_is_set(self):
        adapter = build_adapter("websocket", "ws://example.com/stream")
        assert adapter.url == "ws://example.com/stream"

    def test_unknown_source_type_exits_with_code_1(self):
        with pytest.raises(SystemExit) as exc_info:
            build_adapter("unknown_source", "http://example.com")
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Topic name matches source type
# ---------------------------------------------------------------------------

class TestTopicRouting:
    """The Kafka topic name must equal the SOURCE_TYPE value."""

    def _make_publisher(self):
        pub = MagicMock(spec=KafkaPublisher)
        return pub

    def _make_adapter(self, source_type: str, n: int = 1):
        adapter = MagicMock()
        adapter.fetch.return_value = [
            RawDataPoint(source=source_type, value=i, raw={"v": i})
            for i in range(n)
        ]
        return adapter

    def test_rest_api_topic(self):
        publisher = self._make_publisher()
        adapter = self._make_adapter("rest_api")

        # Run one iteration then stop via StopIteration on second fetch
        adapter.fetch.side_effect = [
            [RawDataPoint(source="rest_api", value=1, raw={"v": "1"})],
            StopIteration,
        ]

        with patch("producer.main.time.sleep"):
            with pytest.raises(StopIteration):
                run_loop(adapter, publisher, topic="rest_api", poll_interval_ms=0)

        topic_used = publisher.publish.call_args[0][0]
        assert topic_used == "rest_api"

    def test_iot_simulator_topic(self):
        publisher = self._make_publisher()
        adapter = self._make_adapter("iot_simulator")
        adapter.fetch.side_effect = [
            [RawDataPoint(source="iot_simulator", value=1, raw={"v": "1"})],
            StopIteration,
        ]

        with patch("producer.main.time.sleep"):
            with pytest.raises(StopIteration):
                run_loop(adapter, publisher, topic="iot_simulator", poll_interval_ms=0)

        topic_used = publisher.publish.call_args[0][0]
        assert topic_used == "iot_simulator"

    def test_websocket_topic(self):
        publisher = self._make_publisher()
        adapter = MagicMock()
        adapter.fetch.side_effect = [
            [RawDataPoint(source="websocket", value=1, raw={"v": "1"})],
            StopIteration,
        ]

        with patch("producer.main.time.sleep"):
            with pytest.raises(StopIteration):
                run_loop(adapter, publisher, topic="websocket", poll_interval_ms=0)

        topic_used = publisher.publish.call_args[0][0]
        assert topic_used == "websocket"


# ---------------------------------------------------------------------------
# run_loop — event construction
# ---------------------------------------------------------------------------

class TestRunLoop:
    def _run_one_iteration(self, raw: dict, source: str = "rest_api"):
        publisher = MagicMock(spec=KafkaPublisher)
        adapter = MagicMock()
        adapter.fetch.side_effect = [
            [RawDataPoint(source=source, value=1, raw=raw)],
            StopIteration,
        ]

        with patch("producer.main.time.sleep"):
            with pytest.raises(StopIteration):
                run_loop(adapter, publisher, topic=source, poll_interval_ms=0)

        return publisher.publish.call_args

    def test_publish_called_once_per_data_point(self):
        publisher = MagicMock(spec=KafkaPublisher)
        adapter = MagicMock()
        adapter.fetch.side_effect = [
            [
                RawDataPoint(source="rest_api", value=1, raw={"a": "1"}),
                RawDataPoint(source="rest_api", value=2, raw={"b": "2"}),
                RawDataPoint(source="rest_api", value=3, raw={"c": "3"}),
            ],
            StopIteration,
        ]

        with patch("producer.main.time.sleep"):
            with pytest.raises(StopIteration):
                run_loop(adapter, publisher, topic="rest_api", poll_interval_ms=0)

        assert publisher.publish.call_count == 3

    def test_payload_values_are_strings(self):
        call_args = self._run_one_iteration(raw={"sensor": "temp", "value": 22.5, "count": 3})
        event: Event = call_args[0][1]
        for v in event.payload.values():
            assert isinstance(v, str)

    def test_event_id_is_uuid(self):
        import re
        call_args = self._run_one_iteration(raw={"x": "1"})
        event: Event = call_args[0][1]
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
        assert uuid_pattern.match(event.event_id)

    def test_event_timestamp_is_iso8601_utc(self):
        from datetime import datetime, timezone
        call_args = self._run_one_iteration(raw={"x": "1"})
        event: Event = call_args[0][1]
        # Should parse without error and be UTC-aware
        dt = datetime.fromisoformat(event.timestamp)
        assert dt.tzinfo is not None

    def test_sleep_uses_poll_interval(self):
        publisher = MagicMock(spec=KafkaPublisher)
        adapter = MagicMock()
        adapter.fetch.side_effect = [
            [RawDataPoint(source="rest_api", value=1, raw={"v": "1"})],
            StopIteration,
        ]

        with patch("producer.main.time.sleep") as mock_sleep:
            with pytest.raises(StopIteration):
                run_loop(adapter, publisher, topic="rest_api", poll_interval_ms=500)

        mock_sleep.assert_called_once_with(0.5)


# ---------------------------------------------------------------------------
# Structured log format
# ---------------------------------------------------------------------------

class TestStructuredLogFormat:
    def test_log_emits_valid_json(self, capsys):
        _log("INFO", "test message")
        captured = capsys.readouterr()
        record = json.loads(captured.out.strip())
        assert record["level"] == "INFO"
        assert record["message"] == "test message"
        assert record["component"] == "Producer"
        assert "timestamp" in record

    def test_log_includes_event_id_when_provided(self, capsys):
        _log("INFO", "with event", event_id="abc-123")
        captured = capsys.readouterr()
        record = json.loads(captured.out.strip())
        assert record["event_id"] == "abc-123"

    def test_log_omits_event_id_when_not_provided(self, capsys):
        _log("INFO", "no event id")
        captured = capsys.readouterr()
        record = json.loads(captured.out.strip())
        assert "event_id" not in record

    def test_run_loop_logs_each_published_event(self, capsys):
        publisher = MagicMock(spec=KafkaPublisher)
        adapter = MagicMock()
        adapter.fetch.side_effect = [
            [
                RawDataPoint(source="rest_api", value=1, raw={"v": "1"}),
                RawDataPoint(source="rest_api", value=2, raw={"v": "2"}),
            ],
            StopIteration,
        ]

        with patch("producer.main.time.sleep"):
            with pytest.raises(StopIteration):
                run_loop(adapter, publisher, topic="rest_api", poll_interval_ms=0)

        lines = [l for l in capsys.readouterr().out.strip().splitlines() if l]
        assert len(lines) == 2
        for line in lines:
            record = json.loads(line)
            assert "event_id" in record
            assert record["level"] == "INFO"
            assert "timestamp" in record
            assert "component" in record
            assert "message" in record


# ---------------------------------------------------------------------------
# main() — missing env vars
# ---------------------------------------------------------------------------

class TestMain:
    _REQUIRED = {
        "SOURCE_TYPE": "rest_api",
        "SOURCE_URL": "http://example.com",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "SCHEMA_REGISTRY_URL": "http://localhost:8081",
    }

    def _set_env(self, monkeypatch, overrides: dict | None = None):
        env = dict(self._REQUIRED)
        if overrides:
            env.update(overrides)
        for k, v in env.items():
            if v is None:
                monkeypatch.delenv(k, raising=False)
            else:
                monkeypatch.setenv(k, v)

    @pytest.mark.parametrize("missing_var", list(_REQUIRED.keys()))
    def test_missing_required_var_exits_code_1(self, monkeypatch, missing_var):
        self._set_env(monkeypatch, {missing_var: None})
        from producer import main as main_module
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
        assert exc_info.value.code == 1

    @pytest.mark.parametrize("missing_var", list(_REQUIRED.keys()))
    def test_missing_var_log_identifies_variable(self, monkeypatch, capsys, missing_var):
        self._set_env(monkeypatch, {missing_var: None})
        from producer import main as main_module
        with pytest.raises(SystemExit):
            main_module.main()
        output = capsys.readouterr().out
        # Find the first JSON line that mentions the missing var
        for line in output.strip().splitlines():
            try:
                record = json.loads(line)
                if missing_var in record.get("message", ""):
                    assert record["level"] == "ERROR"
                    break
            except json.JSONDecodeError:
                continue
        else:
            pytest.fail(f"No log line identified missing variable '{missing_var}'")
