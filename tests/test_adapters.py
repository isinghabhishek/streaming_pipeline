"""Unit tests for producer source adapters."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from producer.adapters import (
    IoTSimulatorAdapter,
    RawDataPoint,
    RestApiAdapter,
    WebSocketAdapter,
)
from producer.adapters.base import SourceAdapter


# ---------------------------------------------------------------------------
# RawDataPoint
# ---------------------------------------------------------------------------

class TestRawDataPoint:
    def test_fields(self):
        dp = RawDataPoint(source="test", value=42, raw={"x": 1})
        assert dp.source == "test"
        assert dp.value == 42
        assert dp.raw == {"x": 1}

    def test_default_raw(self):
        dp = RawDataPoint(source="s", value=1)
        assert dp.raw == {}


# ---------------------------------------------------------------------------
# SourceAdapter protocol
# ---------------------------------------------------------------------------

class TestSourceAdapterProtocol:
    def test_protocol_satisfied_by_adapters(self):
        """All three adapters satisfy the SourceAdapter protocol."""
        for cls in (IoTSimulatorAdapter, RestApiAdapter, WebSocketAdapter):
            assert hasattr(cls, "fetch")


# ---------------------------------------------------------------------------
# IoTSimulatorAdapter
# ---------------------------------------------------------------------------

class TestIoTSimulatorAdapter:
    def test_returns_list_of_raw_data_points(self):
        adapter = IoTSimulatorAdapter(num_sensors=3)
        result = adapter.fetch()
        assert isinstance(result, list)
        assert len(result) == 3
        for dp in result:
            assert isinstance(dp, RawDataPoint)

    def test_source_field(self):
        adapter = IoTSimulatorAdapter(num_sensors=2)
        for dp in adapter.fetch():
            assert dp.source == "iot_simulator"

    def test_raw_contains_sensor_and_value(self):
        adapter = IoTSimulatorAdapter(num_sensors=5)
        for dp in adapter.fetch():
            assert "sensor" in dp.raw
            assert "value" in dp.raw
            assert "timestamp" in dp.raw

    def test_value_within_sensor_range(self):
        ranges = {"temperature": (15.0, 35.0), "humidity": (20.0, 90.0), "pressure": (950.0, 1050.0)}
        adapter = IoTSimulatorAdapter(num_sensors=30)
        for dp in adapter.fetch():
            sensor = dp.raw["sensor"]
            lo, hi = ranges[sensor]
            assert lo <= dp.value <= hi

    def test_configurable_num_sensors(self):
        for n in (1, 5, 10):
            assert len(IoTSimulatorAdapter(num_sensors=n).fetch()) == n


# ---------------------------------------------------------------------------
# RestApiAdapter
# ---------------------------------------------------------------------------

class TestRestApiAdapter:
    def test_fetch_list_response(self):
        adapter = RestApiAdapter(url="http://example.com/api")
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"id": 1}, {"id": 2}]
        mock_resp.raise_for_status.return_value = None

        with patch("producer.adapters.rest_api.requests.get", return_value=mock_resp):
            result = adapter.fetch()

        assert len(result) == 2
        assert all(dp.source == "rest_api" for dp in result)

    def test_fetch_dict_response(self):
        adapter = RestApiAdapter(url="http://example.com/api")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"price": 100}
        mock_resp.raise_for_status.return_value = None

        with patch("producer.adapters.rest_api.requests.get", return_value=mock_resp):
            result = adapter.fetch()

        assert len(result) == 1
        assert result[0].raw == {"price": 100}

    def test_backoff_resets_on_success(self):
        adapter = RestApiAdapter(url="http://example.com/api")
        adapter._backoff = 32  # simulate previous failures

        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None

        with patch("producer.adapters.rest_api.requests.get", return_value=mock_resp):
            adapter.fetch()

        assert adapter._backoff == 1  # reset to start

    def test_exponential_backoff_on_connection_error(self):
        """After a connectivity failure, backoff doubles (up to cap)."""
        import requests as req

        adapter = RestApiAdapter(url="http://example.com/api")
        assert adapter._backoff == 1

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

        mock_sleep.assert_called_once_with(1)
        assert adapter._backoff == 1  # reset after success

    def test_backoff_cap(self):
        """Backoff never exceeds 60 s."""
        import requests as req

        adapter = RestApiAdapter(url="http://example.com/api")
        adapter._backoff = 32

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
            with patch("producer.adapters.rest_api.time.sleep"):
                adapter.fetch()

        # After one failure from 32, next would be 64 but capped at 60
        # Then reset to 1 on success
        assert adapter._backoff == 1

    def test_structured_error_log_on_failure(self, caplog):
        """Connectivity failures emit structured JSON error logs."""
        import logging
        import requests as req

        adapter = RestApiAdapter(url="http://example.com/api")

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
            with patch("producer.adapters.rest_api.time.sleep"):
                with caplog.at_level(logging.ERROR, logger="producer.adapters.rest_api"):
                    adapter.fetch()

        assert len(caplog.records) == 1
        log_data = json.loads(caplog.records[0].message)
        assert log_data["level"] == "ERROR"
        assert log_data["component"] == "RestApiAdapter"
        assert "timestamp" in log_data
        assert "message" in log_data


# ---------------------------------------------------------------------------
# WebSocketAdapter
# ---------------------------------------------------------------------------

class TestWebSocketAdapter:
    def test_parse_json_messages(self):
        adapter = WebSocketAdapter(url="ws://example.com/ws")
        messages = ['{"sensor": "temp", "value": 22.5}', '{"sensor": "hum", "value": 55}']
        result = adapter._parse(messages)
        assert len(result) == 2
        assert all(dp.source == "websocket" for dp in result)
        assert result[0].raw == {"sensor": "temp", "value": 22.5}

    def test_parse_plain_string_messages(self):
        adapter = WebSocketAdapter(url="ws://example.com/ws")
        result = adapter._parse(["hello"])
        assert result[0].raw == {"value": "hello"}

    def test_backoff_on_connection_failure(self):
        adapter = WebSocketAdapter(url="ws://example.com/ws")
        assert adapter._backoff == 1

        call_count = 0

        async def mock_collect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("connection refused")
            return ['{"v": 1}']

        with patch.object(adapter, "_collect", side_effect=mock_collect):
            with patch("producer.adapters.websocket.time.sleep") as mock_sleep:
                result = adapter.fetch()

        mock_sleep.assert_called_once_with(1)
        assert len(result) == 1

    def test_backoff_resets_on_success(self):
        adapter = WebSocketAdapter(url="ws://example.com/ws")
        adapter._backoff = 16

        async def mock_collect():
            return ['{"v": 1}']

        with patch.object(adapter, "_collect", side_effect=mock_collect):
            adapter.fetch()

        assert adapter._backoff == 1

    def test_structured_error_log_on_failure(self, caplog):
        import logging

        adapter = WebSocketAdapter(url="ws://example.com/ws")
        call_count = 0

        async def mock_collect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("refused")
            return []

        with patch.object(adapter, "_collect", side_effect=mock_collect):
            with patch("producer.adapters.websocket.time.sleep"):
                with caplog.at_level(logging.ERROR, logger="producer.adapters.websocket"):
                    adapter.fetch()

        assert len(caplog.records) == 1
        log_data = json.loads(caplog.records[0].message)
        assert log_data["level"] == "ERROR"
        assert log_data["component"] == "WebSocketAdapter"
        assert "timestamp" in log_data
