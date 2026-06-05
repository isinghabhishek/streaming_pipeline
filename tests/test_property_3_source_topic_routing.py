# Feature: real-time-streaming-pipeline, Property 3: Source-to-topic routing
"""
Property 3: Source-to-topic routing

For any source type, every publish call made during one iteration of run_loop
must use exactly that source type as the topic name.

Validates: Requirements 1.6
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from producer.adapters.base import RawDataPoint
from producer.main import run_loop
from producer.publisher import KafkaPublisher


@given(st.sampled_from(["rest_api", "iot_simulator", "websocket"]))
@settings(max_examples=100)
def test_source_to_topic_routing(source_type):
    """Validates: Requirements 1.6"""
    data_points = [
        RawDataPoint(source=source_type, value=i, raw={"v": str(i)})
        for i in range(3)
    ]

    publisher = MagicMock(spec=KafkaPublisher)
    adapter = MagicMock()
    adapter.fetch.side_effect = [data_points, StopIteration]

    with patch("producer.main.time.sleep"):
        try:
            run_loop(adapter, publisher, topic=source_type, poll_interval_ms=0)
        except StopIteration:
            pass

    assert publisher.publish.call_count == len(data_points)
    for call_args in publisher.publish.call_args_list:
        topic_used = call_args[0][0]
        assert topic_used == source_type, (
            f"Expected topic '{source_type}', got '{topic_used}'"
        )
