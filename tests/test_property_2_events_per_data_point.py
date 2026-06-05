# Feature: real-time-streaming-pipeline, Property 2: One event published per data point
"""
Property 2: One event published per data point

For any list of N data points returned by the source adapter, exactly N
publish calls must be made after one iteration of run_loop.

Validates: Requirements 1.3
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from producer.adapters.base import RawDataPoint
from producer.main import run_loop
from producer.publisher import KafkaPublisher

# Strategy: generate 1–100 data points with string-valued raw dicts
_data_point_st = st.fixed_dictionaries({
    "key": st.text(min_size=1, max_size=20),
    "value": st.text(max_size=50),
})


@given(st.lists(_data_point_st, min_size=1, max_size=100))
@settings(max_examples=100)
def test_one_event_published_per_data_point(data_dicts):
    """Validates: Requirements 1.3"""
    # Build RawDataPoint list from generated dicts
    data_points = [
        RawDataPoint(source="rest_api", value=d["value"], raw=d)
        for d in data_dicts
    ]

    publisher = MagicMock(spec=KafkaPublisher)
    adapter = MagicMock()
    # First fetch returns the generated data points; second raises StopIteration
    adapter.fetch.side_effect = [data_points, StopIteration]

    with patch("producer.main.time.sleep"):
        try:
            run_loop(adapter, publisher, topic="rest_api", poll_interval_ms=0)
        except StopIteration:
            pass

    assert publisher.publish.call_count == len(data_points), (
        f"Expected {len(data_points)} publish calls, got {publisher.publish.call_count}"
    )
