"""Source adapters for the producer."""
from .base import RawDataPoint, SourceAdapter
from .iot_simulator import IoTSimulatorAdapter
from .rest_api import RestApiAdapter
from .websocket import WebSocketAdapter

__all__ = [
    "RawDataPoint",
    "SourceAdapter",
    "RestApiAdapter",
    "IoTSimulatorAdapter",
    "WebSocketAdapter",
]
