"""IoT simulator source adapter — generates synthetic sensor readings."""
from __future__ import annotations

import random
from datetime import datetime, timezone

from .base import RawDataPoint

_SENSORS = ["temperature", "humidity", "pressure"]
_RANGES = {
    "temperature": (15.0, 35.0),
    "humidity": (20.0, 90.0),
    "pressure": (950.0, 1050.0),
}


class IoTSimulatorAdapter:
    """Generates synthetic IoT sensor readings.

    No external connectivity — no backoff needed.
    Each call to fetch() returns one reading per sensor type.
    """

    def __init__(self, num_sensors: int = 3) -> None:
        self.num_sensors = num_sensors

    def fetch(self) -> list[RawDataPoint]:
        """Generate synthetic sensor readings."""
        now = datetime.now(timezone.utc).isoformat()
        points: list[RawDataPoint] = []
        for _ in range(self.num_sensors):
            sensor = random.choice(_SENSORS)
            lo, hi = _RANGES[sensor]
            reading = round(random.uniform(lo, hi), 2)
            raw = {"sensor": sensor, "value": reading, "timestamp": now}
            points.append(RawDataPoint(source="iot_simulator", value=reading, raw=raw))
        return points
