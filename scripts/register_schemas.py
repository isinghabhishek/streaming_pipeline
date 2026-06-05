#!/usr/bin/env python3
"""
Register Avro schemas in the Confluent Schema Registry.

Reads schema files from schemas/, POSTs them to the Schema Registry REST API,
and verifies backward compatibility on re-registration.

Exit codes:
  0 — all schemas registered successfully
  1 — one or more registrations failed

Environment variables:
  SCHEMA_REGISTRY_URL  default: http://localhost:8081
  EVENTS_TOPIC         default: events
  DLQ_TOPIC            default: events.dlq
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
EVENTS_TOPIC = os.environ.get("EVENTS_TOPIC", "events")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "events.dlq")

COMPONENT = "register_schemas"

# Resolve schema paths relative to this script's location
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_SCHEMAS_DIR = os.path.join(_SCRIPT_DIR, "..", "schemas")


def _log(level: str, message: str) -> None:
    """Emit a structured JSON log line to stdout."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "component": COMPONENT,
        "level": level,
        "message": message,
    }
    print(json.dumps(record), flush=True)


def _schemas() -> list[dict]:
    return [
        {
            "subject": f"{EVENTS_TOPIC}-value",
            "schema_file": os.path.join(_SCHEMAS_DIR, "event.avsc"),
        },
        {
            "subject": f"{DLQ_TOPIC}-value",
            "schema_file": os.path.join(_SCHEMAS_DIR, "dlq_event.avsc"),
        },
    ]


def wait_for_registry(url: str, retries: int = 10, delay: float = 3.0) -> bool:
    """Poll the Schema Registry until it responds or retries are exhausted."""
    try:
        import requests
    except ImportError as exc:
        _log("ERROR", f"Missing dependency: {exc}")
        return False

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(f"{url}/subjects", timeout=5)
            if resp.status_code == 200:
                _log("INFO", f"Schema Registry is ready at {url}")
                return True
        except requests.exceptions.ConnectionError:
            pass
        _log("INFO", f"Waiting for Schema Registry (attempt {attempt}/{retries})")
        time.sleep(delay)
    return False


def check_compatibility(url: str, subject: str, schema_str: str) -> bool:
    """
    Check backward compatibility of the schema against the latest registered version.
    Returns True if compatible or if no previous version exists.
    """
    import requests

    endpoint = f"{url}/compatibility/subjects/{subject}/versions/latest"
    payload = {"schema": schema_str}
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        if resp.status_code == 404:
            # No existing version — first registration, always compatible
            return True
        if resp.status_code == 200:
            is_compatible = resp.json().get("is_compatible", False)
            if not is_compatible:
                _log("WARN", f"Schema for subject '{subject}' is NOT backward compatible")
            return is_compatible
        _log("ERROR", f"Compatibility check for '{subject}' returned HTTP {resp.status_code}: {resp.text}")
        return False
    except requests.exceptions.RequestException as exc:
        _log("ERROR", f"Compatibility check request failed for '{subject}': {exc}")
        return False


def register_schema(url: str, subject: str, schema_str: str) -> bool:
    """POST the schema to the Schema Registry. Returns True on success."""
    import requests

    endpoint = f"{url}/subjects/{subject}/versions"
    payload = {"schema": schema_str}
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        if resp.status_code in (200, 201):
            schema_id = resp.json().get("id")
            _log("INFO", f"Registered subject '{subject}' with schema id={schema_id}")
            return True
        _log("ERROR", f"Failed to register '{subject}' — HTTP {resp.status_code}: {resp.text}")
        return False
    except requests.exceptions.RequestException as exc:
        _log("ERROR", f"Registration request failed for '{subject}': {exc}")
        return False


def main() -> int:
    if not wait_for_registry(SCHEMA_REGISTRY_URL):
        _log("ERROR", "Schema Registry did not become ready in time — aborting")
        return 1

    all_ok = True
    for entry in _schemas():
        subject = entry["subject"]
        schema_file = entry["schema_file"]

        # Load and parse schema file
        try:
            with open(schema_file, "r") as fh:
                schema_obj = json.load(fh)
            schema_str = json.dumps(schema_obj)
        except (OSError, json.JSONDecodeError) as exc:
            _log("ERROR", f"Could not read schema file '{schema_file}': {exc}")
            all_ok = False
            continue

        # Check backward compatibility when a previous version already exists
        compatible = check_compatibility(SCHEMA_REGISTRY_URL, subject, schema_str)
        if not compatible:
            _log("ERROR", f"Skipping registration of '{subject}' due to incompatible schema")
            all_ok = False
            continue

        # Register (or confirm already registered)
        ok = register_schema(SCHEMA_REGISTRY_URL, subject, schema_str)
        if not ok:
            all_ok = False

    if all_ok:
        _log("INFO", "All schemas registered successfully")
    else:
        _log("ERROR", "One or more schema registrations failed")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
