"""
Unit test ba funsaun prinsipál pipeline (la presiza Kafka/Spark halai).
Halai:  pytest -v
"""
import os
os.environ["PG_PORT"] = os.getenv("PG_PORT","5433")
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.wifi_producer import ACCESS_POINTS, DILI_TZ, EVENT_TYPES, WifiSimulator  # noqa: E402
from consumer.raw_consumer import validate  # noqa: E402

REQUIRED = {"event_id", "event_time", "event_type", "ap_id", "building", "user_id",
            "rssi_dbm", "bytes_up", "bytes_down", "latency_ms", "packet_loss_pct"}


def _events(n=2000, **kw):
    sim = WifiSimulator(seed=1, load_override=1.0, **kw)
    ts = datetime(2026, 9, 28, 10, 0, tzinfo=DILI_TZ)
    return [sim.next_event(ts) for _ in range(n)]


def test_total_access_points():
    assert len(ACCESS_POINTS) == 36          # hanesan ho schema.sql


def test_generated_events_have_schema_and_are_valid():
    for e in _events():
        assert REQUIRED <= e.keys()
        assert e["event_type"] in EVENT_TYPES
        _, reasons = validate(e)
        assert reasons == [], reasons


def test_event_ids_unique():
    ids = [e["event_id"] for e in _events()]
    assert len(ids) == len(set(ids))


def test_validate_detects_bad_records():
    good = _events(1)[0]
    cases = {
        "missing_ap_id": {**good, "ap_id": None},
        "rssi_out_of_range": {**good, "rssi_dbm": 15},
        "negative_or_missing_bytes": {**good, "bytes_down": -10},
        "invalid_timestamp": {**good, "event_time": "31/13/2026 25:61"},
        "invalid_event_type": {**good, "event_type": "HACK"},
    }
    for expected, evt in cases.items():
        _, reasons = validate(evt)
        assert expected in reasons, (expected, reasons)


def test_incident_ap_degrades_signal():
    ap = "LAB-F2-AP03"
    ev = [e for e in _events(5000, incident_ap=ap) if e["ap_id"] == ap]
    other = [e for e in _events(5000) if e["ap_id"] == ap]
    avg = lambda xs, k: sum(x[k] for x in xs) / len(xs)  # noqa: E731
    assert avg(ev, "latency_ms") > 2 * avg(other, "latency_ms")
