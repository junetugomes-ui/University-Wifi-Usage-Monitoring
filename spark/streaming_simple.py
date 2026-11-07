"""
streaming_simple.py - Kafka -> PostgreSQL (live_metrics) - FINAL FIX
Fix: Python 3.14 + kafka-python-ng zstd bug
"""
import json
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

FLUSH_EVERY = 5
KEEP_MINUTES = 3

conn = psycopg2.connect(
    "dbname=wifi_monitoring user=wifi password=wifi123 host=localhost port=5433"
)
cur = conn.cursor()

# Garanti tabela iha building NOT NULL lolos
cur.execute("""
CREATE TABLE IF NOT EXISTS live_metrics (
  window_start TIMESTAMPTZ NOT NULL,
  ap_id TEXT NOT NULL,
  building TEXT NOT NULL,
  user_count INT,
  event_count INT,
  avg_rssi FLOAT,
  avg_latency FLOAT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (ap_id, window_start)
)""")
conn.commit()

# --- FIX KRITIKU BA KAFKA ---
consumer = KafkaConsumer(
    "wifi-logs",
    bootstrap_servers="localhost:9092",
    api_version=(0, 10, 2), # evita bug zstd
    group_id="dit-wifi-group-final-09-10", # NARAN FOUN! sei ignora dadus tuan aat
    auto_offset_reset="latest", # lee deit foun
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    enable_auto_commit=True,
)

UPSERT = """
INSERT INTO live_metrics (window_start, ap_id, building, user_count, event_count, avg_rssi, avg_latency, updated_at)
VALUES %s
ON CONFLICT (ap_id, window_start) DO UPDATE SET
  building = EXCLUDED.building,
  user_count = EXCLUDED.user_count,
  event_count = EXCLUDED.event_count,
  avg_rssi = EXCLUDED.avg_rssi,
  avg_latency = EXCLUDED.avg_latency,
  updated_at = now()
"""

windows = {}

def flush():
    if not windows:
        return
    rows = [
        (minute, ap, w["building"], len(w["users"]), w["n"],
         w["rssi"] / w["n"], w["lat"] / w["n"])
        for (ap, minute), w in windows.items()
    ]
    execute_values(cur, UPSERT, rows, template="(%s,%s,%s,%s,%s,%s,%s,now())")
    conn.commit()
    print(f"[flush] {len(rows)} linha | user total = {sum(r[3] for r in rows)}")
    newest = max(m for (_, m) in windows)
    for key in [k for k in windows if (newest - k[1]).total_seconds() > KEEP_MINUTES * 60]:
        del windows[key]

print("Simple Streaming started... waiting data (group final-09-10)")
last_flush = time.time()

while True:
    polled = consumer.poll(timeout_ms=1000, max_records=1000)
    for records in polled.values():
        for msg in records:
            d = msg.value
            ap, uid = d.get("ap_id"), d.get("user_id")
            rssi, lat = d.get("rssi_dbm"), d.get("latency_ms")
            if not ap or not uid or rssi is None or lat is None:
                continue
            if d.get("event_type") == "AUTH_FAIL" or not (-100 <= rssi <= -20):
                continue
            try:
                ts = datetime.fromisoformat(d["event_time"])
            except Exception:
                continue
            minute = ts.replace(second=0, microsecond=0)
            w = windows.setdefault(
                (ap, minute),
                {"building": d.get("building") or ap.split("-")[0],
                 "users": set(), "n": 0, "rssi": 0.0, "lat": 0.0},
            )
            w["users"].add(uid)
            w["n"] += 1
            w["rssi"] += rssi
            w["lat"] += lat

    if time.time() - last_flush >= FLUSH_EVERY:
        flush()
        last_flush = time.time()