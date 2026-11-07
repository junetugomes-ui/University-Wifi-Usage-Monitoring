#!/usr/bin/env python3
"""
=====================================================================
 raw_consumer.py — KAFKA CONSUMER (Bronze layer / Raw archive)
=====================================================================
 Knaar:
   1. Simu mensajen husi topic Kafka 'wifi-logs' (consumer group)
   2. VALIDA kada record (data quality): timestamp, RSSI, bytes, AP...
   3. Rai HOTU (válidu + invalidu ho razaun) iha tabela raw_events
      - event_id = PRIMARY KEY → duplikadu rejeita automátiku
   4. Commit offset Kafka DEPOIS de rai iha DB (at-least-once +
      idempotent insert = efetivamente exactly-once)
   5. Hakerek métrika kualidade iha dq_ingest_log

 Halai:  python -m consumer.raw_consumer
 Bele halai instánsia barak (group hanesan) → Kafka fahe partisaun
 (hatudu scalability horizontal).
=====================================================================
"""
import json
import os
import signal
import time
from datetime import datetime, timezone

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wifi-logs")
GROUP_ID = os.getenv("KAFKA_GROUP", "wifi-raw-archiver")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SEC = float(os.getenv("FLUSH_SEC", "2"))
PG = dict(host=os.getenv("PG_HOST", "localhost"), port=int(os.getenv("PG_PORT", "5433")),
          dbname=os.getenv("PG_DB", "wifi_monitoring"), user=os.getenv("PG_USER", "wifi"),
          password=os.getenv("PG_PASSWORD", "wifi123"))

VALID_EVENT_TYPES = {"CONNECT", "USAGE", "DISCONNECT", "AUTH_FAIL"}
COLUMNS = ["event_id", "event_time", "event_type", "ap_id", "building", "floor", "ssid", "band",
           "channel", "user_id", "user_role", "device_type", "rssi_dbm", "bytes_up", "bytes_down",
           "session_duration_s", "latency_ms", "packet_loss_pct", "is_valid", "dq_reason",
           "kafka_partition", "kafka_offset"]

INSERT_SQL = f"""
INSERT INTO raw_events ({', '.join(COLUMNS)})
VALUES %s
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""


def _to_int(v):
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------
# REGRA DATA QUALITY — funsaun puru (fasil atu unit test)
# ---------------------------------------------------------------------
def validate(evt: dict):
    """Fila fali (event_time_parsed, lista_razaun). Lista mamuk = válidu."""
    reasons = []
    ts = None
    try:
        ts = datetime.fromisoformat(str(evt.get("event_time")))
        if ts.tzinfo is None:
            reasons.append("timestamp_no_timezone")
    except ValueError:
        reasons.append("invalid_timestamp")

    if not evt.get("ap_id"):
        reasons.append("missing_ap_id")
    if not evt.get("user_id"):
        reasons.append("missing_user_id")
    if evt.get("event_type") not in VALID_EVENT_TYPES:
        reasons.append("invalid_event_type")

    rssi = _to_int(evt.get("rssi_dbm"))
    if rssi is None or not -100 <= rssi <= -20:          # limite fíziku Wi-Fi
        reasons.append("rssi_out_of_range")

    up, down = _to_int(evt.get("bytes_up")), _to_int(evt.get("bytes_down"))
    if up is None or down is None or up < 0 or down < 0:
        reasons.append("negative_or_missing_bytes")

    lat = _to_float(evt.get("latency_ms"))
    if lat is None or lat < 0:
        reasons.append("invalid_latency")
    loss = _to_float(evt.get("packet_loss_pct"))
    if loss is None or not 0 <= loss <= 100:
        reasons.append("invalid_packet_loss")
    return ts, reasons


def to_row(evt, ts, reasons, partition, offset):
    return (
        str(evt.get("event_id")), ts, evt.get("event_type"), evt.get("ap_id"), evt.get("building"),
        _to_int(evt.get("floor")), evt.get("ssid"), evt.get("band"), _to_int(evt.get("channel")),
        evt.get("user_id"), evt.get("user_role"), evt.get("device_type"),
        _to_int(evt.get("rssi_dbm")), _to_int(evt.get("bytes_up")), _to_int(evt.get("bytes_down")),
        _to_int(evt.get("session_duration_s")), _to_float(evt.get("latency_ms")),
        _to_float(evt.get("packet_loss_pct")),
        len(reasons) == 0, ";".join(reasons) or None, partition, offset,
    )


def main():
    import psycopg2
    from psycopg2.extras import execute_values
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",   # dala uluk: lee hahú husi mensajen primeiru
        "enable.auto.commit": False,       # commit manual depois DB rai ona
    })
    consumer.subscribe([TOPIC])
    conn = psycopg2.connect(**PG)
    print(f"[consumer] Rona topic '{TOPIC}' (group={GROUP_ID}) → PostgreSQL {PG['host']}/{PG['dbname']}")

    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    buffer = []                 # lista (row, ts)
    totals = {"received": 0, "inserted": 0, "invalid": 0, "duplicates": 0, "unparseable": 0}
    last_flush = time.time()

    def flush() -> bool:
        nonlocal conn
        rows = [r for r, _ in buffer]
        now = datetime.now(timezone.utc)
        lags = [(now - ts).total_seconds() * 1000 for (r, ts) in buffer if r[18] and ts]
        try:
            with conn.cursor() as cur:
                inserted = execute_values(cur, INSERT_SQL, rows, page_size=1000, fetch=True)
                n_ins = len(inserted)
                n_inv = sum(1 for r in rows if not r[18])
                n_dup = len(rows) - n_ins
                avg_lag = sum(lags) / len(lags) if lags else None
                cur.execute(
                    "INSERT INTO dq_ingest_log (received, inserted, invalid, duplicates, batch_latency_ms)"
                    " VALUES (%s,%s,%s,%s,%s)", (len(rows), n_ins, n_inv, n_dup, avg_lag))
            conn.commit()
        except psycopg2.Error as exc:
            print(f"[consumer] ERRU DB: {exc}. Koko fali...")
            try:
                conn.rollback()
            except psycopg2.Error:
                conn = psycopg2.connect(**PG)
            time.sleep(2)
            return False                         # la commit offset → Kafka sei fó fali
        totals["received"] += len(rows)
        totals["inserted"] += n_ins
        totals["invalid"] += n_inv
        totals["duplicates"] += n_dup
        lag_txt = f"{avg_lag:,.0f} ms" if avg_lag is not None else "-"
        print(f"  batch={len(rows):4d}  rai={n_ins:4d}  invalidu={n_inv:3d}  duplikadu={n_dup:2d}  "
              f"lag={lag_txt}  | total rai={totals['inserted']:,}")
        return True

    try:
        while running:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[consumer] Kafka error: {m.error()}")
                    continue
                try:
                    evt = json.loads(m.value())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    totals["unparseable"] += 1
                    continue
                if not evt.get("event_id"):
                    totals["unparseable"] += 1
                    continue
                ts, reasons = validate(evt)
                buffer.append((to_row(evt, ts, reasons, m.partition(), m.offset()), ts))

            if buffer and (len(buffer) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_SEC):
                if flush():
                    consumer.commit(asynchronous=False)   # offset commit DEPOIS DB commit
                    buffer.clear()
                last_flush = time.time()
    finally:
        if buffer and flush():
            consumer.commit(asynchronous=False)
        consumer.close()
        conn.close()
        print(f"[consumer] PARA. {totals}")


if __name__ == "__main__":
    main()
