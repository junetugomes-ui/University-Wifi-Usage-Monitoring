#!/usr/bin/env python3
"""
=====================================================================
 streaming_job.py — SPARK STRUCTURED STREAMING (Real-time processing)
=====================================================================
 Fluxu:
   Kafka 'wifi-logs'
     → [1] READ     : lee stream (readStream format kafka)
     → [2] PARSE    : JSON → kolun ho schema
     → [3] CLEAN    : hasai record invalidu (filtru data quality)
     → [4] WATERMARK: tolera dadus tarde to'o 2 minutu
     → [5] AGGREGATE: janela 1 minutu kada AP (users, tráfiku, RSSI...)
     → [6] SINK     : foreachBatch → UPSERT ba PostgreSQL + deteta ALERTA
   (opsionál) console sink: rezumu kada edifísiu → hatudu iha terminal demo

 Halai:  python spark/streaming_job.py
 Spark UI:  http://localhost:4040  (Structured Streaming tab)
=====================================================================
"""
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wifi-logs")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "./checkpoints")
TRIGGER = os.getenv("TRIGGER", "10 seconds")      # micro-batch kada 10 s
DEMO_CONSOLE = os.getenv("DEMO_CONSOLE", "1") == "1"
PG = dict(host=os.getenv("PG_HOST", "localhost"), port=int(os.getenv("PG_PORT", "5432")),
          dbname=os.getenv("PG_DB", "wifi_monitoring"), user=os.getenv("PG_USER", "wifi"),
          password=os.getenv("PG_PASSWORD", "wifi123"))

# Limiar alerta (bele adapta tuir kondisaun rede DIT)
TH_OVERLOAD_WARN = int(os.getenv("TH_OVERLOAD_WARN", "30"))    # uzuáriu/AP/minutu
TH_OVERLOAD_CRIT = int(os.getenv("TH_OVERLOAD_CRIT", "40"))
TH_WEAK_RSSI = float(os.getenv("TH_WEAK_RSSI", "-75"))         # dBm
TH_LATENCY = float(os.getenv("TH_LATENCY", "80"))              # ms
TH_LOSS = float(os.getenv("TH_LOSS", "3"))                     # %
TH_AUTH_FAIL = int(os.getenv("TH_AUTH_FAIL", "5"))             # falla/minutu

# ---------------------------------------------------------------------
# [0] SparkSession — konektor Kafka download automátiku (spark.jars.packages)
# ---------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("DIT-WiFi-Streaming")
         .master(os.getenv("SPARK_MASTER", "local[*]"))
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
         .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "6"))
         .config("spark.sql.session.timeZone", "Asia/Dili")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Schema JSON — tenke hanesan ho output producer
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("ap_id", StringType()),
    StructField("building", StringType()),
    StructField("floor", IntegerType()),
    StructField("ssid", StringType()),
    StructField("band", StringType()),
    StructField("channel", IntegerType()),
    StructField("user_id", StringType()),
    StructField("user_role", StringType()),
    StructField("device_type", StringType()),
    StructField("rssi_dbm", IntegerType()),
    StructField("bytes_up", LongType()),
    StructField("bytes_down", LongType()),
    StructField("session_duration_s", LongType()),
    StructField("latency_ms", DoubleType()),
    StructField("packet_loss_pct", DoubleType()),
])

# ---------------------------------------------------------------------
# [1] READ — stream husi Kafka (startingOffsets=latest → só dadus foun)
# ---------------------------------------------------------------------
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS", "50000"))  # back-pressure
       .option("failOnDataLoss", "false")
       .load())

# ---------------------------------------------------------------------
# [2] PARSE — value (bytes) → string JSON → kolun
# ---------------------------------------------------------------------
events = (raw
          .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
          .select("e.*")
          .withColumn("event_ts", F.to_timestamp("event_time")))   # ISO-8601 + timezone

# ---------------------------------------------------------------------
# [3] CLEAN — regra hanesan ho consumer.validate()
# ---------------------------------------------------------------------
clean = (events
         .filter(F.col("event_ts").isNotNull())
         .filter(F.col("ap_id").isNotNull() & F.col("user_id").isNotNull())
         .filter(F.col("event_type").isin("CONNECT", "USAGE", "DISCONNECT", "AUTH_FAIL"))
         .filter(F.col("rssi_dbm").between(-100, -20))
         .filter((F.col("bytes_up") >= 0) & (F.col("bytes_down") >= 0))
         .withColumn("total_bytes", F.col("bytes_up") + F.col("bytes_down"))
         # [4] WATERMARK — janela sei taka 2 minutu depois evento ikus
         .withWatermark("event_ts", "2 minutes"))

# ---------------------------------------------------------------------
# [5] AGGREGATE — tumbling window 1 minutu kada AP
#     approx_count_distinct: count distinct ne'ebé suporta iha streaming
# ---------------------------------------------------------------------
ap_metrics = (clean
              .groupBy(F.window("event_ts", "1 minute"), "ap_id", "building")
              .agg(F.count("*").alias("event_count"),
                   F.approx_count_distinct("user_id").alias("active_users"),
                   F.sum(F.when(F.col("event_type") == "CONNECT", 1).otherwise(0)).alias("connects"),
                   F.sum(F.when(F.col("event_type") == "DISCONNECT", 1).otherwise(0)).alias("disconnects"),
                   F.sum(F.when(F.col("event_type") == "AUTH_FAIL", 1).otherwise(0)).alias("auth_failures"),
                   F.round(F.sum("total_bytes") / 1048576, 3).alias("total_mb"),
                   F.round(F.avg("rssi_dbm"), 2).alias("avg_rssi"),
                   F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
                   F.round(F.avg("packet_loss_pct"), 2).alias("avg_packet_loss")))

UPSERT_SQL = """
INSERT INTO ap_metrics_1min (window_start, window_end, ap_id, building, event_count, active_users,
    connects, disconnects, auth_failures, total_mb, avg_rssi, avg_latency_ms, avg_packet_loss, updated_at)
VALUES %s
ON CONFLICT (ap_id, window_start) DO UPDATE SET
    event_count = EXCLUDED.event_count, active_users = EXCLUDED.active_users,
    connects = EXCLUDED.connects, disconnects = EXCLUDED.disconnects,
    auth_failures = EXCLUDED.auth_failures, total_mb = EXCLUDED.total_mb,
    avg_rssi = EXCLUDED.avg_rssi, avg_latency_ms = EXCLUDED.avg_latency_ms,
    avg_packet_loss = EXCLUDED.avg_packet_loss, updated_at = now()
"""
UPSERT_TEMPLATE = "(to_timestamp(%s), to_timestamp(%s), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())"

ALERT_SQL = """
INSERT INTO alerts (ap_id, building, window_start, alert_type, severity, metric_value, threshold, message)
VALUES %s
ON CONFLICT (ap_id, window_start, alert_type) DO NOTHING
"""
ALERT_TEMPLATE = "(%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s)"


def detect_alerts(rows):
    """Regra alerta simples (rule-based) ba kada linha agregasaun."""
    out = []
    for r in rows:
        def add(atype, sev, val, th, msg):
            out.append((r.ap_id, r.building, r.ws, atype, sev, float(val), float(th), msg))
        if r.active_users >= TH_OVERLOAD_WARN:
            sev = "CRITICAL" if r.active_users >= TH_OVERLOAD_CRIT else "WARNING"
            add("OVERLOAD", sev, r.active_users, TH_OVERLOAD_WARN,
                f"{r.ap_id}: {r.active_users} uzuáriu iha minutu ida — AP besik nakonu")
        if r.avg_rssi is not None and r.avg_rssi < TH_WEAK_RSSI:
            add("WEAK_SIGNAL", "WARNING", r.avg_rssi, TH_WEAK_RSSI,
                f"{r.ap_id}: sinal fraku (média {r.avg_rssi} dBm)")
        if r.avg_latency_ms is not None and r.avg_latency_ms > TH_LATENCY:
            add("HIGH_LATENCY", "CRITICAL", r.avg_latency_ms, TH_LATENCY,
                f"{r.ap_id}: latency aas ({r.avg_latency_ms} ms)")
        if r.avg_packet_loss is not None and r.avg_packet_loss > TH_LOSS:
            add("PACKET_LOSS", "WARNING", r.avg_packet_loss, TH_LOSS,
                f"{r.ap_id}: packet loss {r.avg_packet_loss}%")
        if r.auth_failures >= TH_AUTH_FAIL:
            add("AUTH_FAIL_SPIKE", "WARNING", r.auth_failures, TH_AUTH_FAIL,
                f"{r.ap_id}: {r.auth_failures} autentikasaun falla — verifika password/ataka")
    return out


# ---------------------------------------------------------------------
# [6] SINK — foreachBatch: kada micro-batch → UPSERT PostgreSQL + alerta
#     Window timestamp konverte ba epoch (long) atu evita problema timezone
# ---------------------------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    rows = (batch_df.select(
                F.col("window.start").cast("long").alias("ws"),
                F.col("window.end").cast("long").alias("we"),
                "ap_id", "building", "event_count", "active_users", "connects", "disconnects",
                "auth_failures", "total_mb", "avg_rssi", "avg_latency_ms", "avg_packet_loss")
            .collect())                         # rezultadu agregadu ki'ik → seguru atu collect
    if not rows:
        return
    alerts = detect_alerts(rows)
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:        # 'with conn' = transasaun (commit/rollback)
            execute_values(cur, UPSERT_SQL, [tuple(r) for r in rows], template=UPSERT_TEMPLATE)
            if alerts:
                execute_values(cur, ALERT_SQL, alerts, template=ALERT_TEMPLATE)
    finally:
        conn.close()
    print(f"[stream] batch {batch_id}: upsert {len(rows)} linha AP-minutu, {len(alerts)} alerta")


query_pg = (ap_metrics.writeStream
            .outputMode("update")               # haruka só janela ne'ebé muda
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", f"{CHECKPOINT}/ap_metrics")   # tolerânsia ba falla
            .trigger(processingTime=TRIGGER)
            .queryName("ap_metrics_to_postgres")
            .start())

# (Opsionál) console sink ba demo: rezumu kada edifísiu iha terminal
if DEMO_CONSOLE:
    building_summary = (clean
                        .groupBy(F.window("event_ts", "1 minute"), "building")
                        .agg(F.count("*").alias("events"),
                             F.approx_count_distinct("user_id").alias("users"),
                             F.round(F.sum("total_bytes") / 1048576, 2).alias("mb"))
                        .select(F.date_format("window.start", "HH:mm").alias("minutu"),
                                "building", "events", "users", "mb"))
    (building_summary.writeStream
     .outputMode("update")
     .format("console")
     .option("truncate", "false")
     .option("numRows", 20)
     .option("checkpointLocation", f"{CHECKPOINT}/console")
     .trigger(processingTime=TRIGGER)
     .queryName("building_console")
     .start())

print(f"[stream] Spark Structured Streaming hahú ona. Kafka={BOOTSTRAP} topic={TOPIC} trigger={TRIGGER}")
print("[stream] Spark UI: http://localhost:4040")
spark.streams.awaitAnyTermination()
