#!/usr/bin/env python3
import os, sys, types, time

# --- FIX Python 3.12/3.13/3.14: distutils lakon ---
try:
    import distutils.version
except ModuleNotFoundError:
    try:
        import setuptools._distutils.version as dv_mod
        import setuptools._distutils as d_mod
        sys.modules['distutils'] = d_mod
        sys.modules['distutils.version'] = dv_mod
    except Exception:
        class LooseVersion:
            def __init__(self, v): self.v = str(v)
            def __str__(self): return self.v
            def __ge__(self, o): return True
            def __lt__(self, o): return False
        mod = types.ModuleType('distutils')
        vmod = types.ModuleType('distutils.version')
        vmod.LooseVersion = LooseVersion
        sys.modules['distutils'] = mod
        sys.modules['distutils.version'] = vmod

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, Window, functions as F

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "wifi_monitoring")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": os.getenv("PG_USER", "wifi"), "password": os.getenv("PG_PASSWORD", "wifi123"),
              "driver": "org.postgresql.Driver"}
MB = 1048576.0
GB = 1073741824.0

spark = (SparkSession.builder
        .appName("DIT-WiFi-Batch-Analytics")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.sql.session.timeZone", "Asia/Dili")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
        .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
t_start = time.time()

def read_sql(query: str):
    return spark.read.jdbc(JDBC_URL, f"({query}) AS t", properties=JDBC_PROPS)

def write_table(df, table: str):
    (df.write.format("jdbc")
      .option("url", JDBC_URL).option("dbtable", table)
      .option("user", JDBC_PROPS["user"]).option("password", JDBC_PROPS["password"])
      .option("driver", JDBC_PROPS["driver"]).option("truncate", "true")
      .mode("overwrite").save())
    print(f" → hakerek {table}")

# 0. LEE DADUS
df = (read_sql("""SELECT event_id, event_time, event_type, ap_id, building, band, user_id,
                         user_role, device_type, rssi_dbm, bytes_up, bytes_down,
                         latency_ms, packet_loss_pct
                  FROM raw_events WHERE is_valid""")
     .withColumn("total_bytes", F.col("bytes_up") + F.col("bytes_down"))
     .withColumn("date", F.to_date("event_time"))
     .withColumn("hour", F.hour("event_time"))
     .withColumn("day_of_week", F.expr("((dayofweek(event_time) + 5) % 7) + 1"))
     .cache())

n_valid = df.count()
print(f"[batch] Record válidu: {n_valid:,}")
if n_valid < 100:
    raise SystemExit("Dadus menus liu. Halai producer --mode backfill uluk.")

summary = []

# A. DESCRIPTIVE
print("[batch] A. Descriptive analytics")
o = df.agg(F.count("*").alias("total_events"),
           F.countDistinct("user_id").alias("unique_users"),
           F.countDistinct("ap_id").alias("unique_aps"),
           (F.sum("total_bytes") / GB).alias("total_gb"),
           F.avg("rssi_dbm").alias("avg_rssi"),
           F.stddev("rssi_dbm").alias("std_rssi"),
           F.avg("latency_ms").alias("avg_latency_ms"),
           F.percentile_approx("latency_ms", 0.5).alias("p50_latency_ms"),
           F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
           F.avg("packet_loss_pct").alias("avg_packet_loss")).first().asDict()
for k, v in o.items():
    summary.append(("overall", k, "all", float(v or 0)))

for dim in ["user_role", "device_type", "building", "band"]:
    for r in (df.groupBy(dim)
               .agg(F.countDistinct("user_id").alias("users"),
                     (F.sum("total_bytes") / GB).alias("gb"),
                     F.count("*").alias("events")).collect()):
        cat = f"by_{dim.replace('_type', '').replace('user_', '')}"
        summary.append((cat, "unique_users", r[dim], float(r["users"])))
        summary.append((cat, "total_gb", r[dim], float(r["gb"] or 0)))
        summary.append((cat, "events", r[dim], float(r["events"])))

dq = read_sql("SELECT count(*) AS total, sum(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid FROM raw_events").first()
summary.append(("data_quality", "total_records", "all", float(dq["total"])))
summary.append(("data_quality", "valid_pct", "all", 100.0 * float(dq["valid"]) / max(1, dq["total"])))

# B. TREND
print("[batch] B. Trend analysis")
day_names = F.create_map(*[F.lit(x) for kv in [(1, "Segunda"), (2, "Tersa"), (3, "Kuarta"), (4, "Kinta"), (5, "Sesta"), (6, "Sábadu"), (7, "Domingu")] for x in kv])
hourly = (df.groupBy("date", "day_of_week", "hour")
           .agg(F.countDistinct("user_id").alias("users"), F.sum("total_bytes").alias("bytes"), F.count("*").alias("events"))
           .groupBy("day_of_week", "hour")
           .agg(F.round(F.avg("users"), 2).alias("avg_users"), F.round(F.avg("bytes") / MB, 2).alias("avg_mb"), F.sum("events").alias("events"))
           .withColumn("day_name", F.element_at(day_names, F.col("day_of_week")))
           .select("day_of_week", "day_name", "hour", "avg_users", "avg_mb", "events")
           .orderBy("day_of_week", "hour"))
write_table(hourly, "analytics_hourly_pattern")

w_day = Window.orderBy("date")
daily = (df.groupBy("date").agg(F.countDistinct("user_id").alias("unique_users"), F.round(F.sum("total_bytes") / GB, 3).alias("total_gb"), F.count("*").alias("events"), F.round(F.avg("rssi_dbm"), 2).alias("avg_rssi"))
          .withColumn("prev", F.lag("unique_users").over(w_day))
          .withColumn("users_growth_pct", F.round((F.col("unique_users") - F.col("prev")) / F.col("prev") * 100, 2))
          .drop("prev").orderBy("date"))
write_table(daily, "analytics_daily_trend")
peak = hourly.orderBy(F.desc("avg_users")).first()
summary.append(("trend", "peak_avg_users", f"{peak['day_name']} {peak['hour']:02d}h", float(peak["avg_users"])))

# C. STATISTICAL
print("[batch] C. Statistical analysis")
ap_hour = (df.groupBy("ap_id", "building", "date", "hour")
            .agg(F.countDistinct("user_id").alias("users"), F.avg("latency_ms").alias("latency"), F.avg("rssi_dbm").alias("rssi"), F.avg("packet_loss_pct").alias("loss"), F.sum("total_bytes").alias("bytes")).cache())
corr_rows = []
for x, y in [("rssi_dbm", "latency_ms"), ("rssi_dbm", "packet_loss_pct"), ("latency_ms", "packet_loss_pct")]:
    corr_rows.append((x, y, "event", float(df.stat.corr(x, y) or 0)))
for x, y in [("users", "latency"), ("users", "loss"), ("users", "bytes")]:
    corr_rows.append((x, y, "ap_hour", float(ap_hour.stat.corr(x, y) or 0)))
corr_df = spark.createDataFrame(corr_rows, "var_x string, var_y string, level string, pearson_r double")
write_table(corr_df, "analytics_correlation")

# D. K-Means SEM Pipeline (evita bug)
print("[batch] D. K-Means clustering")
ap_feat = (ap_hour.groupBy("ap_id", "building")
                 .agg(F.round(F.avg("users"), 2).alias("avg_users_per_hour"), F.max("users").alias("peak_users"), F.round(F.sum("bytes") / GB, 3).alias("total_gb"), F.round(F.avg("rssi"), 2).alias("avg_rssi"), F.round(F.avg("latency"), 2).alias("avg_latency_ms")))
features = ["avg_users_per_hour", "peak_users", "total_gb", "avg_rssi", "avg_latency_ms"]
assembler = VectorAssembler(inputCols=features, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")

df_vec = assembler.transform(ap_feat)
scaler_model = scaler.fit(df_vec)
df_scaled = scaler_model.transform(df_vec)

best = None
for k in range(2, 6):
    km = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster")
    model = km.fit(df_scaled)
    pred = model.transform(df_scaled)
    sil = evaluator.evaluate(pred)
    summary.append(("ml", "silhouette", f"k={k}", float(sil)))
    print(f" k={k} silhouette={sil:.4f}")
    if best is None or sil > best[1]:
        best = (k, sil, pred, model)

k, sil, pred, _ = best
summary.append(("ml", "best_k", "kmeans", float(k)))
means = (pred.groupBy("cluster").agg(F.avg("avg_users_per_hour").alias("m")).orderBy(F.desc("m")).collect())
labels = {}
for rank, r in enumerate(means):
    if rank == 0: labels[r["cluster"]] = "Hotspot (beban aas)"
    elif rank == len(means)-1: labels[r["cluster"]] = "Beban ki'ik"
    else: labels[r["cluster"]] = f"Beban médiu {rank}"
label_map = F.create_map(*[F.lit(x) for kv in labels.items() for x in kv])
clusters = (pred.withColumn("cluster_label", F.element_at(label_map, F.col("cluster"))).select("ap_id", "building", *features, "cluster", "cluster_label"))
write_table(clusters, "analytics_ap_clusters")

elapsed = time.time() - t_start
summary.append(("performance", "batch_runtime_s", f"{n_valid} rows", elapsed))
summary_df = (spark.createDataFrame(summary, "category string, metric string, dimension string, value double").withColumn("run_at", F.current_timestamp()).select("run_at", "category", "metric", "dimension", "value"))
write_table(summary_df, "analytics_summary")
print(f"[batch] REMATA iha {elapsed:.1f} s")
spark.stop()