-- =====================================================================
--  STORAGE SCHEMA — PostgreSQL  (base dadus: wifi_monitoring)
--  Arkitetura "medallion" simples:
--    BRONZE : raw_events           → log orijinál husi Kafka (inklui dadus sala)
--    GOLD   : ap_metrics_1min      → agregasaun real-time husi Spark Streaming
--             alerts               → alerta automátiku (AP sobrekarga, sinal fraku)
--    BATCH  : analytics_*          → rezultadu Spark batch (estatístika, trend, ML)
--    DQ     : dq_ingest_log        → métrika kualidade dadus kada batch ingestaun
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. DIMENSAUN: Access Point (AP) iha kampus DIT
--    Tenke hanesan ho lista BUILDINGS iha producer/wifi_producer.py
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_points (
    ap_id          TEXT PRIMARY KEY,          -- ex: LAB-F2-AP03
    building       TEXT NOT NULL,             -- kódigu edifísiu
    building_name  TEXT NOT NULL,
    floor          INT  NOT NULL,
    max_clients    INT  NOT NULL DEFAULT 30   -- kapasidade rekomendada
);

INSERT INTO access_points (ap_id, building, building_name, floor, max_clients)
SELECT b.code || '-F' || f || '-AP' || lpad(a::text, 2, '0'),
       b.code, b.name, f, b.cap
FROM (VALUES
        ('REK',  'Edifísiu Reitoria',        2, 3, 25),
        ('AULA', 'Edifísiu Sala Aula',       3, 4, 35),
        ('LAB',  'Laboratóriu Komputador',   2, 4, 35),
        ('BIB',  'Biblioteka',               2, 3, 30),
        ('KAN',  'Kantina & Área Estudante', 1, 4, 40)
     ) AS b(code, name, floors, aps, cap)
CROSS JOIN LATERAL generate_series(1, b.floors) AS f
CROSS JOIN LATERAL generate_series(1, b.aps)    AS a
ON CONFLICT (ap_id) DO NOTHING;

-- ---------------------------------------------------------------------
-- 2. BRONZE: raw_events — kada log Wi-Fi ne'ebé Kafka consumer simu
--    event_id = PRIMARY KEY → duplikadu automátiku la tama (idempotent)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_events (
    event_id            TEXT PRIMARY KEY,
    event_time          TIMESTAMPTZ,              -- NULL se timestamp invalidu
    event_type          TEXT,                     -- CONNECT | USAGE | DISCONNECT | AUTH_FAIL
    ap_id               TEXT,
    building            TEXT,
    floor               INT,
    ssid                TEXT,
    band                TEXT,                     -- 2.4GHz | 5GHz
    channel             INT,
    user_id             TEXT,                     -- ID pseudónimu (privasidade)
    user_role           TEXT,                     -- student | staff | guest
    device_type         TEXT,
    rssi_dbm            INT,                      -- forsa sinal (-30 di'ak, -90 aat)
    bytes_up            BIGINT,
    bytes_down          BIGINT,
    session_duration_s  BIGINT,
    latency_ms          DOUBLE PRECISION,
    packet_loss_pct     DOUBLE PRECISION,
    is_valid            BOOLEAN NOT NULL DEFAULT TRUE,
    dq_reason           TEXT,                     -- razaun se la validu
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_event_time  ON raw_events (event_time);
CREATE INDEX IF NOT EXISTS idx_raw_ap          ON raw_events (ap_id);
CREATE INDEX IF NOT EXISTS idx_raw_valid       ON raw_events (is_valid);
CREATE INDEX IF NOT EXISTS idx_raw_ingested    ON raw_events (ingested_at DESC);

-- ---------------------------------------------------------------------
-- 3. GOLD (streaming): métrika kada AP kada minutu
--    Spark halo UPSERT (ON CONFLICT) tanba janela minutu ida bele atualiza
--    dala barak antes watermark taka.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ap_metrics_1min (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    ap_id            TEXT NOT NULL,
    building         TEXT NOT NULL,
    event_count      INT,
    active_users     INT,                 -- uzuáriu úniku (approx) iha minutu ne'e
    connects         INT,
    disconnects      INT,
    auth_failures    INT,
    total_mb         DOUBLE PRECISION,    -- tráfiku (up + down)
    avg_rssi         DOUBLE PRECISION,
    avg_latency_ms   DOUBLE PRECISION,
    avg_packet_loss  DOUBLE PRECISION,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ap_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_metrics_window ON ap_metrics_1min (window_start DESC);

-- ---------------------------------------------------------------------
-- 4. GOLD (streaming): alerta
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id      BIGSERIAL PRIMARY KEY,
    ap_id         TEXT NOT NULL,
    building      TEXT,
    window_start  TIMESTAMPTZ NOT NULL,
    alert_type    TEXT NOT NULL,          -- OVERLOAD | WEAK_SIGNAL | HIGH_LATENCY | PACKET_LOSS | AUTH_FAIL_SPIKE
    severity      TEXT NOT NULL,          -- WARNING | CRITICAL
    metric_value  DOUBLE PRECISION,
    threshold     DOUBLE PRECISION,
    message       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ap_id, window_start, alert_type)
);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts (created_at DESC);

-- ---------------------------------------------------------------------
-- 5. DATA QUALITY: log kada batch ingestaun husi raw_consumer
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_ingest_log (
    id                BIGSERIAL PRIMARY KEY,
    logged_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    received          INT,       -- mensajen simu husi Kafka
    inserted          INT,       -- rai duni (la duplikadu)
    invalid           INT,       -- la liu validasaun
    duplicates        INT,       -- rejeita tanba event_id ona iha
    batch_latency_ms  DOUBLE PRECISION   -- média (tempu rai − event_time)
);

-- ---------------------------------------------------------------------
-- 6. BATCH ANALYTICS (Spark batch hakerek, modu overwrite + truncate)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_summary (
    run_at     TIMESTAMPTZ,
    category   TEXT,          -- overall | by_role | by_device | by_building | ml | outlier | data_quality
    metric     TEXT,
    dimension  TEXT,
    value      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS analytics_hourly_pattern (
    day_of_week  INT,         -- 1 = Segunda ... 7 = Domingu
    day_name     TEXT,
    hour         INT,
    avg_users    DOUBLE PRECISION,
    avg_mb       DOUBLE PRECISION,
    events       BIGINT
);

CREATE TABLE IF NOT EXISTS analytics_daily_trend (
    date              DATE,
    unique_users      BIGINT,
    total_gb          DOUBLE PRECISION,
    events            BIGINT,
    avg_rssi          DOUBLE PRECISION,
    users_growth_pct  DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS analytics_ap_clusters (
    ap_id               TEXT,
    building            TEXT,
    avg_users_per_hour  DOUBLE PRECISION,
    peak_users          BIGINT,
    total_gb            DOUBLE PRECISION,
    avg_rssi            DOUBLE PRECISION,
    avg_latency_ms      DOUBLE PRECISION,
    cluster             INT,
    cluster_label       TEXT
);

CREATE TABLE IF NOT EXISTS analytics_correlation (
    var_x      TEXT,
    var_y      TEXT,
    level      TEXT,         -- event | ap_hour
    pearson_r  DOUBLE PRECISION
);
