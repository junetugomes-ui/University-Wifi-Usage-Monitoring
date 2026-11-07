import os, psycopg2
PG_PORT=os.getenv("PG_PORT","5433")
conn=psycopg2.connect(host="localhost",port=PG_PORT,dbname="wifi_monitoring",user="wifi",password="wifi123")
cur=conn.cursor()
print("Fix ap_metrics_1min schema...")

cur.execute('''
DROP TABLE IF EXISTS ap_metrics_1min CASCADE;
CREATE TABLE ap_metrics_1min AS
SELECT
    date_trunc('minute', event_time) as window_start,
    ap_id,
    building,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(*) as event_count,
    SUM((bytes_up+bytes_down)/1048576.0) as total_mb,
    AVG(rssi_dbm) as avg_rssi,
    AVG(latency_ms) as avg_latency_ms,
    AVG(packet_loss_pct) as avg_packet_loss,
    SUM(CASE WHEN event_type='auth_failure' THEN 1 ELSE 0 END) as auth_failures,
    NOW() as updated_at
FROM raw_events
WHERE is_valid = true
GROUP BY 1,2,3;

DROP TABLE IF EXISTS analytics_summary CASCADE;
CREATE TABLE analytics_summary AS
SELECT NOW() as run_at, 'overall'::text as category, 'total_events'::text as metric, 'all'::text as dimension, COUNT(*)::double precision as value FROM raw_events
UNION ALL SELECT NOW(),'overall','unique_users','all',COUNT(DISTINCT user_id)::double precision FROM raw_events
UNION ALL SELECT NOW(),'overall','unique_aps','all',COUNT(DISTINCT ap_id)::double precision FROM raw_events
UNION ALL SELECT NOW(),'overall','total_gb','all',SUM((bytes_up+bytes_down)/1073741824.0)::double precision FROM raw_events;

DROP TABLE IF EXISTS analytics_hourly_pattern CASCADE;
CREATE TABLE analytics_hourly_pattern AS
SELECT EXTRACT(DOW FROM event_time)::int as day_of_week, TRIM(to_char(event_time,'Day')) as day_name, EXTRACT(HOUR FROM event_time)::int as hour, COUNT(DISTINCT user_id)::double precision as avg_users, SUM(bytes_up+bytes_down)/1048576.0 as avg_mb, COUNT(*) as events FROM raw_events WHERE is_valid GROUP BY 1,2,3 ORDER BY 1,3;

DROP TABLE IF EXISTS analytics_daily_trend CASCADE;
CREATE TABLE analytics_daily_trend AS
SELECT event_time::date as date, COUNT(DISTINCT user_id) as unique_users, SUM(bytes_up+bytes_down)/1073741824.0 as total_gb, COUNT(*) as events, AVG(rssi_dbm) as avg_rssi FROM raw_events WHERE is_valid GROUP BY 1 ORDER BY 1;

DROP TABLE IF EXISTS analytics_ap_clusters CASCADE;
CREATE TABLE analytics_ap_clusters AS
SELECT ap_id, building, AVG(rssi_dbm)::double precision as avg_rssi, COUNT(DISTINCT user_id)::double precision as avg_users_per_hour, COUNT(*)::int as peak_users, SUM(bytes_up+bytes_down)/1073741824.0 as total_gb, AVG(latency_ms)::double precision as avg_latency_ms, 0 as cluster, 'Hotspot' as cluster_label FROM raw_events WHERE is_valid GROUP BY ap_id, building;
''')
conn.commit()
print("SUCESSU! Refresh http://localhost:8501")