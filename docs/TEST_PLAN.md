# Planu Teste — DIT Wi-Fi Usage Monitoring

Kada teste iha ID, pasu, rezultadu espera no kritériu aseitasaun. Rejista rezultadu loos iha kolun "Rezultadu" no hatama iha Kapítulu 5.5 Final Report.
Komandu SQL hotu halai iha: `docker exec -it postgres psql -U wifi -d wifi_monitoring`.

---

## 1. Functional testing — "sistema halo saida mak tenke halo?"

| ID | Teste | Pasu | Rezultadu espera | Rezultadu |
|----|-------|------|------------------|-----------|
| F01 | Producer haruka ba Kafka | Halai producer `--mode live --max-events 1000` | Kafka UI: topic `wifi-logs` hetan mensajen 1000 | |
| F02 | Consumer arkiva raw | Halai consumer, tuir fali F01 | `SELECT count(*) FROM raw_events` sae 1000 (menus duplikadu) | |
| F03 | Validasaun dadus invalidu | `pytest tests/test_pipeline.py::test_validate_detects_bad_records` | Razaun loos rejista iha `dq_reason` | |
| F04 | Duplikadu la tama | Haruka evento ho `event_id` hanesan dala 2 | Linha 1 de'it iha `raw_events`; `dq_ingest_log.duplicates` = 1 | |
| F05 | Agregasaun streaming | Halai streaming job + producer live | `ap_metrics_1min` iha linha foun kada minutu ≤ 20 s depois | |
| F06 | UPSERT la duplika janela | `SELECT ap_id, window_start, count(*) FROM ap_metrics_1min GROUP BY 1,2 HAVING count(*)>1` | 0 linha | |
| F07 | Alerta insidente | Producer `--incident-ap LAB-F2-AP03` | Alerta HIGH_LATENCY/PACKET_LOSS ba AP ne'e iha ≤ 2 minutu | |
| F08 | Dashboard auto-refresh | Loke dashboard, hein 10 s | KPI no "Dadus atualiza" muda la presiza F5 | |
| F09 | Batch analytics | `python spark/batch_analytics.py` | Tabela `analytics_*` 5 hotu iha dadus; dashboard tab 2 hatudu gráfiku | |
| F10 | Recovery consumer | Para consumer 1 min (producer kontinua), halai fali | La iha dadus lakon: count `raw_events` = total haruka (menus duplikadu) | |
| F11 | Recovery Spark (checkpoint) | Para streaming job, hein 1 min, halai fali | Kontinua husi offset ikus, janela minutu ne'ebé falta sei kalkula | |

---

## 2. Performance testing — "lalais oinsá?"

**Métrika:**
- *Throughput producer* — output producer (`rate=.../s`)
- *Throughput ingestaun* — `SELECT date_trunc('minute', logged_at), sum(received) FROM dq_ingest_log GROUP BY 1 ORDER BY 1 DESC LIMIT 5;`
- *Latency ingestaun (event → DB)* — `SELECT avg(batch_latency_ms) FROM dq_ingest_log WHERE logged_at > now() - interval '2 min';`
- *Latency end-to-end (event → dashboard)* — KPI "Dadus atualiza" + trigger Spark
- *Spark* — Spark UI :4040 → Structured Streaming → *Input rate*, *Process rate*, *Batch duration*
- *Consumer lag* — `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group wifi-raw-archiver`
- *Rekursu* — `docker stats` + Task Manager (CPU, RAM)

**Senáriu (kada ida 3 minutu):**

| ID | Rate producer | Throughput DB (rec/s) | Latency ingestaun (s) | Spark batch duration (s) | Consumer lag | CPU/RAM | Status |
|----|---------------|------------------------|------------------------|---------------------------|--------------|---------|--------|
| P01 | 50/s | | | | | | |
| P02 | 200/s | | | | | | |
| P03 | 500/s | | | | | | |
| P04 | 1,000/s | | | | | | |
| P05 | 2,000/s (stress) | | | | | | |

Komandu: `python -m producer.wifi_producer --mode live --rate 500 --load 1.0 --max-events 90000`

**Kritériu aseitasaun (ezemplu):** iha 200 evento/s → latency ingestaun < 5 s, end-to-end < 20 s, consumer lag la sae kontinuamente, Spark *process rate ≥ input rate*.
**Batch:** rejista `batch_runtime_s` husi `analytics_summary` ba 20k / 100k / 500k record.

---

## 3. Data quality testing — "dadus loos no kompletu?"

Producer hatama **2% dadus foer** ho hakarak (`--dirty-rate 0.02`): AP mamuk, RSSI la fíziku, bytes negativu, timestamp sala, duplikadu.

| ID | Dimensaun | Query SQL | Target |
|----|-----------|-----------|--------|
| DQ01 | Completeness | `SELECT 100.0*avg((ap_id IS NOT NULL AND user_id IS NOT NULL AND event_time IS NOT NULL)::int) FROM raw_events;` | ≥ 99% |
| DQ02 | Validity | `SELECT 100.0*avg(is_valid::int) FROM raw_events;` | ≈ 98% (tuir dirty-rate) |
| DQ03 | Deteksaun erru | `SELECT dq_reason, count(*) FROM raw_events WHERE NOT is_valid GROUP BY 1;` | Kategoria erru 4 hotu detetadu |
| DQ04 | Uniqueness | `SELECT sum(duplicates)*100.0/sum(received) FROM dq_ingest_log;` + `SELECT count(*)-count(DISTINCT event_id) FROM raw_events;` | Duplikadu iha tabela = 0 |
| DQ05 | Timeliness | `SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at-event_time)) FROM raw_events WHERE is_valid AND ingested_at > now()-interval '10 min';` | p95 < 10 s |
| DQ06 | Consistency (range) | `SELECT count(*) FROM raw_events WHERE is_valid AND (rssi_dbm NOT BETWEEN -100 AND -20 OR bytes_down<0);` | 0 |
| DQ07 | Consistency (referénsia) | `SELECT count(*) FROM raw_events r LEFT JOIN access_points a USING(ap_id) WHERE r.is_valid AND a.ap_id IS NULL;` | 0 |
| DQ08 | Reconciliation | Total `sent` producer vs `received` consumer vs `count(raw_events)+duplicates` | Diferensa = 0 |
| DQ09 | Streaming vs batch | Kompara `sum(event_count)` iha `ap_metrics_1min` ho `count(*)` `raw_events` válidu ba minutu hanesan | Diferensa < 1% (duplikadu no dadus tarde) |

---

## 4. Scalability testing — "bele sai boot?"

| ID | Tipu | Pasu | Mede |
|----|------|------|------|
| S01 | Volume dadus (batch) | Backfill 20k → 100k → 500k → 1M record, halai batch job | Runtime vs volume (gráfiku; ideál besik linear) |
| S02 | Vertical (Spark core) | `SPARK_MASTER=local[1]`, `local[2]`, `local[4]`, `local[*]` ho dadus 500k | Speed-up = T(1 core) / T(n core) |
| S03 | Horizontal (consumer) | Halai `raw_consumer` 1 → 2 → 3 instánsia (group hanesan) iha rate 1,000/s | Throughput total, Kafka UI hatudu partisaun fahe (rebalance) |
| S04 | Partisaun Kafka | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic wifi-logs --partitions 6`, repete S03 ho consumer 6 | Throughput maximu |
| S05 | Shuffle partitions | `SHUFFLE_PARTITIONS=2 / 6 / 12` ba streaming job | Batch duration iha Spark UI |
| S06 | Número AP | Aumenta `BUILDINGS` (ex. 36 → 150 AP, simula kampus boot) | Linha `ap_metrics_1min`/minutu no batch duration |

**Template rezultadu S01:**

| Record | Runtime batch (s) | Record/s |
|--------|-------------------|----------|
| 20,000 | | |
| 100,000 | | |
| 500,000 | | |
| 1,000,000 | | |

**Nota ba report:** esplika katak Kafka partisaun = limite paralelizmu consumer, no katak Spark bele muda ba cluster (standalone/YARN/K8s) la presiza muda kódigu — muda `SPARK_MASTER` de'it.
