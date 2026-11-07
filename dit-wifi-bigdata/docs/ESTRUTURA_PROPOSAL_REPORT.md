# Estrutura Proposal no Final Report
**Capstone Project — Big Data · S1 Siénsia Komputador · Dili Institute of Technology**
**Tema:** Sistema Monitorizasaun Uzu Wi-Fi Universidade Real-time ho Apache Kafka no Apache Spark

> Título sujestaun: *"Dezeñu no Implementasaun Sistema Big Data Real-time ba Monitorizasaun Uzu Wi-Fi Kampus DIT uza Apache Kafka, Apache Spark no PostgreSQL"*

---

## A. PROPOSAL (Kapítulu 1–5) — ±15–25 pájina

**Pájina preliminár:** kapa, pájina aprovasaun, rezumu (abstract ±200 liafuan), índise, lista tabela/figura, lista abreviatura (AP, RSSI, ETL, DQ, KPI).

### Kapítulu 1 — Introdusaun
1.1 **Kontextu (Background)** — Wi-Fi hanesan infraestrutura esensiál ba aprendizajen; kresimentu uzuáriu no dispozitivu iha DIT; problema ne'ebé estudante sente (koneksaun neineik iha oras pik, sinal fraku iha sala balu); ekipa TI seidauk iha vizibilidade real-time; log AP produz dadus ho volume, velosidade no variedade aas (karakterístika Big Data 5V).
1.2 **Formulasaun problema** — ezemplu:
  - Oinsá dezeña pipeline ne'ebé bele simu no prosesa log Wi-Fi iha real-time?
  - Oras no fatin ne'ebé mak iha beban aas liu?
  - Faktór saida mak afeita kualidade koneksaun (RSSI, beban AP, latency)?
  - Bele grupu AP tuir padraun uzu atu apoia desizaun ba upgrade rede?
1.3 **Objetivu** — jerál (dezenvolve sistema monitorizasaun real-time) no espesífiku (ingestaun Kafka; prosesamentu Spark batch + streaming; storage PostgreSQL; analitika descriptive/trend/statistical/clustering; dashboard real-time; teste performance).
1.4 **Benefísiu** — ba Divizaun TI (planeamentu kapasidade, deteksaun problema lalais), ba estudante/staff (kualidade koneksaun), ba akadémiku (referénsia implementasaun Big Data iha Timor-Leste).
1.5 **Skopu no limitasaun** — dadus simuladu ne'ebé reprezenta log controller (tanba privasidade no asesu); ≥ 10,000 record; 36 AP iha edifísiu 5; la inklui seguransa rede avansadu; deploy lokál (Docker, single node).
1.6 **Sistematika hakerek** — rezumu badak kada kapítulu.

### Kapítulu 2 — Revizaun Literatura
2.1 Konseitu Big Data (5V, arkitetura Lambda vs Kappa, medallion bronze/silver/gold).
2.2 Rede Wi-Fi no métrika kualidade (IEEE 802.11, band 2.4/5 GHz, RSSI, latency, packet loss, autentikasaun WPA2-Enterprise).
2.3 Apache Kafka (topic, partisaun, producer, consumer group, offset, garantia entrega).
2.4 Apache Spark (RDD/DataFrame, Structured Streaming, micro-batch, watermark, window, MLlib).
2.5 PostgreSQL ba dadus time-series (índise, UPSERT, alternativa HDFS/MongoDB — tanba sá hili PostgreSQL).
2.6 Visualizasaun dadus no dashboard (Streamlit, prinsípiu dashboard operasionál).
2.7 Métodu analitika (estatístika deskritiva, korrelasaun Pearson, z-score, K-Means, silhouette score).
2.8 **Estudu anteriór** — tabela 5–8 jornál (autór, tinan, métodu, rezultadu, diferensa ho projetu ne'e).
2.9 Kuadru konseptuál (diagrama input → prosesu → output).

### Kapítulu 3 — Metodolojia
3.1 Métodu dezenvolvimentu (ezemplu: **CRISP-DM** ba parte analitika + **Agile/Scrum** sprint 2 semana ba dezenvolvimentu sistema).
3.2 Fonte dadus — deskrisaun simulador (baze ba padraun oras klase, edifísiu, role), no oinsá troka ho fonte loos (syslog/REST API controller).
3.3 **Dicionáriu dadus** — tabela kampu 18 (naran, tipu, deskrisaun, ezemplu, regra validasaun).
3.4 Ferramenta no ambiente (hardware laptop, Docker, Python 3.11, Kafka 3.8, Spark 3.5, PostgreSQL 16, Streamlit).
3.5 Planu analitika (pergunta análize ↔ métodu ↔ output vizuál).
3.6 Planu teste (rezumu husi TEST_PLAN.md).
3.7 Indikadór susesu / KPI projetu (ex. latency end-to-end < 15 s, throughput ≥ 500 evento/s, validity ≥ 98%).

### Kapítulu 4 — Dezeñu Sistema Proposta
4.1 Diagrama arkitetura sistema (husi data source to'o dashboard).
4.2 Data flow diagram (DFD nível 0 no 1) / diagrama sequénsia.
4.3 Dezeñu storage — ERD no deskrisaun tabela (raw_events, ap_metrics_1min, alerts, analytics_*).
4.4 Dezeñu prosesamentu — streaming (janela, watermark, regra alerta) no batch (job analitika + ML).
4.5 Wireframe dashboard (tab Real-time, Analitika, Kualidade dadus).
4.6 Rekezitu funsionál no non-funsionál (performance, scalability, reliability, privasidade).

### Kapítulu 5 — Kronograma, Rekursu no Risku
5.1 Kronograma (Gantt 14 semana): S1–2 proposal & literatura · S3–4 setup infraestrutura · S5–6 ingestion · S7–9 processing & storage · S10–11 analytics & ML · S12 dashboard · S13 teste · S14 report & demo.
5.2 Rekursu no orsamentu (hardware, software open source = $0, internet).
5.3 Analize risku no mitigasaun (RAM laptop la to'o → reduz paralelizmu; asesu dadus loos → simulador; tempu).
5.4 Rezultadu ne'ebé espera.

**Referénsia** (APA/IEEE, minimu 15) · **Anexu** (skema inisiál, dicionáriu dadus kompletu).

---

## B. FINAL REPORT (Kapítulu 1–6) — ±40–70 pájina

**Pájina preliminár:** hanesan proposal + deklarasaun orijinalidade, agradesimentu.

### Kapítulu 1 — Introdusaun
Atualiza husi proposal (1.1–1.6) — muda tempu verbu ba "halo ona", aumenta kontribuisaun projetu.

### Kapítulu 2 — Revizaun Literatura
Hanesan proposal, aumenta referénsia foun ne'ebé uza durante implementasaun.

### Kapítulu 3 — Metodolojia
Hanesan proposal, maibé deskreve saida mak **halo duni** (inklui mudansa husi planu no razaun).

### Kapítulu 4 — Implementasaun Sistema
4.1 Setup ambiente (docker-compose, konfigurasaun Kafka KRaft, PostgreSQL).
4.2 **Data source & ingestion** — kódigu producer (simulasaun sesaun, padraun oras, injesaun dadus foer), konfigurasaun producer (acks, idempotence, compression), consumer (validasaun, commit manual, idempotent insert). Screenshot Kafka UI.
4.3 **Data processing** — Spark Structured Streaming (parse, limpeza, watermark, window, foreachBatch UPSERT, regra alerta); Spark batch (job analitika). Screenshot Spark UI.
4.4 **Data storage** — implementasaun schema, índise, estratéjia UPSERT, volume dadus final.
4.5 **Data analytics** — implementasaun descriptive, trend, statistical, K-Means (seleksaun k ho silhouette).
4.6 **Dashboard** — screenshot kada tab ho esplikasaun.
4.7 Dezafiu téknika no solusaun (ex. timezone, dadus tarde, duplikadu).

### Kapítulu 5 — Rezultadu no Diskusaun
5.1 **Descriptive** — total evento, uzuáriu úniku, tráfiku, distribuisaun role/dispozitivu/edifísiu (tabela + gráfiku).
5.2 **Trend** — padraun oras pik, diferensa loron servisu vs fin-de-semana, trend loron-loron.
5.3 **Statistical** — korrelasaun (RSSI–latency, uzuáriu–latency), persentil latency, outlier.
5.4 **Machine learning** — rezultadu K-Means, silhouette kada k, interpretasaun klaster (hotspot → rekomenda aumenta AP).
5.5 **Rezultadu teste** — functional (pass/fail), performance (throughput, latency), data quality (%), scalability (gráfiku).
5.6 Diskusaun — hatán formulasaun problema 1-by-1; kompara ho estudu anteriór.
5.7 Limitasaun sistema.

### Kapítulu 6 — Konkluzaun no Rekomendasaun
6.1 Konkluzaun (hatán kada objetivu espesífiku).
6.2 Rekomendasaun ba Divizaun TI DIT (ex. aumenta AP iha klaster hotspot, monitoriza kanal 2.4 GHz).
6.3 Servisu futuru (integra ho controller loos, deteksaun anomalia ho ML, prediksaun beban ho regression/LSTM, deploy cluster multi-node, notifikasaun Telegram).

**Referénsia** · **Anexu:** A. Kódigu fonte (link GitHub) · B. Manual instalasaun no uza · C. Log teste kompletu · D. Screenshot adisionál · E. Dicionáriu dadus.
