#!/usr/bin/env python3
"""

 app.py — DASHBOARD REAL-TIME (Streamlit) - VERSAUN NABILAN

"""
import os
import base64
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://wifi:wifi123@localhost:5433/wifi_monitoring")
TZ = "Asia/Dili"
TH = {"users": 30, "rssi": -75, "latency": 80, "loss": 3}
# Cor nabilan liu
BUILDING_COLORS = {"REK": "#2563eb", "AULA": "#06b6d4", "LAB": "#f59e0b",
                   "BIB": "#8b5cf6", "KAN": "#ef4444"}

st.set_page_config(page_title="DIT Wi-Fi Monitor", page_icon="📶", layout="wide")

# --- FUNSAUN BACKGROUND NABILAN ---
def get_bg_base64():
    for p in ["dashboard/assets/bg.jpg", "assets/bg.jpg", "bg.jpg"]:
        if os.path.exists(p):
            with open(p, "rb") as f:
                return base64.b64encode(f.read()).decode()
    return ""

bg_b64 = get_bg_base64()
bg_url = f"url('data:image/jpg;base64,{bg_b64}')" if bg_b64 else "none"

st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700;800&display=swap');
    * {{ font-family: 'Inter', sans-serif; }}

  /* APP BACKGROUND NABILAN */
 .stApp {{
    background: linear-gradient(135deg, rgba(37,99,235,0.12) 0%, rgba(139,92,246,0.12) 30%, rgba(6,182,214,0.15) 70%, rgba(245,158,11,0.10) 100%),
                {bg_url};
    background-size: cover;
    background-attachment: fixed;
    background-blend-mode: overlay;
  }}

  /* SIDEBAR GRADIENT ESCURO NABILAN */
  section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, #0f172a 0%, #1e3a8a 30%, #3730a3 70%, #6d28d9 100%)!important;
    border-right: 1px solid rgba(255,255,255,0.1);
  }}
  section[data-testid="stSidebar"] * {{ color: #e0e7ff!important; }}
  section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3 {{
    color: #ffffff!important;
    font-weight: 800!important;
  }}

  /* METRIC CARDS - GLASS NABILAN */
  [data-testid="stMetric"] {{
    background: linear-gradient(135deg, rgba(255,255,255,0.9) 0%, rgba(255,255,255,0.75) 100%)!important;
    backdrop-filter: blur(16px);
    border-radius: 20px!important;
    border: 1px solid rgba(255,255,255,0.6)!important;
    box-shadow: 0 8px 32px rgba(37,99,235,0.12), 0 2px 8px rgba(0,0,0,0.05)!important;
    padding: 18px 20px!important;
    border-left: 5px solid #2563eb!important;
    transition: transform 0.2s;
  }}
  [data-testid="stMetric"]:hover {{ transform: translateY(-3px); box-shadow: 0 12px 40px rgba(37,99,235,0.2)!important; }}
  [data-testid="stMetricLabel"] p {{ font-size: 0.85rem; font-weight: 600; color: #475569!important; }}
  [data-testid="stMetricValue"] {{ font-weight: 800!important; color: #0f172a!important; }}

  /* TITULO GRADIENT NABILAN */
  h1 {{
    background: linear-gradient(90deg, #2563eb 0%, #7c3aed 35%, #06b6d4 70%, #0e5e6f 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 900!important;
    letter-spacing: -1px;
  }}

  /* TABS NABILAN */
  button[data-baseweb="tab"] {{
    background: rgba(255,255,255,0.6);
    border-radius: 12px 12px 0 0;
    font-weight: 700;
  }}
  button[data-baseweb="tab"][aria-selected="true"] {{
    background: linear-gradient(90deg, #2563eb, #7c3aed)!important;
    color: white!important;
  }}

  /* CHART / DATAFRAME GLASS */
  div[data-testid="stPlotlyChart"], div[data-testid="stDataFrame"] {{
    background: rgba(255,255,255,0.82)!important;
    backdrop-filter: blur(12px);
    border-radius: 18px;
    padding: 10px;
    box-shadow: 0 4px 24px rgba(0,0,0,0.06);
    border: 1px solid rgba(255,255,255,0.5);
  }}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------
# Koneksaun DB
# ---------------------------------------------------------------------
@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True,
                         connect_args={"options": f"-c timezone={TZ}"})

def q(sql: str, **params) -> pd.DataFrame:
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as exc:
        st.error(f"La konsege lee base dadus: {exc}")
        return pd.DataFrame()

# ---------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------
st.sidebar.header("⚙️ Kontrolu")
refresh = st.sidebar.slider("Refresh kada (segundu)", 2, 30, 5)
window_min = st.sidebar.slider("Janela tempu (minutu)", 5, 120, 30)
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Pipeline**\n\n"
    "1. Simulador AP → **Kafka** `wifi-logs`\n"
    "2. Consumer → PostgreSQL `raw_events`\n"
    "3. **Spark Streaming** → `ap_metrics_1min`, `alerts`\n"
    "4. **Spark Batch** → `analytics_*`\n"
    "5. **Streamlit** (pájina ne'e)")

st.title("📶 DIT Wi-Fi Usage Monitoring")
st.caption("Monitorizasaun uzu Wi-Fi kampus Dili Institute of Technology — Kafka · Spark · PostgreSQL · Streamlit")

tab_live, tab_hist, tab_dq = st.tabs(["⚡ Real-time", "📊 Analitika istóriku", "🧪 Kualidade dadus"])

# =====================================================================
# TAB 1 — REAL-TIME (hela hanesan, grafiku nabilan liu)
# =====================================================================
def live_panel():
    now = pd.Timestamp.now(tz=TZ)
    m = q("""SELECT m.window_start, m.ap_id, m.building, a.building_name, m.active_users,
                    m.event_count, m.total_mb, m.avg_rssi, m.avg_latency_ms, m.avg_packet_loss,
                    m.auth_failures, m.updated_at
             FROM ap_metrics_1min m LEFT JOIN access_points a USING (ap_id)
             WHERE m.window_start >= now() - make_interval(mins => :mins)
             ORDER BY m.window_start""", mins=window_min)
    if m.empty:
        st.info("Seidauk iha dadus streaming. Hahú `producer --mode live` no `spark/streaming_job.py`.")
        return

    latest = m["window_start"].max()
    cur = m[m.window_start == latest]
    prev = m[m.window_start == latest - pd.Timedelta(minutes=1)]

    def delta(col, agg="sum"):
        if prev.empty:
            return None
        return round(getattr(cur[col], agg)() - getattr(prev[col], agg)(), 1)

    n_alerts = q("SELECT count(*) AS n FROM alerts WHERE created_at >= now() - interval '10 minutes'")
    freshness = (now - m["updated_at"].max()).total_seconds()

    c = st.columns(6)
    c[0].metric("👥 Uzuáriu ativu", int(cur.active_users.sum()), delta("active_users"))
    c[1].metric("📦 Tráfiku (MB/min)", f"{cur.total_mb.sum():.1f}", delta("total_mb"))
    c[2].metric("📡 RSSI médiu", f"{cur.avg_rssi.mean():.1f} dBm", delta("avg_rssi", "mean"))
    c[3].metric("⏱️ Latency médiu", f"{cur.avg_latency_ms.mean():.0f} ms",
                delta("avg_latency_ms", "mean"), delta_color="inverse")
    c[4].metric("🚨 Alerta (10 min)", int(n_alerts.n.iloc[0]) if not n_alerts.empty else 0)
    c[5].metric("🔄 Dadus atualiza", f"{freshness:.0f} s liuba")

    left, right = st.columns([2, 1])
    with left:
        by_b = m.groupby(["window_start", "building"], as_index=False)["active_users"].sum()
        fig = px.line(by_b, x="window_start", y="active_users", color="building",
                      color_discrete_map=BUILDING_COLORS, markers=True,
                      labels={"window_start": "Minutu", "active_users": "Uzuáriu", "building": "Edifísiu"},
                      title="Uzuáriu ativu kada minutu tuir edifísiu")
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=50, b=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, key="live_line")
    with right:
        tb = m.groupby("building_name", as_index=False)["total_mb"].sum().sort_values("total_mb")
        fig = px.bar(tb, x="total_mb", y="building_name", orientation="h",
                     labels={"total_mb": "MB", "building_name": ""},
                     title=f"Tráfiku {window_min} minutu ikus", color_discrete_sequence=["#2563eb"])
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=50, b=10), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, key="live_bar")

    hm = m.assign(minutu=m.window_start.dt.strftime("%H:%M")) \
         .pivot_table(index="ap_id", columns="minutu", values="active_users", aggfunc="sum").fillna(0)
    fig = px.imshow(hm, aspect="auto", color_continuous_scale="Turbo",
                    labels=dict(x="Minutu", y="Access Point", color="Uzuáriu"),
                    title="Heatmap okupasaun AP")
    fig.update_layout(height=max(380, 14 * len(hm)), margin=dict(l=10, r=10, t=50, b=10), paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig, key="live_heatmap")

    a, b = st.columns(2)
    with a:
        st.subheader("🏷️ AP ho beban aas liu (minutu ikus)")
        def status(r):
            if r.active_users >= TH["users"] or r.avg_latency_ms > TH["latency"]:
                return "🔴 Kritiku"
            if r.avg_rssi < TH["rssi"] or r.avg_packet_loss > TH["loss"]:
                return "🟠 Atensaun"
            return "🟢 Normal"
        top = cur.sort_values("active_users", ascending=False).head(10).copy()
        top["status"] = top.apply(status, axis=1)
        st.dataframe(top[["status", "ap_id", "building_name", "active_users", "total_mb",
                          "avg_rssi", "avg_latency_ms", "avg_packet_loss"]], hide_index=True)
    with b:
        st.subheader("🚨 Alerta ikus")
        al = q("""SELECT created_at, severity, alert_type, ap_id, message
                  FROM alerts ORDER BY created_at DESC LIMIT 12""")
        if al.empty:
            st.success("La iha alerta. Rede normal.")
        else:
            al["created_at"] = al.created_at.dt.strftime("%H:%M:%S")
            st.dataframe(al, hide_index=True)

    st.subheader("📜 Live feed — evento ikus ne'ebé tama")
    feed = q("""SELECT ingested_at, event_time, event_type, ap_id, user_role, device_type,
                       rssi_dbm, latency_ms, is_valid, dq_reason
                FROM raw_events ORDER BY ingested_at DESC LIMIT 15""")
    if not feed.empty:
        for col in ["ingested_at", "event_time"]:
            feed[col] = feed[col].dt.strftime("%H:%M:%S")
        st.dataframe(feed, hide_index=True)
    st.caption(f"Atualiza ikus: {now:%H:%M:%S} · refresh automátiku kada {refresh} s")

with tab_live:
    st.fragment(run_every=refresh)(live_panel)()

def hist_panel():
    s = q("SELECT category, metric, dimension, value, run_at FROM analytics_summary")
    if s.empty:
        st.info("Seidauk iha rezultadu batch. Halai `python spark/batch_analytics.py`.")
        return
    st.caption(f"Batch ikus halai: {s.run_at.max():%Y-%m-%d %H:%M}")
    ov = s[s.category == "overall"].set_index("metric")["value"]
    c = st.columns(5)
    c[0].metric("Total evento", f"{ov.get('total_events', 0):,.0f}")
    c[1].metric("Uzuáriu úniku", f"{ov.get('unique_users', 0):,.0f}")
    c[2].metric("Tráfiku total", f"{ov.get('total_gb', 0):,.2f} GB")
    c[3].metric("Latency p50 / p95", f"{ov.get('p50_latency_ms', 0):.0f} / {ov.get('p95_latency_ms', 0):.0f} ms")
    c[4].metric("RSSI médiu ± sd", f"{ov.get('avg_rssi', 0):.1f} ± {ov.get('std_rssi', 0):.1f}")

    hp = q("SELECT day_of_week, day_name, hour, avg_users FROM analytics_hourly_pattern")
    if not hp.empty:
        pv = hp.sort_values("day_of_week").pivot_table(index="day_name", columns="hour",
                                                       values="avg_users", sort=False)
        fig = px.imshow(pv, aspect="auto", color_continuous_scale="Blues",
                        labels=dict(x="Oras", y="", color="Média uzuáriu"),
                        title="Padraun uzu: loron semana × oras")
        st.plotly_chart(fig, key="hist_heat")

    l, r = st.columns(2)
    with l:
        dt = q("SELECT * FROM analytics_daily_trend ORDER BY date")
        if not dt.empty:
            fig = px.line(dt, x="date", y="unique_users", markers=True,
                          title="Trend loron-loron", color_discrete_sequence=["#2563eb"])
            st.plotly_chart(fig, key="hist_daily")
    with r:
        roles = s[(s.category == "by_role") & (s.metric == "total_gb")]
        if not roles.empty:
            fig = px.pie(roles, names="dimension", values="value", hole=0.5,
                         title="Fahe tráfiku tuir role", color_discrete_sequence=px.colors.qualitative.Bold)
            st.plotly_chart(fig, key="hist_role")

    st.subheader("🤖 K-Means clustering Access Point")
    cl = q("SELECT * FROM analytics_ap_clusters")
    if not cl.empty:
        fig = px.scatter(cl, x="avg_users_per_hour", y="avg_latency_ms", color="cluster_label",
                         size="total_gb", hover_data=["ap_id", "building"],
                         title="Klaster AP: beban vs latency")
        st.plotly_chart(fig, key="hist_cluster")
        st.dataframe(cl, hide_index=True)

with tab_hist:
    if st.button("🔄 Karrega fali rezultadu batch"):
        st.rerun()
    hist_panel()

def dq_panel():
    t = q("""SELECT COALESCE(sum(received),0) AS received, COALESCE(sum(inserted),0) AS inserted,
                    COALESCE(sum(invalid),0) AS invalid, COALESCE(sum(duplicates),0) AS duplicates
             FROM dq_ingest_log""")
    if t.empty or t.received.iloc[0] == 0:
        st.info("Seidauk iha log ingestaun. Hahú `consumer/raw_consumer.py`.")
        return
    r = t.iloc[0]
    rec = float(r.received)
    c = st.columns(4)
    c[0].metric("Record simu", f"{rec:,.0f}")
    c[1].metric("Validity", f"{100 * (1 - r.invalid / rec):.2f} %")
    c[2].metric("Uniqueness", f"{100 * (1 - r.duplicates / rec):.2f} %")
    comp = q("""SELECT 100.0 * avg(CASE WHEN ap_id IS NOT NULL AND user_id IS NOT NULL
                                         AND event_time IS NOT NULL THEN 1 ELSE 0 END) AS pct FROM raw_events""")
    c[3].metric("Completeness", f"{comp.pct.iloc[0]:.2f} %" if not comp.empty else "-")

with tab_dq:
    st.fragment(run_every=refresh)(dq_panel)()