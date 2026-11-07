#!/usr/bin/env python3
"""
 app.py — DASHBOARD REAL-TIME (Streamlit) - lee husi live_metrics (simple_streaming.py)
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
BUILDING_COLORS = {"REK": "#2563eb", "AULA": "#06b6d4", "LAB": "#f59e0b",
                   "BIB": "#8b5cf6", "KAN": "#c91616"}

st.set_page_config(page_title="Wi-Fi Monitor Kampus", page_icon="📶", layout="wide")


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
.stApp {{
    background: linear-gradient(135deg, rgba(37,99,235,0.12) 0%, rgba(139,92,246,0.12) 30%, rgba(6,182,214,0.15) 70%, rgba(245,158,11,0.10) 100%),
                {bg_url};
    background-size: cover;
    background-attachment: fixed;
    background-blend-mode: overlay;
  }}
  section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, #0f172a 0%, #1e3a8a 30%, #3730a3 70%, #6d28d9 100%)!important;
    border-right: 1px solid rgba(255,255,255,0.1);
  }}
  section[data-testid="stSidebar"] * {{ color: #e0e7ff!important; }}
  section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3 {{
    color: #ffffff!important; font-weight: 800!important;
  }}
  [data-testid="stMetric"] {{
    background: linear-gradient(135deg, rgba(255,255,255,0.9) 0%, rgba(255,255,255,0.75) 100%)!important;
    backdrop-filter: blur(16px); border-radius: 20px!important;
    border: 1px solid rgba(255,255,255,0.6)!important;
    box-shadow: 0 8px 32px rgba(37,99,235,0.12), 0 2px 8px rgba(0,0,0,0.05)!important;
    padding: 18px 20px!important; border-left: 5px solid #2563eb!important;
  }}
  h1 {{
    background: linear-gradient(90deg, #2563eb 0%, #7c3aed 35%, #06b6d4 70%, #0e5e6f 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    font-weight: 900!important;
  }}
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True, connect_args={"options": f"-c timezone={TZ}"})


def q(sql: str, **params) -> pd.DataFrame:
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as exc:
        st.error(f"La konsege lee base dadus: {exc} | DB_URL={DB_URL}")
        return pd.DataFrame()


st.sidebar.header("⚙️ Kontrolu")
refresh = st.sidebar.slider("Refresh kada (segundu)", 2, 30, 5)
window_min = st.sidebar.slider("Janela tempu (minutu)", 5, 120, 30)
st.sidebar.markdown("---")
st.sidebar.markdown("**Pipeline**\n\n1. Simulador AP → **Kafka** `wifi-logs`\n"
                    "2. **Simple Streaming** (`simple_streaming.py`) → `live_metrics`\n"
                    "3. **Streamlit**")

st.title("📶 Wi-Fi Usage Monitoring")
st.caption("Monitorizasaun uzu Wi-Fi kampus — Kafka · Spark · PostgreSQL · Streamlit")

tab_live, tab_hist, tab_dq = st.tabs(["⚡ Real-time", "📊 Analitika istóriku", "🧪 Kualidade dadus"])


# =====================================================================
# TAB 1 — REAL-TIME
# =====================================================================
def live_panel():
    now = pd.Timestamp.now(tz=TZ)

    m = q("""SELECT window_start, ap_id, building, user_count AS active_users, event_count,
                    avg_rssi, avg_latency AS avg_latency_ms, updated_at
             FROM live_metrics
             WHERE window_start >= now() - make_interval(mins => :mins)
             ORDER BY window_start""", mins=window_min)
    fonte = "live_metrics (Simple Streaming)"

    # Fallback: Spark
    if m.empty:
        m = q("""SELECT window_start, ap_id, building, active_users, event_count,
                        avg_rssi, avg_latency_ms, updated_at
                 FROM ap_metrics_1min
                 WHERE window_start >= now() - make_interval(mins => :mins)
                 ORDER BY window_start""", mins=window_min)
        fonte = "ap_metrics_1min (Spark)"

    if m.empty:
        st.info("Seidauk iha dadus streaming. Hahú `producer --mode live` no `simple_streaming.py`. "
                "Se mosu error 'column building', halai `DROP TABLE live_metrics;` dala ida no hahú fali consumer.")
        return

    st.success(f"Fonte dadus: **{fonte}** | Total: {len(m)} records")

    # Minutu ikus ne'ebé sei lao seidauk kompletu → uza minutu kompletu ikus ba kartaun
    minutes = sorted(m["window_start"].drop_duplicates())
    cur_min = minutes[-2] if len(minutes) >= 2 else minutes[-1]
    prev_min = minutes[-3] if len(minutes) >= 3 else None
    cur = m[m.window_start == cur_min]
    prev = m[m.window_start == prev_min] if prev_min is not None else m.iloc[0:0]

    def delta(col, agg="sum"):
        if prev.empty:
            return None
        return round(getattr(cur[col], agg)() - getattr(prev[col], agg)(), 1)

    n_alerts = q("SELECT count(*) AS n FROM alerts WHERE created_at >= now() - interval '10 minutes'")
    freshness = (now - pd.to_datetime(m["updated_at"]).max()).total_seconds()

    c = st.columns(6)
    c[0].metric("👥 Uzuáriu ativu", int(cur.active_users.sum()), delta("active_users"))
    c[1].metric("📦 Event/min", f"{cur.event_count.sum():.0f}", delta("event_count"))
    c[2].metric("📡 RSSI médiu", f"{cur.avg_rssi.mean():.1f} dBm", delta("avg_rssi", "mean"))
    c[3].metric("⏱️ Latency médiu", f"{cur.avg_latency_ms.mean():.0f} ms",
                delta("avg_latency_ms", "mean"), delta_color="inverse")
    c[4].metric("🚨 Alerta (10 min)", int(n_alerts.n.iloc[0]) if not n_alerts.empty else 0)
    c[5].metric("🔄 Dadus atualiza", f"{freshness:.0f} s liuba")
    st.caption(f"Kartaun uza minutu kompletu ikus: {pd.Timestamp(cur_min).tz_convert(TZ):%H:%M}")

    chart_df = m[m.window_start <= cur_min]

    left, right = st.columns([2, 1])
    with left:
        by_b = chart_df.groupby(["window_start", "building"], as_index=False)["active_users"].sum()
        fig = px.line(by_b, x="window_start", y="active_users", color="building",
                      color_discrete_map=BUILDING_COLORS, markers=True,
                      labels={"window_start": "Minutu", "active_users": "Uzuáriu", "building": "Edifísiu"},
                      title="Uzuáriu ativu kada minutu tuir edifísiu")
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, key="live_line")
    with right:
        tb = cur.groupby("building", as_index=False)["active_users"].sum().sort_values("active_users")
        fig = px.bar(tb, x="active_users", y="building", orientation="h",
                     labels={"active_users": "Uzuáriu", "building": ""},
                     title="Uzuáriu ativu agora tuir edifísiu",
                     color_discrete_sequence=["#2563eb"])
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=50, b=10))
        st.plotly_chart(fig, key="live_bar")

    st.subheader("🏷️ AP ho beban aas liu (minutu kompletu ikus)")
    top = cur.sort_values("active_users", ascending=False).head(10)
    st.dataframe(top[["ap_id", "building", "active_users", "event_count", "avg_rssi", "avg_latency_ms"]],
                 hide_index=True)

    st.subheader("📜 Live feed — 15 ikus")
    feed = q("""SELECT updated_at, window_start, ap_id, building, user_count, event_count,
                       round(avg_rssi::numeric, 1) AS avg_rssi, round(avg_latency::numeric, 1) AS avg_latency
                FROM live_metrics ORDER BY updated_at DESC LIMIT 15""")
    if not feed.empty:
        st.dataframe(feed, hide_index=True)


with tab_live:
    st.fragment(run_every=refresh)(live_panel)()


# =====================================================================
# TAB 2 — ANALITIKA ISTÓRIKU
# =====================================================================
def hist_panel():
    live = q("""SELECT COUNT(*) AS total, COALESCE(SUM(event_count), 0) AS events,
                       COUNT(DISTINCT ap_id) AS aps, AVG(avg_rssi) AS rssi, AVG(avg_latency) AS lat
                FROM live_metrics""")
    if live.empty or live.total.iloc[0] == 0:
        st.info("Seidauk iha live_metrics")
        return

    summary = q("SELECT category, metric, value, run_at FROM analytics_summary")
    uniq, run_at = "-", "-"
    if not summary.empty:
        u = summary[(summary.category == "overall") & (summary.metric == "unique_users")]
        if not u.empty:
            uniq = f"{u.value.iloc[0]:,.0f}"
        run_at = f"{summary.run_at.max():%Y-%m-%d %H:%M}"

    c = st.columns(5)
    c[0].metric("Total evento (Live)", f"{live.events.iloc[0]:,.0f}")
    c[1].metric("AP Ativu", f"{live.aps.iloc[0]}")
    c[2].metric("RSSI médiu", f"{live.rssi.iloc[0]:.1f} dBm")
    c[3].metric("Latency médiu", f"{live.lat.iloc[0]:.0f} ms")
    c[4].metric("Uzuáriu úniku (Batch)", uniq)
    st.caption(f"Batch ikus halai: {run_at} | Live records: {live.total.iloc[0]}")

    df = q("""SELECT window_start, building, SUM(user_count) AS user_count
              FROM live_metrics
              WHERE window_start >= now() - interval '24 hours'
              GROUP BY window_start, building ORDER BY window_start""")
    if not df.empty:
        fig = px.line(df, x="window_start", y="user_count", color="building",
                      color_discrete_map=BUILDING_COLORS,
                      title="Trend Live - Uzuáriu kada edifísiu (24 oras ikus)")
        st.plotly_chart(fig, key="hist_trend")
        st.dataframe(df.tail(20), hide_index=True)


with tab_hist:
    if st.button("🔄 Karrega fali rezultadu batch"):
        st.rerun()
    hist_panel()


# =====================================================================
# TAB 3 — KUALIDADE DADUS
# =====================================================================
def dq_panel():
    t = q("""SELECT COALESCE(sum(received),0) AS received, COALESCE(sum(inserted),0) AS inserted,
                    COALESCE(sum(invalid),0) AS invalid, COALESCE(sum(duplicates),0) AS duplicates
             FROM dq_ingest_log""")
    if t.empty or t.received.iloc[0] == 0:
        cnt = q("SELECT COUNT(*) AS n FROM live_metrics")
        st.metric("Total Live Records", int(cnt.n.iloc[0]) if not cnt.empty else 0)
        st.info("Seidauk iha log ingestaun (dq_ingest_log). Dadus live_metrics iha ona.")
        return
    r = t.iloc[0]
    rec = float(r.received)
    c = st.columns(4)
    c[0].metric("Record simu", f"{rec:,.0f}")
    c[1].metric("Validity", f"{100 * (1 - r.invalid / rec):.2f} %")
    c[2].metric("Uniqueness", f"{100 * (1 - r.duplicates / rec):.2f} %")


with tab_dq:
    st.fragment(run_every=refresh)(dq_panel)()
