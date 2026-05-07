"""
pages/4_Aziende.py  —  Analisi per azienda
===========================================
Dashboard completa per singola azienda:
  - Prezzi OHLCV interattivi
  - Sentiment nel tempo da tutte le sorgenti
  - Topic trending Google Trends
  - Ultimi eventi con link cliccabile
  - Co-menzioni con altre aziende
"""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from pymongo import MongoClient
from pymongo.errors import PyMongoError

st.set_page_config(page_title="Analisi Aziende", page_icon="🏢", layout="wide")
load_dotenv()

MONGO_URI      = os.getenv("MONGO_URI",      "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME  = os.getenv("MONGO_DB_NAME",  "media_twin")
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mediatwin1")

COMPANIES = {
    "NVDA":"NVIDIA",  "TSLA":"Tesla",    "AAPL":"Apple",
    "META":"Meta",    "GOOGL":"Google",  "MSFT":"Microsoft",
    "DIS":"Disney",   "NFLX":"Netflix",  "AMZN":"Amazon",
    "JPM":"JPMorgan", "PYPL":"PayPal",   "COIN":"Coinbase",
    "XOM":"Exxon",    "BA":"Boeing",     "RACE":"Ferrari",
}

@st.cache_resource(ttl=30)
def get_mongo():
    try:
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        c.server_info()
        return c[MONGO_DB_NAME]["raw_data"], None
    except PyMongoError as e:
        return None, str(e)

@st.cache_resource(ttl=30)
def get_neo4j():
    try:
        d = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        d.verify_connectivity()
        return d, None
    except Exception as e:
        return None, str(e)

# ── Query ──────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_ohlcv(ticker, limit=96):
    col, _ = get_mongo()
    if col is None:
        return []
    docs = list(col.find(
        {"source": "yfinance", "data_type": "ohlcv", "payload.ticker": ticker},
        {"payload": 1}
    ).sort("payload.timestamp", -1).limit(limit))
    rows = []
    for d in docs:
        p = d["payload"]
        rows.append({
            "timestamp": p.get("timestamp",""),
            "open":   p.get("open",  0),
            "high":   p.get("high",  0),
            "low":    p.get("low",   0),
            "close":  p.get("close", 0),
            "volume": p.get("volume",0),
        })
    return sorted(rows, key=lambda x: x["timestamp"])

@st.cache_data(ttl=60)
def load_sentiment_timeline(ticker, days):
    col, _ = get_mongo()
    if col is None:
        return []
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    return list(col.aggregate([
        {"$match": {
            "payload.mentions": ticker,
            "payload.sentiment_label": {"$exists": True},
            "ingested_at": {"$gte": since},
        }},
        {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
        {"$group": {
            "_id": {"day": "$day", "source": "$source", "label": "$payload.sentiment_label"},
            "n":   {"$sum": 1},
            "avg": {"$avg": "$payload.sentiment_score"},
        }},
        {"$sort": {"_id.day": 1}},
    ]))

@st.cache_data(ttl=60)
def load_recent_events(ticker, source, limit=30):
    col, _ = get_mongo()
    if col is None:
        return []
    q = {"payload.mentions": ticker}
    if source != "Tutte":
        q["source"] = source
    docs = list(col.find(q, {
        "source": 1, "ingested_at": 1,
        "payload.title": 1, "payload.url": 1,
        "payload.sentiment_label": 1, "payload.sentiment_score": 1,
        "payload.publisher": 1, "payload.source_name": 1,
    }).sort("ingested_at", -1).limit(limit))
    rows = []
    for d in docs:
        p = d.get("payload", {})
        rows.append({
            "Sorgente":  d.get("source",""),
            "Titolo":    (p.get("title") or "—")[:80],
            "URL":       p.get("url",""),
            "Sentiment": p.get("sentiment_label") or "—",
            "Score":     round(p.get("sentiment_score") or 0, 3),
            "Data":      (d.get("ingested_at",""))[:16].replace("T"," "),
        })
    return rows

@st.cache_data(ttl=60)
def load_neo4j_data(ticker):
    drv, _ = get_neo4j()
    if drv is None:
        return {}, {}, [], []
    try:
        with drv.session() as s:
            info = s.run("""
                MATCH (c:Company {ticker:$t})
                OPTIONAL MATCH (c)-[:BELONGS_TO]->(i:Industry)-[:PART_OF]->(sec:Sector)
                RETURN c.name AS name, c.last_close AS close,
                       c.last_volume AS volume, c.updated_at AS updated,
                       i.name AS industry, sec.name AS sector
            """, t=ticker).single()

            comentions = s.run("""
                MATCH (c1:Company {ticker:$t})<-[:MENZIONATA_IN]-(e:Event)
                      -[:MENZIONATA_IN]->(c2:Company)
                WHERE c2.ticker <> $t
                RETURN c2.ticker AS ticker, count(e) AS n
                ORDER BY n DESC LIMIT 10
            """, t=ticker).data()

            topics = s.run("""
                MATCH (c:Company {ticker:$t})-[:TRENDING_WITH]->(t:Topic)
                RETURN t.name AS topic LIMIT 20
            """, t=ticker).data()

            neo_sent = s.run("""
                MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker:$t})
                WHERE e.sentiment_label IS NOT NULL
                RETURN e.source AS src, e.sentiment_label AS label, count(e) AS n
                ORDER BY n DESC
            """, t=ticker).data()

        return (
            dict(info) if info else {},
            comentions,
            [r["topic"] for r in topics],
            neo_sent,
        )
    except Neo4jError:
        return {}, [], [], []

# ════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("🏢 Analisi Aziende")
    st.divider()
    ticker = st.selectbox(
        "Azienda",
        list(COMPANIES.keys()),
        format_func=lambda t: f"{t} — {COMPANIES[t]}",
    )
    days       = st.slider("Finestra temporale (giorni)", 1, 30, 7)
    src_filter = st.selectbox("Sorgente eventi", ["Tutte","gdelt","newsapi","reddit","youtube"])
    st.divider()
    if st.button("🔄 Aggiorna", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ════════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════════
neo_info, comentions, topics, neo_sent = load_neo4j_data(ticker)
name = neo_info.get("name") or COMPANIES.get(ticker, ticker)

st.title(f"🏢 {ticker} — {name}")
if neo_info.get("industry"):
    st.caption(f"**Industria:** {neo_info['industry']}  ·  **Settore:** {neo_info.get('sector','—')}")
st.divider()

# ── KPI rapidi ────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("Ultimo prezzo",  f"${neo_info['close']:.2f}"    if neo_info.get("close")  else "—")
k2.metric("Volume",         f"{neo_info['volume']:,}"       if neo_info.get("volume") else "—")
k3.metric("Co-menzionato con", f"{len(comentions)} aziende")
k4.metric("Topic trending", len(topics))

st.divider()

# ════════════════════════════════════════════════════════════════
# OHLCV
# ════════════════════════════════════════════════════════════════
st.subheader("📈 Prezzi OHLCV")
ohlcv = load_ohlcv(ticker)

if ohlcv:
    df_ohlcv = pd.DataFrame(ohlcv)
    tab_line, tab_candle = st.tabs(["Linea", "Candlestick"])
    with tab_line:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_ohlcv["timestamp"], y=df_ohlcv["close"],
                                  mode="lines", name="Close",
                                  line=dict(color="#1a6bbd", width=2)))
        fig.add_trace(go.Bar(x=df_ohlcv["timestamp"], y=df_ohlcv["volume"],
                              name="Volume", yaxis="y2",
                              marker_color="rgba(26,107,189,0.2)"))
        fig.update_layout(
            height=340, margin=dict(l=0,r=0,t=10,b=0),
            yaxis=dict(title="Prezzo (USD)"),
            yaxis2=dict(title="Volume", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab_candle:
        fig2 = go.Figure(go.Candlestick(
            x=df_ohlcv["timestamp"],
            open=df_ohlcv["open"], high=df_ohlcv["high"],
            low=df_ohlcv["low"],   close=df_ohlcv["close"],
        ))
        fig2.update_layout(height=340, margin=dict(l=0,r=0,t=10,b=0),
                            xaxis_rangeslider_visible=False)
        st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Nessun dato OHLCV — avvia `ingest_yfinance.py` con mercato aperto.")

st.divider()

# ════════════════════════════════════════════════════════════════
# SENTIMENT NEL TEMPO
# ════════════════════════════════════════════════════════════════
st.subheader("📊 Sentiment nel tempo")
sent_data = load_sentiment_timeline(ticker, days)

if sent_data:
    rows = [{"Giorno": r["_id"]["day"], "Sorgente": r["_id"]["source"],
             "Sentiment": r["_id"]["label"], "n": r["n"], "avg": round(r["avg"] or 0, 3)}
            for r in sent_data]
    df_s = pd.DataFrame(rows)

    tab_s1, tab_s2 = st.tabs(["Per giorno e sentiment", "Per sorgente"])
    with tab_s1:
        color_map = {"positive":"#16a34a","negative":"#dc2626","neutral":"#ca8a04"}
        df_agg = df_s.groupby(["Giorno","Sentiment"])["n"].sum().reset_index()
        fig = px.bar(df_agg, x="Giorno", y="n", color="Sentiment",
                     barmode="stack", height=280,
                     color_discrete_map=color_map,
                     labels={"n":"Documenti"})
        fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

    with tab_s2:
        fig2 = px.line(df_s, x="Giorno", y="n", color="Sorgente", markers=True,
                       height=280, labels={"n":"Documenti"})
        fig2.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Nessun dato sentiment — avvia le pipeline di ingestion.")

st.divider()

# ════════════════════════════════════════════════════════════════
# CO-MENZIONI + TOPIC
# ════════════════════════════════════════════════════════════════
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("🔗 Co-menzioni")
    if comentions:
        df_co = pd.DataFrame(comentions).rename(columns={"ticker":"Azienda","n":"Co-menzioni"})
        fig = px.bar(df_co, x="Co-menzioni", y="Azienda", orientation="h",
                     color="Co-menzioni", color_continuous_scale="Blues", height=320)
        fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nessuna co-menzione trovata.")

with col_b:
    st.subheader("🔍 Topic trending")
    if topics:
        st.markdown("**Google Trends — query correlate:**")
        for i, t in enumerate(topics, 1):
            st.markdown(f"{i}. `{t}`")
    else:
        st.info("Nessun topic — avvia `ingest_pytrends.py`.")

    if neo_sent:
        st.markdown("**Sentiment per sorgente (Neo4j)**")
        df_ns = pd.DataFrame(neo_sent).rename(
            columns={"src":"Sorgente","label":"Sentiment","n":"Conteggio"})
        st.dataframe(df_ns, use_container_width=True, hide_index=True)

st.divider()

# ════════════════════════════════════════════════════════════════
# EVENTI RECENTI
# ════════════════════════════════════════════════════════════════
st.subheader(f"📰 Ultimi eventi — {ticker}")
events = load_recent_events(ticker, src_filter, limit=50)

if events:
    df_ev = pd.DataFrame(events)

    # Colora sentiment
    def style_sentiment(val):
        colors = {"positive":"background-color:#dcfce7","negative":"background-color:#fee2e2","neutral":"background-color:#fef9c3"}
        return colors.get(val, "")

    # Mostra con link cliccabili
    for _, row in df_ev.iterrows():
        sent_emoji = {"positive":"🟢","negative":"🔴","neutral":"🟡"}.get(row["Sentiment"],"⚪")
        title_part = f"[{row['Titolo']}]({row['URL']})" if row["URL"] else row["Titolo"]
        st.markdown(
            f"{sent_emoji} **{row['Sorgente']}** · {row['Data']} · "
            f"score: `{row['Score']}` — {title_part}"
        )
    st.caption(f"{len(df_ev)} eventi visualizzati")
else:
    st.info("Nessun evento trovato con i filtri selezionati.")
