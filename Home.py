"""
Home.py  —  Media Digital Twin Dashboard
=========================================
Avvio: streamlit run Home.py

Pagina principale: stato sistema, KPI globali,
volume ingestion ultimi 7 giorni, ultimo run pipeline.
"""

import glob
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from pymongo import MongoClient
from pymongo.errors import PyMongoError

st.set_page_config(page_title="Media Digital Twin", page_icon="🌐", layout="wide")
load_dotenv()

MONGO_URI      = os.getenv("MONGO_URI",      "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME  = os.getenv("MONGO_DB_NAME",  "media_twin")
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mediatwin1")
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR       = os.path.join(BASE_DIR, "..", "logs")

PIPELINE_META = [
    {"key": "yfinance",      "label": "yfinance",  "emoji": "📈", "log_dir": "ingestion_yfinance"},
    {"key": "gdelt",         "label": "gdelt",     "emoji": "🌍", "log_dir": "ingestion_gdelt"},
    {"key": "newsapi",       "label": "newsapi",   "emoji": "📰", "log_dir": "ingestion_newsapi"},
    {"key": "reddit",        "label": "reddit",    "emoji": "💬", "log_dir": "ingestion_reddit"},
    {"key": "youtube",       "label": "youtube",   "emoji": "▶️",  "log_dir": "ingestion_youtube"},
    {"key": "google_trends", "label": "pytrends",  "emoji": "🔍", "log_dir": "ingestion_pytrends"},
    {"key": "nlp",           "label": "nlp",       "emoji": "🧠", "log_dir": "nlp_layer"},
]

# ── Connessioni ──────────────────────────────────────────────────────────────

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

def last_log_time(log_dir_name):
    path  = os.path.join(LOGS_DIR, log_dir_name)
    files = sorted(glob.glob(os.path.join(path, "*.log")), reverse=True) if os.path.isdir(path) else []
    if not files:
        return "Mai eseguita"
    return datetime.fromtimestamp(os.path.getmtime(files[0])).strftime("%d/%m %H:%M")

@st.cache_data(ttl=30)
def load_kpis():
    col, err = get_mongo()
    if col is None:
        return None, err
    try:
        total   = col.count_documents({})
        by_src  = list(col.aggregate([{"$group": {"_id": "$source", "n": {"$sum": 1}}}]))
        pending = col.count_documents({
            "source":                  {"$nin": ["yfinance", "google_trends"]},
            "payload.sentiment_label": {"$exists": False},
        })
        since  = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        by_day = list(col.aggregate([
            {"$match": {"ingested_at": {"$gte": since}}},
            {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
            {"$group": {"_id": {"day": "$day", "src": "$source"}, "n": {"$sum": 1}}},
            {"$sort": {"_id.day": 1}},
        ]))
        return {"total": total, "by_src": by_src, "pending": pending, "by_day": by_day}, None
    except PyMongoError as e:
        return None, str(e)

@st.cache_data(ttl=30)
def load_neo4j_kpis():
    d, err = get_neo4j()
    if d is None:
        return None, err
    try:
        with d.session() as s:
            counts = {r["l"]: r["n"] for r in s.run(
                "MATCH (n) RETURN labels(n)[0] AS l, count(n) AS n").data()}
            rels = s.run("MATCH ()-[r]->() RETURN count(r) AS n").single()["n"]
        return {"counts": counts, "rels": rels}, None
    except Neo4jError as e:
        return None, str(e)

# ════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("🌐 Media Digital Twin")
    st.caption("Mazzini & Rossi · Big Data")
    st.divider()
    if st.button("🔄 Aggiorna dati", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ════════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════════
st.title("🌐 Media Digital Twin")
st.caption(f"Sistema di monitoraggio real-time · {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
st.divider()

col_db, err_m = get_mongo()
drv,    err_n = get_neo4j()
kpis,   _     = load_kpis()
nkpis,  _     = load_neo4j_kpis()

# ── Stato DB ─────────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)
with c1:
    st.subheader("MongoDB")
    if col_db is not None:
        st.success("● Connesso")
        st.caption(MONGO_URI[:45] + "…")
    else:
        st.error("● Offline")
        st.code("docker compose up -d", language="bash")

with c2:
    st.subheader("Neo4j")
    if drv:
        st.success("● Connesso")
        st.caption(NEO4J_URI)
    else:
        st.error("● Offline")
        st.code("docker compose up -d", language="bash")

with c3:
    st.subheader("NLP Queue")
    pending = kpis["pending"] if kpis else 0
    if pending == 0:
        st.success(f"● {pending} documenti in coda")
    else:
        st.warning(f"● {pending} documenti in coda")
    st.caption("Documenti senza sentiment_label")

st.divider()

# ── KPI ──────────────────────────────────────────────────────────────────────
st.subheader("KPI Globali")
k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Documenti totali",  f"{kpis['total']:,}"                          if kpis  else "—")
k2.metric("Nodi Event",        f"{nkpis['counts'].get('Event',0):,}"          if nkpis else "—")
k3.metric("Nodi Company",      f"{nkpis['counts'].get('Company',0)}"          if nkpis else "—")
k4.metric("Nodi Topic",        f"{nkpis['counts'].get('Topic',0)}"            if nkpis else "—")
k5.metric("Relazioni Neo4j",   f"{nkpis['rels']:,}"                          if nkpis else "—")
k6.metric("In coda NLP",       f"{kpis['pending']:,}"                         if kpis  else "—")

st.divider()

# ── Volume 7 giorni ───────────────────────────────────────────────────────────
st.subheader("Volume ingestion — ultimi 7 giorni")
if kpis and kpis["by_day"]:
    df = pd.DataFrame([
        {"Giorno": r["_id"]["day"], "Sorgente": r["_id"]["src"], "Documenti": r["n"]}
        for r in kpis["by_day"]
    ])
    fig = px.bar(df, x="Giorno", y="Documenti", color="Sorgente",
                 barmode="stack", height=320,
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Nessun dato negli ultimi 7 giorni — avvia almeno una pipeline.")

st.divider()

# ── Stato pipeline da log ────────────────────────────────────────────────────
st.subheader("Ultimo run per pipeline")
mongo_counts = {r["_id"]: r["n"] for r in (kpis["by_src"] if kpis else [])}
cols = st.columns(len(PIPELINE_META))
for i, p in enumerate(PIPELINE_META):
    with cols[i]:
        n = mongo_counts.get(p["key"], 0)
        st.markdown(f"**{p['emoji']} {p['label']}**")
        st.caption(f"Run: {last_log_time(p['log_dir'])}")
        if p["key"] != "nlp":
            st.caption(f"Docs: {n:,}")