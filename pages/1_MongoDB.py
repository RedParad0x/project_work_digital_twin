"""
pages/1_MongoDB.py  —  Data Lake Explorer
==========================================
Tre sezioni:
  1. Explorer documenti con filtri (sorgente, data, sentiment, ticker)
  2. JSON Viewer — mostra schema grezzo di un documento (punto forte: schema flessibile)
  3. Aggregazioni — query builder visuale + pipeline Mongo custom
"""

import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

st.set_page_config(page_title="MongoDB Explorer", page_icon="📦", layout="wide")
load_dotenv()

MONGO_URI     = os.getenv("MONGO_URI",     "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "media_twin")

SOURCES   = ["Tutte", "gdelt", "newsapi", "reddit", "youtube", "yfinance", "google_trends"]
SENTIMENTS = ["Tutti", "positive", "neutral", "negative"]
COMPANIES  = ["NVDA","TSLA","AAPL","META","GOOGL","MSFT","DIS","NFLX","AMZN","JPM","PYPL","COIN","XOM","BA","RACE"]

# ── Connessione ───────────────────────────────────────────────────────────────

@st.cache_resource(ttl=30)
def get_col():
    try:
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        c.server_info()
        return c[MONGO_DB_NAME]["raw_data"], None
    except PyMongoError as e:
        return None, str(e)

# ── Query helpers ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def fetch_docs(source, sentiment, ticker, date_from, date_to, limit):
    col, err = get_col()
    if col is None:
        return [], err
    try:
        q = {}
        if source != "Tutte":
            q["source"] = source
        if sentiment != "Tutti":
            q["payload.sentiment_label"] = sentiment
        if ticker != "Tutte":
            q["payload.mentions"] = ticker
        if date_from:
            q.setdefault("ingested_at", {})["$gte"] = date_from.isoformat()
        if date_to:
            q.setdefault("ingested_at", {})["$lte"] = (
                datetime.combine(date_to, datetime.max.time()).isoformat()
            )
        docs = list(col.find(q).sort("ingested_at", -1).limit(limit))
        return docs, None
    except PyMongoError as e:
        return [], str(e)

@st.cache_data(ttl=30)
def fetch_one(source):
    col, _ = get_col()
    if col is None:
        return None
    q = {} if source == "Tutte" else {"source": source}
    return col.find_one(q, sort=[("ingested_at", -1)])

@st.cache_data(ttl=60)
def run_aggregation(pipeline_json: str):
    col, err = get_col()
    if col is None:
        return [], err
    try:
        pipeline = json.loads(pipeline_json)
        result   = list(col.aggregate(pipeline))
        return result, None
    except json.JSONDecodeError as e:
        return [], f"JSON non valido: {e}"
    except PyMongoError as e:
        return [], str(e)

@st.cache_data(ttl=60)
def sentiment_over_time(source, ticker):
    col, _ = get_col()
    if col is None:
        return []
    q = {"payload.sentiment_label": {"$exists": True}}
    if source != "Tutte":
        q["source"] = source
    if ticker != "Tutte":
        q["payload.mentions"] = ticker
    return list(col.aggregate([
        {"$match": q},
        {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
        {"$group": {
            "_id": {"day": "$day", "label": "$payload.sentiment_label"},
            "n": {"$sum": 1}
        }},
        {"$sort": {"_id.day": 1}},
    ]))

# ════════════════════════════════════════════════════════════════
# SIDEBAR filtri
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("📦 MongoDB Explorer")
    st.divider()
    src_filter  = st.selectbox("Sorgente",  SOURCES)
    sent_filter = st.selectbox("Sentiment", SENTIMENTS)
    tick_filter = st.selectbox("Ticker",    ["Tutte"] + COMPANIES)
    st.divider()
    d_from = st.date_input("Data da", value=None)
    d_to   = st.date_input("Data a",  value=None)
    limit  = st.slider("Max documenti", 10, 500, 100)
    st.divider()
    if st.button("🔄 Aggiorna", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

col_db, conn_err = get_col()

st.title("📦 MongoDB — Data Lake Explorer")
st.caption("Esplora i documenti grezzi, analizza la flessibilità dello schema e lancia aggregazioni.")

if conn_err:
    st.error(f"MongoDB non raggiungibile: {conn_err}")
    st.info("Avvia con: `docker compose up -d`")
    st.stop()

st.divider()

# ════════════════════════════════════════════════════════════════
# TAB 1 — EXPLORER
# ════════════════════════════════════════════════════════════════
tab1, tab2, tab3 = st.tabs(["🔍 Explorer documenti", "📄 JSON Viewer", "⚙️ Aggregazioni"])

with tab1:
    docs, err = fetch_docs(src_filter, sent_filter, tick_filter, d_from, d_to, limit)

    if err:
        st.error(err)
    elif not docs:
        st.warning("Nessun documento trovato con i filtri selezionati.")
    else:
        st.caption(f"**{len(docs)}** documenti trovati")

        rows = []
        for d in docs:
            p = d.get("payload", {})
            rows.append({
                "_id":        str(d.get("_id", ""))[:12] + "…",
                "Sorgente":   d.get("source", ""),
                "Tipo":       d.get("data_type", ""),
                "Titolo/Ticker": (p.get("title") or p.get("ticker") or p.get("keyword") or "—")[:70],
                "Mentions":   ", ".join(p.get("mentions", [])) if p.get("mentions") else "—",
                "Sentiment":  p.get("sentiment_label") or "—",
                "Score":      round(p.get("sentiment_score") or 0, 3),
                "Ingested":   (d.get("ingested_at") or "")[:16].replace("T", " "),
            })

        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True, height=420)

        # Mini grafici sotto la tabella
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Distribuzione per sorgente**")
            fig = px.pie(df, names="Sorgente", height=240,
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown("**Distribuzione sentiment**")
            s_counts = df["Sentiment"].value_counts().reset_index()
            s_counts.columns = ["Sentiment", "n"]
            color_map = {"positive": "#16a34a", "negative": "#dc2626", "neutral": "#ca8a04", "—": "#94a3b8"}
            fig2 = px.bar(s_counts, x="Sentiment", y="n", color="Sentiment",
                          color_discrete_map=color_map, height=240)
            fig2.update_layout(margin=dict(l=0,r=0,t=10,b=0), showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

        # Sentiment nel tempo
        st.markdown("**Sentiment nel tempo**")
        sent_time = sentiment_over_time(src_filter, tick_filter)
        if sent_time:
            df_st = pd.DataFrame([
                {"Giorno": r["_id"]["day"], "Label": r["_id"]["label"], "n": r["n"]}
                for r in sent_time
            ])
            fig3 = px.line(df_st, x="Giorno", y="n", color="Label", markers=True,
                           color_discrete_map={"positive": "#16a34a", "negative": "#dc2626", "neutral": "#ca8a04"},
                           height=260)
            fig3.update_layout(margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# TAB 2 — JSON VIEWER
# ════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("""
    **Perché questo è importante**
    MongoDB è un *document store* a schema flessibile: un documento GDELT e uno Reddit
    vivono nella stessa collezione ma hanno strutture completamente diverse.
    Questo è impossibile in un database relazionale senza denormalizzazione forzata.
    """)

    src_json = st.selectbox("Sorgente da visualizzare", SOURCES[1:], key="json_src")
    doc      = fetch_one(src_json)

    if doc:
        doc["_id"] = str(doc["_id"])  # ObjectId non è serializzabile
        col_a, col_b = st.columns([3, 2])
        with col_a:
            st.markdown(f"**Documento grezzo — sorgente: `{src_json}`**")
            st.json(doc, expanded=True)
        with col_b:
            st.markdown("**Struttura del documento**")
            payload_keys = list(doc.get("payload", {}).keys())
            st.markdown(f"- `_id`: `{str(doc['_id'])[:20]}…`")
            st.markdown(f"- `source`: `{doc.get('source','')}`")
            st.markdown(f"- `data_type`: `{doc.get('data_type','')}`")
            st.markdown(f"- `ingested_at`: `{doc.get('ingested_at','')[:16]}`")
            st.markdown(f"- `payload` → **{len(payload_keys)} campi**: `{', '.join(payload_keys)}`")

            st.divider()
            st.markdown("**Confronto campi payload per sorgente**")
            comparison = {
                "gdelt":         ["gdelt_id","url","publisher","published_at","mentions","sentiment_score","sentiment_label"],
                "newsapi":       ["title","description","url","source_name","author","published_at","mentions"],
                "reddit":        ["subreddit","post_id","title","body","score","num_comments","author","created_utc"],
                "youtube":       ["video_id","title","channel","published_at","views","likes","comments"],
                "yfinance":      ["ticker","timestamp","open","high","low","close","volume"],
                "google_trends": ["ticker","keyword","timeframe","interest_data","rising_queries","top_queries"],
            }
            for s, fields in comparison.items():
                with st.expander(f"`{s}` — {len(fields)} campi"):
                    st.code("\n".join(f"payload.{f}" for f in fields))
    else:
        st.info(f"Nessun documento trovato per sorgente `{src_json}`.")

# ════════════════════════════════════════════════════════════════
# TAB 3 — AGGREGAZIONI
# ════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("**Query builder — aggregazioni predefinite**")

    PRESET_PIPELINES = {
        "Documenti per sorgente": json.dumps([
            {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ], indent=2),

        "Sentiment medio per sorgente": json.dumps([
            {"$match": {"payload.sentiment_score": {"$exists": True}}},
            {"$group": {
                "_id": "$source",
                "avg_score": {"$avg": "$payload.sentiment_score"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"avg_score": -1}}
        ], indent=2),

        "Top 10 ticker più menzionati": json.dumps([
            {"$unwind": "$payload.mentions"},
            {"$group": {"_id": "$payload.mentions", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ], indent=2),

        "Documenti per giorno (ultimi 14gg)": json.dumps([
            {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
            {"$group": {"_id": "$day", "count": {"$sum": 1}}},
            {"$sort": {"_id": -1}},
            {"$limit": 14}
        ], indent=2),

        "Distribuzione sentiment per ticker": json.dumps([
            {"$match": {"payload.mentions": {"$exists": True}, "payload.sentiment_label": {"$exists": True}}},
            {"$unwind": "$payload.mentions"},
            {"$group": {
                "_id": {"ticker": "$payload.mentions", "label": "$payload.sentiment_label"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 30}
        ], indent=2),
    }

    preset = st.selectbox("Seleziona aggregazione predefinita", list(PRESET_PIPELINES.keys()))
    pipeline_str = st.text_area(
        "Pipeline MongoDB (JSON)",
        value=PRESET_PIPELINES[preset],
        height=200,
    )
    st.caption("Puoi modificare la pipeline direttamente nel campo sopra.")

    if st.button("▶ Esegui aggregazione", type="primary"):
        with st.spinner("Esecuzione in corso…"):
            result, err = run_aggregation(pipeline_str)

        if err:
            st.error(f"Errore: {err}")
        elif not result:
            st.warning("La query non ha restituito risultati.")
        else:
            # Appiattisci _id nested
            flat = []
            for r in result:
                row = {}
                for k, v in r.items():
                    if k == "_id" and isinstance(v, dict):
                        for sk, sv in v.items():
                            row[sk] = sv
                    else:
                        row[k] = round(v, 4) if isinstance(v, float) else v
                flat.append(row)

            df_res = pd.DataFrame(flat)
            st.dataframe(df_res, use_container_width=True, hide_index=True)

            # Grafico automatico se ci sono colonne numeriche
            num_cols = df_res.select_dtypes(include="number").columns.tolist()
            str_cols = df_res.select_dtypes(include="object").columns.tolist()
            if num_cols and str_cols:
                fig = px.bar(df_res, x=str_cols[0], y=num_cols[0],
                             color_discrete_sequence=["#1a6bbd"], height=300)
                fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig, use_container_width=True)
