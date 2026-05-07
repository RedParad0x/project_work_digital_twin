"""
pages/3_Confronto.py  —  Confronto MongoDB vs Neo4j
=====================================================
La pagina più importante per la tesina.
Stessa domanda → due database → due risposte diverse.
Mostra esplicitamente i punti di forza di ciascun DB.
"""

import os
import time
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

st.set_page_config(page_title="Confronto DB", page_icon="⚖️", layout="wide")
load_dotenv()

MONGO_URI      = os.getenv("MONGO_URI",      "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME  = os.getenv("MONGO_DB_NAME",  "media_twin")
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mediatwin1")

COMPANIES = ["NVDA","TSLA","AAPL","META","GOOGL","MSFT","DIS","NFLX","AMZN","JPM","PYPL","COIN","XOM","BA","RACE"]

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

# ════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("⚖️ Confronto DB")
    st.divider()
    ticker   = st.selectbox("Azienda", COMPANIES, index=0)
    days     = st.slider("Finestra temporale (giorni)", 1, 30, 7)
    st.divider()
    run_btn  = st.button("🚀 Avvia confronto", type="primary", use_container_width=True)
    st.divider()
    st.caption(
        "Questa pagina interroga **entrambi** i database con la stessa domanda "
        "e mostra le differenze di risposta, evidenziando i punti di forza di ciascuno."
    )

# ════════════════════════════════════════════════════════════════
# HEADER + SPIEGAZIONE
# ════════════════════════════════════════════════════════════════
st.title("⚖️ MongoDB vs Neo4j — Confronto diretto")
st.markdown(f"""
**Domanda:** *Cosa sappiamo di `{ticker}` negli ultimi `{days}` giorni?*

Due database, stessa domanda, risposte complementari.
""")

st.divider()

# ── Schema visivo dell'architettura ──────────────────────────────────────────
with st.expander("📐 Architettura Dual-Write — come funziona", expanded=False):
    st.markdown("""
    ```
    Pipeline di Ingestion
            │
            ▼
    ┌─────────────────────────────────────┐
    │         ingestion_layer.py          │
    │   dual_write(source, payload)       │
    └───────────┬─────────────────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
    ┌─────────┐    ┌──────────┐
    │ MongoDB │    │  Neo4j   │
    │ raw_data│    │  Graph   │
    └─────────┘    └──────────┘
    Document       Relationship
    Store          Database
    Forza:         Forza:
    - Volume       - Connessioni
    - Schema       - Pattern
      flessibile   - Traversal
    - Aggregazioni - Co-menzioni
      veloci       - Gerarchia
    ```

    **Ogni documento scritto in MongoDB** ottiene un `mongo_id` (UUID) usato come
    chiave di coerenza nel nodo Neo4j corrispondente. In caso di fallimento Neo4j,
    MongoDB esegue un rollback automatico.
    """)

if not run_btn:
    st.info("👈 Seleziona un'azienda e premi **Avvia confronto** nella sidebar.")
    st.stop()

col_db, err_m = get_mongo()
drv,    err_n = get_neo4j()

if err_m:
    st.error(f"MongoDB: {err_m}")
if err_n:
    st.error(f"Neo4j: {err_n}")
if err_m or err_n:
    st.stop()

since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

st.divider()
st.subheader(f"Risultati per **{ticker}** — ultimi {days} giorni")

left, right = st.columns(2)

# ════════════════════════════════════════════════════════════════
# COLONNA SINISTRA — MONGODB
# ════════════════════════════════════════════════════════════════
with left:
    st.markdown("### 📦 MongoDB")
    st.caption("Document Store — risponde bene a: *quanti? con quale distribuzione? nel tempo?*")

    t0 = time.perf_counter()

    # 1. Totale documenti
    total = col_db.count_documents({
        "payload.mentions": ticker,
        "ingested_at": {"$gte": since},
    })

    # 2. Per sorgente
    by_src = list(col_db.aggregate([
        {"$match": {"payload.mentions": ticker, "ingested_at": {"$gte": since}}},
        {"$group": {"_id": "$source", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
    ]))

    # 3. Sentiment aggregato
    sentiment = list(col_db.aggregate([
        {"$match": {
            "payload.mentions": ticker,
            "payload.sentiment_label": {"$exists": True},
            "ingested_at": {"$gte": since},
        }},
        {"$group": {
            "_id": "$payload.sentiment_label",
            "n": {"$sum": 1},
            "avg": {"$avg": "$payload.sentiment_score"},
        }},
    ]))

    # 4. Serie temporale
    time_series = list(col_db.aggregate([
        {"$match": {"payload.mentions": ticker, "ingested_at": {"$gte": since}}},
        {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
        {"$group": {"_id": "$day", "n": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]))

    elapsed_m = round((time.perf_counter() - t0) * 1000, 1)

    # KPI
    k1, k2, k3 = st.columns(3)
    k1.metric("Documenti trovati", f"{total:,}")
    sent_pos = next((r["n"] for r in sentiment if r["_id"] == "positive"), 0)
    sent_neg = next((r["n"] for r in sentiment if r["_id"] == "negative"), 0)
    k2.metric("Sentiment positivo", sent_pos)
    k3.metric("Sentiment negativo", sent_neg)

    st.caption(f"⏱ Query MongoDB completata in **{elapsed_m} ms**")

    # Per sorgente
    if by_src:
        df_src = pd.DataFrame([{"Sorgente": r["_id"], "Documenti": r["n"]} for r in by_src])
        fig = px.bar(df_src, x="Sorgente", y="Documenti",
                     color_discrete_sequence=["#1a6bbd"], height=220)
        fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

    # Sentiment donut
    if sentiment:
        df_sent = pd.DataFrame([{"Label": r["_id"], "n": r["n"], "avg": round(r["avg"] or 0, 3)}
                                 for r in sentiment])
        color_map = {"positive":"#16a34a","negative":"#dc2626","neutral":"#ca8a04"}
        fig2 = px.pie(df_sent, names="Label", values="n", hole=0.5, height=220,
                      color="Label", color_discrete_map=color_map)
        fig2.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, use_container_width=True)
        st.dataframe(df_sent.rename(columns={"Label":"Sentiment","n":"Conteggio","avg":"Score medio"}),
                     use_container_width=True, hide_index=True)

    # Serie temporale
    if time_series:
        df_ts = pd.DataFrame([{"Giorno": r["_id"], "Documenti": r["n"]} for r in time_series])
        fig3 = px.line(df_ts, x="Giorno", y="Documenti", markers=True, height=200,
                       color_discrete_sequence=["#1a6bbd"])
        fig3.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig3, use_container_width=True)

    # Ciò che MongoDB fa BENE qui
    st.success("""
    ✅ **Punti di forza MongoDB in questo contesto:**
    - Aggregazioni veloci su grandi volumi di documenti
    - Schema flessibile: GDELT, Reddit, NewsAPI nella stessa collezione
    - Serie temporale immediata con `$group` su substring di data
    - Sentiment medio calcolato con `$avg` in una sola pipeline
    """)

# ════════════════════════════════════════════════════════════════
# COLONNA DESTRA — NEO4J
# ════════════════════════════════════════════════════════════════
with right:
    st.markdown("### 🕸️ Neo4j")
    st.caption("Graph Database — risponde bene a: *con chi è connessa? quali pattern esistono? quanto è centrale?*")

    t0 = time.perf_counter()

    with drv.session() as s:

        # 1. Menzioni totali
        res_total = s.run("""
            MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker: $t})
            RETURN count(e) AS n
        """, t=ticker).single()
        neo_total = res_total["n"] if res_total else 0

        # 2. Co-menzioni (chi viene citato insieme?)
        comentions = s.run("""
            MATCH (c1:Company {ticker: $t})<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
            WHERE c2.ticker <> $t
            RETURN c2.ticker AS Azienda, count(e) AS CoMenzioni
            ORDER BY CoMenzioni DESC LIMIT 10
        """, t=ticker).data()

        # 3. Topic trending collegati
        topics = s.run("""
            MATCH (c:Company {ticker: $t})-[:TRENDING_WITH]->(top:Topic)
            RETURN top.name AS Topic
            LIMIT 15
        """, t=ticker).data()

        # 4. Distribuzione per sorgente (via nodi Event)
        neo_src = s.run("""
            MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker: $t})
            RETURN e.source AS Sorgente, count(e) AS n
            ORDER BY n DESC
        """, t=ticker).data()

        # 5. Gerarchia
        hierarchy = s.run("""
            MATCH (c:Company {ticker: $t})-[:BELONGS_TO]->(i:Industry)-[:PART_OF]->(s:Sector)
            RETURN c.name AS Azienda, i.name AS Industria, s.name AS Settore
        """, t=ticker).single()

        # 6. Sentiment via nodi Event Neo4j
        neo_sent = s.run("""
            MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker: $t})
            WHERE e.sentiment_label IS NOT NULL
            RETURN e.sentiment_label AS Label, count(e) AS n
            ORDER BY n DESC
        """, t=ticker).data()

    elapsed_n = round((time.perf_counter() - t0) * 1000, 1)

    # KPI
    k1, k2, k3 = st.columns(3)
    k1.metric("Nodi Event collegati", f"{neo_total:,}")
    k2.metric("Aziende co-menzionate", len(comentions))
    k3.metric("Topic trending", len(topics))

    st.caption(f"⏱ Query Neo4j completata in **{elapsed_n} ms**")

    # Co-menzioni — questo è il valore unico di Neo4j
    if comentions:
        st.markdown("**Aziende co-menzionate** *(appaiono nello stesso evento)*")
        df_co = pd.DataFrame(comentions)
        fig = px.bar(df_co, x="Azienda", y="CoMenzioni",
                     color_discrete_sequence=["#7c3aed"], height=220)
        fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

    # Topic trending
    if topics:
        topic_list = [r["Topic"] for r in topics]
        st.markdown("**Topic trending collegati**")
        st.write("  ·  ".join([f"`{t}`" for t in topic_list]))
    else:
        st.info("Nessun topic — avvia ingest_pytrends.py")

    # Gerarchia
    if hierarchy:
        st.markdown("**Posizione nel grafo**")
        st.markdown(
            f"`{ticker}` → Industry: `{hierarchy['Industria']}` → Sector: `{hierarchy['Settore']}`"
        )

    # Sentiment Neo4j
    if neo_sent:
        df_ns = pd.DataFrame(neo_sent)
        color_map = {"positive":"#16a34a","negative":"#dc2626","neutral":"#ca8a04"}
        fig4 = px.pie(df_ns, names="Label", values="n", hole=0.5, height=220,
                      color="Label", color_discrete_map=color_map)
        fig4.update_layout(margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig4, use_container_width=True)

    # Ciò che Neo4j fa BENE qui
    st.success("""
    ✅ **Punti di forza Neo4j in questo contesto:**
    - Traversal immediato: trova co-menzioni con una sola `MATCH` a 3 hop
    - Relazioni esplicite: impossibile fare questo efficacemente in MongoDB
    - Topic collegati tramite archi `[:TRENDING_WITH]`
    - Gerarchia Company → Industry → Sector navigabile in O(1) per nodo
    """)

# ════════════════════════════════════════════════════════════════
# SEZIONE FINALE — QUANDO USARE QUALE
# ════════════════════════════════════════════════════════════════
st.divider()
st.subheader("📚 Quando usare quale database — sintesi per la tesina")

comparison_data = {
    "Domanda": [
        "Quanti articoli su TSLA questa settimana?",
        "Qual è il sentiment medio per ticker?",
        "Quali aziende vengono citate insieme a TSLA?",
        "Com'è cambiato il volume nel tempo?",
        "Quanto è 'centrale' AAPL nel grafo delle notizie?",
        "Quali topic trending sono correlati a NVDA?",
        "Mostrami il documento grezzo di un evento GDELT",
        "Qual è la gerarchia settore → industria → azienda?",
    ],
    "MongoDB ✅": [
        "✅ count_documents()",
        "✅ $avg in aggregation pipeline",
        "❌ Richiede join complessi",
        "✅ $group per giorno",
        "❌ Non ha concetto di centralità",
        "❌ Non ha relazioni esplicite",
        "✅ find_one() immediato",
        "❌ Richiede documenti separati",
    ],
    "Neo4j ✅": [
        "✅ MATCH count()",
        "✅ AVG(e.sentiment_score)",
        "✅ Pattern a 3 hop in una riga",
        "✅ Con range di date",
        "✅ Grado di ingresso nativo",
        "✅ MATCH -[:TRENDING_WITH]->",
        "❌ Nodi Neo4j non hanno il payload grezzo",
        "✅ MATCH -[:BELONGS_TO]-[:PART_OF]->",
    ],
    "Vincitore": [
        "Entrambi", "Entrambi", "Neo4j", "Entrambi",
        "Neo4j", "Neo4j", "MongoDB", "Neo4j",
    ],
}

df_comp = pd.DataFrame(comparison_data)
st.dataframe(df_comp, use_container_width=True, hide_index=True, height=320)

st.info("""
💡 **Conclusione architetturale:** MongoDB e Neo4j non si sostituiscono — si complementano.
MongoDB gestisce il volume grezzo e le aggregazioni temporali.
Neo4j gestisce le connessioni semantiche tra entità.
Il `dual_write` in `ingestion_layer.py` garantisce coerenza tra i due tramite `mongo_id` (UUID4).
""")
