"""
pages/2_Neo4j.py  —  Knowledge Graph Explorer
==============================================
Tre sezioni:
  1. Statistiche nodi e relazioni
  2. Query Cypher runner (predefinite + custom)
  3. Rete co-menzioni tra aziende (grafico interattivo)
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

st.set_page_config(page_title="Neo4j Explorer", page_icon="🕸️", layout="wide")
load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mediatwin1")

# ── Connessione ───────────────────────────────────────────────────────────────

@st.cache_resource(ttl=30)
def get_driver():
    try:
        d = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        d.verify_connectivity()
        return d, None
    except Exception as e:
        return None, str(e)

def run_cypher(query: str, params: dict = None):
    d, err = get_driver()
    if d is None:
        return [], err
    try:
        with d.session() as s:
            result = s.run(query, **(params or {}))
            return result.data(), None
    except Neo4jError as e:
        return [], str(e)

# ── Query predefinite ─────────────────────────────────────────────────────────

PRESET_QUERIES = {
    "Nodi per tipo": (
        "MATCH (n) RETURN labels(n)[0] AS Tipo, count(n) AS Conteggio ORDER BY Conteggio DESC",
        "Conta tutti i nodi raggruppati per label."
    ),
    "Relazioni per tipo": (
        "MATCH ()-[r]->() RETURN type(r) AS Relazione, count(r) AS Conteggio ORDER BY Conteggio DESC",
        "Conta tutte le relazioni per tipo."
    ),
    "Top 10 aziende più menzionate": (
        """MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company)
        RETURN c.ticker AS Ticker, c.name AS Nome, count(e) AS Menzioni
        ORDER BY Menzioni DESC LIMIT 10""",
        "Quali aziende compaiono più spesso negli eventi raccolti?"
    ),
    "Aziende con più sentiment negativo": (
        """MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company)
        WHERE e.sentiment_label = 'negative'
        RETURN c.ticker AS Ticker, count(e) AS EventiNegativi
        ORDER BY EventiNegativi DESC LIMIT 10""",
        "Ranking aziende per numero di eventi con sentiment negativo."
    ),
    "Aziende co-menzionate (per tesina)": (
        """MATCH (c1:Company)<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
        WHERE c1.ticker < c2.ticker
        RETURN c1.ticker AS Azienda1, c2.ticker AS Azienda2, count(e) AS CoMenzioni
        ORDER BY CoMenzioni DESC LIMIT 20""",
        "Coppie di aziende che appaiono insieme nello stesso articolo/evento."
    ),
    "Gerarchia settori": (
        """MATCH (c:Company)-[:BELONGS_TO]->(i:Industry)-[:PART_OF]->(s:Sector)
        RETURN s.name AS Settore, i.name AS Industria, collect(c.ticker) AS Aziende""",
        "Struttura gerarchica Sector → Industry → Company."
    ),
    "Topic trending per azienda": (
        """MATCH (c:Company)-[:TRENDING_WITH]->(t:Topic)
        RETURN c.ticker AS Ticker, collect(t.name)[..5] AS TopTopics, count(t) AS NumTopics
        ORDER BY NumTopics DESC""",
        "Quali topic di Google Trends sono collegati a ogni azienda."
    ),
    "Eventi recenti con sentiment": (
        """MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company)
        WHERE e.sentiment_label IS NOT NULL
        RETURN e.source AS Sorgente, e.title AS Titolo,
               e.sentiment_label AS Sentiment, c.ticker AS Ticker
        ORDER BY e.ingested_at DESC LIMIT 20""",
        "Ultimi eventi con sentiment e azienda collegata."
    ),
    "Centralità aziende (grado)": (
        """MATCH (c:Company)
        OPTIONAL MATCH (e:Event)-[:MENZIONATA_IN]->(c)
        RETURN c.ticker AS Ticker, c.name AS Nome,
               count(e) AS GradoIngresso, c.last_close AS UltimoPrezzo
        ORDER BY GradoIngresso DESC""",
        "Grado di ingresso di ogni nodo Company — quanti Event puntano a essa."
    ),
}

# ════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("🕸️ Neo4j Explorer")
    st.divider()
    st.markdown(f"**Endpoint:** `{NEO4J_URI}`")
    st.markdown(f"**Browser:** [localhost:7474](http://localhost:7474)")
    st.divider()
    if st.button("🔄 Aggiorna", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

drv, conn_err = get_driver()

st.title("🕸️ Neo4j — Knowledge Graph Explorer")
st.caption("Esplora nodi, relazioni e pattern del grafo. Lancia query Cypher e visualizza la rete di co-menzioni.")

if conn_err:
    st.error(f"Neo4j non raggiungibile: {conn_err}")
    st.info("Avvia con: `docker compose up -d`")
    st.stop()

st.divider()

# ════════════════════════════════════════════════════════════════
# SEZIONE 1 — STATISTICHE
# ════════════════════════════════════════════════════════════════
st.subheader("📊 Statistiche del grafo")

@st.cache_data(ttl=30)
def load_stats():
    counts, _ = run_cypher("MATCH (n) RETURN labels(n)[0] AS l, count(n) AS n")
    rels, _   = run_cypher("MATCH ()-[r]->() RETURN type(r) AS t, count(r) AS n ORDER BY n DESC")
    return counts, rels

counts, rels = load_stats()

if counts:
    count_map = {r["l"]: r["n"] for r in counts}
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Nodi Company", count_map.get("Company", 0))
    c2.metric("Nodi Event",   f"{count_map.get('Event', 0):,}")
    c3.metric("Nodi Topic",   count_map.get("Topic", 0))
    c4.metric("Nodi Industry",count_map.get("Industry", 0))
    c5.metric("Nodi Sector",  count_map.get("Sector", 0))

    col_a, col_b = st.columns(2)
    with col_a:
        df_c = pd.DataFrame(counts).rename(columns={"l": "Tipo nodo", "n": "Conteggio"})
        fig = px.bar(df_c, x="Tipo nodo", y="Conteggio", color="Tipo nodo",
                     color_discrete_sequence=px.colors.qualitative.Set2, height=260)
        fig.update_layout(showlegend=False, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        if rels:
            df_r = pd.DataFrame(rels).rename(columns={"t": "Relazione", "n": "Conteggio"})
            fig2 = px.pie(df_r, names="Relazione", values="Conteggio", height=260,
                          color_discrete_sequence=px.colors.qualitative.Pastel)
            fig2.update_layout(margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ════════════════════════════════════════════════════════════════
# SEZIONE 2 — QUERY RUNNER
# ════════════════════════════════════════════════════════════════
st.subheader("⚡ Query Cypher")

tab_preset, tab_custom = st.tabs(["Query predefinite", "Query custom"])

with tab_preset:
    selected = st.selectbox("Seleziona query", list(PRESET_QUERIES.keys()))
    query_str, query_desc = PRESET_QUERIES[selected]

    st.caption(f"**Descrizione:** {query_desc}")
    st.code(query_str, language="cypher")

    if st.button("▶ Esegui", type="primary", key="run_preset"):
        with st.spinner("Esecuzione query…"):
            rows, err = run_cypher(query_str)

        if err:
            st.error(f"Errore: {err}")
        elif not rows:
            st.warning("Nessun risultato.")
        else:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"{len(df)} righe restituite")

            # Grafico automatico per colonne numeriche
            num_cols = df.select_dtypes(include="number").columns.tolist()
            str_cols = df.select_dtypes(include="object").columns.tolist()
            if num_cols and str_cols:
                fig = px.bar(df.head(20), x=str_cols[0], y=num_cols[0],
                             color_discrete_sequence=["#1a6bbd"], height=300)
                fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig, use_container_width=True)

with tab_custom:
    st.markdown("Scrivi una query Cypher personalizzata:")
    custom_q = st.text_area(
        "Query Cypher",
        value="MATCH (c:Company) RETURN c.ticker AS Ticker, c.name AS Nome, c.last_close AS Prezzo LIMIT 15",
        height=140,
    )
    st.caption("⚠️ Usa solo `MATCH` e `RETURN` — evita query distruttive (`DELETE`, `DETACH DELETE`).")

    if st.button("▶ Esegui query custom", type="primary", key="run_custom"):
        if any(kw in custom_q.upper() for kw in ["DELETE", "REMOVE", "DROP", "SET", "CREATE", "MERGE"]):
            st.error("Query con operazioni di scrittura non permesse dalla dashboard.")
        else:
            with st.spinner("Esecuzione…"):
                rows, err = run_cypher(custom_q)
            if err:
                st.error(f"Errore Cypher: {err}")
            elif not rows:
                st.warning("Nessun risultato.")
            else:
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"{len(df)} righe")

st.divider()

# ════════════════════════════════════════════════════════════════
# SEZIONE 3 — RETE CO-MENZIONI
# ════════════════════════════════════════════════════════════════
st.subheader("🔗 Rete di co-menzioni tra aziende")
st.caption(
    "Ogni nodo è un'azienda. Un arco esiste se le due aziende compaiono nello stesso articolo/evento. "
    "Lo spessore dell'arco è proporzionale al numero di co-menzioni."
)

@st.cache_data(ttl=60)
def load_comentions(min_co: int):
    rows, _ = run_cypher("""
        MATCH (c1:Company)<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
        WHERE c1.ticker < c2.ticker
        RETURN c1.ticker AS a, c2.ticker AS b, count(e) AS w
        ORDER BY w DESC LIMIT 60
    """)
    return [r for r in rows if r["w"] >= min_co]

min_w = st.slider("Soglia minima co-menzioni", 1, 20, 2)
edges = load_comentions(min_w)

if not edges:
    st.info("Nessuna co-menzione trovata. Raccogli più dati con le pipeline di ingestion.")
else:
    # Costruisce grafo con Plotly scatter
    nodes = list({r["a"] for r in edges} | {r["b"] for r in edges})
    node_idx = {n: i for i, n in enumerate(nodes)}

    # Layout circolare semplice
    import math
    n = len(nodes)
    pos = {nd: (math.cos(2*math.pi*i/n), math.sin(2*math.pi*i/n))
           for i, nd in enumerate(nodes)}

    edge_x, edge_y, edge_w = [], [], []
    for r in edges:
        x0, y0 = pos[r["a"]]
        x1, y1 = pos[r["b"]]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]
        edge_w.append(r["w"])

    fig = go.Figure()

    # Archi
    max_w = max(r["w"] for r in edges) if edges else 1
    for r in edges:
        x0, y0 = pos[r["a"]]
        x1, y1 = pos[r["b"]]
        width   = 1 + 5 * (r["w"] / max_w)
        fig.add_trace(go.Scatter(
            x=[x0, x1], y=[y0, y1], mode="lines",
            line=dict(width=width, color="rgba(26,107,189,0.4)"),
            hoverinfo="none", showlegend=False,
        ))

    # Nodi
    node_x = [pos[nd][0] for nd in nodes]
    node_y = [pos[nd][1] for nd in nodes]
    degree = {nd: sum(r["w"] for r in edges if r["a"]==nd or r["b"]==nd) for nd in nodes}
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode="markers+text",
        text=nodes, textposition="top center",
        marker=dict(
            size=[8 + 20 * (degree[nd] / max(degree.values())) for nd in nodes],
            color=[degree[nd] for nd in nodes],
            colorscale="Blues", showscale=True,
            colorbar=dict(title="Peso"),
        ),
        hovertext=[f"{nd}<br>Peso totale: {degree[nd]}" for nd in nodes],
        hoverinfo="text", showlegend=False,
    ))

    fig.update_layout(
        height=520,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Tabella co-menzioni
    with st.expander("📋 Tabella co-menzioni completa"):
        df_co = pd.DataFrame(edges).rename(columns={"a": "Azienda 1", "b": "Azienda 2", "w": "Co-menzioni"})
        st.dataframe(df_co, use_container_width=True, hide_index=True)
