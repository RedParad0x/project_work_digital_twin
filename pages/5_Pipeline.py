"""
pages/5_Pipeline.py  —  Pipeline Control
=========================================
Pannello di controllo della pipeline:
  - Avvio singolo script con bottone
  - Log viewer in tempo reale
  - Stato MongoDB per ogni sorgente
  - Throughput per run
"""

import glob
import os
import subprocess
import sys
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

st.set_page_config(page_title="Pipeline Control", page_icon="⚙️", layout="wide")
load_dotenv()

MONGO_URI     = os.getenv("MONGO_URI",     "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "media_twin")

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = BASE_DIR
LOGS_DIR    = os.path.join(BASE_DIR, "..", "logs")
PYTHON      = sys.executable

PIPELINE = [
    {"key": "yfinance",      "file": "ingest_yfinance.py",  "emoji": "📈",
     "desc": "Prezzi OHLCV da Yahoo Finance",      "log_dir": "ingestion_yfinance"},
    {"key": "gdelt",         "file": "ingest_gdelt.py",     "emoji": "🌍",
     "desc": "Notizie globali GDELT GKG v2",       "log_dir": "ingestion_gdelt"},
    {"key": "newsapi",       "file": "ingest_newsapi.py",   "emoji": "📰",
     "desc": "Articoli da NewsAPI",                "log_dir": "ingestion_newsapi"},
    {"key": "reddit",        "file": "ingest_reddit.py",    "emoji": "💬",
     "desc": "Post e commenti Reddit",             "log_dir": "ingestion_reddit"},
    {"key": "youtube",       "file": "ingest_youtube.py",   "emoji": "▶️",
     "desc": "Video e commenti YouTube",           "log_dir": "ingestion_youtube"},
    {"key": "google_trends", "file": "ingest_pytrends.py",  "emoji": "🔍",
     "desc": "Google Trends (trendspy)",           "log_dir": "ingestion_pytrends"},
    {"key": "nlp",           "file": "nlp_layer.py",        "emoji": "🧠",
     "desc": "Analisi sentiment NLP",              "log_dir": "nlp_layer"},
]

# ── Helper ────────────────────────────────────────────────────────────────────

def get_latest_log(log_dir_name: str) -> str | None:
    path  = os.path.join(LOGS_DIR, log_dir_name)
    files = sorted(glob.glob(os.path.join(path, "*.log")), reverse=True) if os.path.isdir(path) else []
    return files[0] if files else None

def read_log_tail(log_path: str, n_lines: int = 80) -> str:
    if not log_path or not os.path.exists(log_path):
        return "Nessun log disponibile."
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return "".join(lines[-n_lines:])
    except Exception as e:
        return f"Errore lettura log: {e}"

def count_docs(source: str) -> int:
    try:
        c   = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        col = c[MONGO_DB_NAME]["raw_data"]
        n   = col.count_documents({"source": source})
        c.close()
        return n
    except PyMongoError:
        return -1

def mongo_ok() -> bool:
    try:
        c = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        c.server_info()
        c.close()
        return True
    except PyMongoError:
        return False

# ════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("⚙️ Pipeline Control")
    st.divider()
    st.markdown("**Info sistema**")
    st.caption(f"Python: `{sys.version[:6]}`")
    st.caption(f"Scripts: `{SCRIPTS_DIR}`")
    db_ok = mongo_ok()
    if db_ok:
        st.success("MongoDB: connesso")
    else:
        st.error("MongoDB: offline")
        st.code("docker compose up -d", language="bash")
    st.divider()
    log_lines = st.slider("Righe log visualizzate", 20, 200, 80)

# ════════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════════
st.title("⚙️ Pipeline Control")
st.caption("Avvia le pipeline di ingestion, monitora i log in tempo reale e verifica lo stato di MongoDB.")
st.divider()

# ════════════════════════════════════════════════════════════════
# SEZIONE 1 — STATO PIPELINE
# ════════════════════════════════════════════════════════════════
st.subheader("📊 Stato pipeline")

status_rows = []
for p in PIPELINE:
    log_path = get_latest_log(p["log_dir"])
    last_run = (
        datetime.fromtimestamp(os.path.getmtime(log_path)).strftime("%d/%m %H:%M")
        if log_path else "Mai"
    )
    n_docs = count_docs(p["key"])
    status_rows.append({
        "":          p["emoji"],
        "Script":    p["file"],
        "Descrizione": p["desc"],
        "Ultimo run": last_run,
        "Documenti MongoDB": f"{n_docs:,}" if n_docs >= 0 else "—",
    })

df_status = pd.DataFrame(status_rows)
st.dataframe(df_status, use_container_width=True, hide_index=True)

st.divider()

# ════════════════════════════════════════════════════════════════
# SEZIONE 2 — AVVIO SCRIPT
# ════════════════════════════════════════════════════════════════
st.subheader("▶ Avvia script")

st.warning(
    "⚠️ Gli script avviano processi Python separati. "
    "Tieni aperto il terminale / Docker durante l'esecuzione. "
    "I log appariranno nel viewer sotto."
)

# Griglia bottoni
cols = st.columns(len(PIPELINE))
for i, p in enumerate(PIPELINE):
    with cols[i]:
        if st.button(f"{p['emoji']}\n{p['key']}", use_container_width=True, key=f"btn_{p['key']}"):
            script_path = os.path.join(SCRIPTS_DIR, p["file"])
            if not os.path.exists(script_path):
                st.error(f"File non trovato: {p['file']}")
            elif not db_ok:
                st.error("MongoDB offline — avvia Docker prima.")
            else:
                st.session_state["running_script"] = p
                st.session_state["running_log"]    = None

# Esecuzione e streaming log
if "running_script" in st.session_state:
    p           = st.session_state["running_script"]
    script_path = os.path.join(SCRIPTS_DIR, p["file"])

    st.divider()
    st.markdown(f"### ▶ Esecuzione: `{p['file']}`")
    st.caption(p["desc"])

    log_box  = st.empty()
    prog_bar = st.progress(0, text="Avvio script…")

    env                        = os.environ.copy()
    env["PIPELINE_SINGLE_RUN"] = "1"
    env["PYTHONUNBUFFERED"]    = "1"

    try:
        proc = subprocess.Popen(
            [PYTHON, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )

        output_lines = []
        start        = time.time()
        timeout      = 600  # 10 minuti max

        while True:
            line = proc.stdout.readline()
            if line:
                output_lines.append(line)
                elapsed = time.time() - start
                pct     = min(int(elapsed / timeout * 100), 95)
                prog_bar.progress(pct, text=f"In esecuzione… ({int(elapsed)}s)")
                log_box.code("".join(output_lines[-log_lines:]), language="log")

            if proc.poll() is not None:
                break

            if time.time() - start > timeout:
                proc.kill()
                output_lines.append("\n[TIMEOUT: script interrotto dopo 10 minuti]\n")
                break

        prog_bar.progress(100, text="Completato")
        rc = proc.returncode

        if rc == 0:
            st.success(f"✅ `{p['file']}` completato con successo.")
        else:
            st.error(f"❌ `{p['file']}` terminato con errore (exit code {rc}).")

        log_box.code("".join(output_lines), language="log")

    except Exception as e:
        st.error(f"Errore avvio processo: {e}")
    finally:
        del st.session_state["running_script"]

st.divider()

# ════════════════════════════════════════════════════════════════
# SEZIONE 3 — LOG VIEWER
# ════════════════════════════════════════════════════════════════
st.subheader("📋 Log viewer")

log_scripts = [p for p in PIPELINE if get_latest_log(p["log_dir"])]

if not log_scripts:
    st.info("Nessun log trovato. Avvia almeno uno script per generare i log.")
else:
    selected_log = st.selectbox(
        "Seleziona log",
        log_scripts,
        format_func=lambda p: f"{p['emoji']} {p['file']} — {p['desc']}",
    )

    log_path = get_latest_log(selected_log["log_dir"])
    if log_path:
        st.caption(f"File: `{log_path}`")
        col_refresh, col_lines = st.columns([1, 3])
        with col_refresh:
            live = st.toggle("🔄 Live (aggiorna ogni 3s)", value=False)

        log_content = read_log_tail(log_path, log_lines)
        log_placeholder = st.empty()
        log_placeholder.code(log_content, language="log")

        if live:
            st.caption("Modalità live attiva — la pagina si aggiorna ogni 3 secondi.")
            time.sleep(3)
            st.rerun()

st.divider()

# ════════════════════════════════════════════════════════════════
# SEZIONE 4 — THROUGHPUT
# ════════════════════════════════════════════════════════════════
st.subheader("📈 Throughput ingestion")

@st.cache_data(ttl=30)
def load_throughput():
    try:
        c   = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        col = c[MONGO_DB_NAME]["raw_data"]
        res = list(col.aggregate([
            {"$addFields": {"hour": {"$substr": ["$ingested_at", 0, 13]}}},
            {"$group": {"_id": {"hour": "$hour", "src": "$source"}, "n": {"$sum": 1}}},
            {"$sort": {"_id.hour": -1}},
            {"$limit": 200},
        ]))
        c.close()
        return res
    except PyMongoError:
        return []

tp = load_throughput()
if tp:
    df_tp = pd.DataFrame([
        {"Ora": r["_id"]["hour"], "Sorgente": r["_id"]["src"], "Documenti": r["n"]}
        for r in tp
    ]).sort_values("Ora")
    fig = px.line(df_tp, x="Ora", y="Documenti", color="Sorgente",
                  markers=True, height=300,
                  color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(margin=dict(l=0,r=0,t=10,b=0))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Nessun dato di throughput disponibile.")
