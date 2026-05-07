"""
ingest_pytrends.py
==================
Script di ingestion per dati Google Trends tramite trendspy.
(sostituto di pytrends, archiviato ad aprile 2025)

Logica:
  1. Per ogni azienda raccoglie:
     - Interesse nel tempo (ultimi 7 giorni, risoluzione oraria)
     - Query correlate (rising + top) per arricchire il Knowledge Graph
  2. Autenticazione tramite cookie Google dal .env (evita 429)
  3. Dual-write su MongoDB + Neo4j tramite ingestion_layer.py
  4. Crea nodi Topic in Neo4j collegati ai nodi Company
  5. Sleep random tra le richieste per rispettare i rate limit
  6. Schedulato una volta al giorno alle 06:00

Note:
  - Il cookie GOOGLE_TRENDS_COOKIE nel .env scade periodicamente
  - Quando ricomincia il 429, aggiorna il cookie dal browser
  - I valori di interesse sono indici relativi (0-100), non volumi assoluti

Autori : Massimo Mazzini, Rafaele Rossi
Corso  : Big Data Engineer & Solution Architect — 2° anno
"""

from __future__ import annotations

import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import schedule
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from trendspy import Trends

# Aggiunge la cartella scripts al path per importare ingestion_layer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingestion_layer import IngestionLayer

# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente
# ---------------------------------------------------------------------------
load_dotenv()

NEO4J_URI            = os.getenv("NEO4J_URI")
NEO4J_USER           = os.getenv("NEO4J_USER")
NEO4J_PASSWORD       = os.getenv("NEO4J_PASSWORD")
GOOGLE_TRENDS_COOKIE = os.getenv("GOOGLE_TRENDS_COOKIE", "")

# ---------------------------------------------------------------------------
# Configurazione logging (terminale + file)
# ---------------------------------------------------------------------------
start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_DIR    = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "ingestion_pytrends")
LOG_FILE   = os.path.join(LOG_DIR, f"pytrends_{start_time}.log")
os.makedirs(LOG_DIR, exist_ok=True)

_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)
_root_logger.handlers.clear()

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_formatter)
_root_logger.addHandler(_console_handler)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(_formatter)
_root_logger.addHandler(_file_handler)

log = logging.getLogger("ingest_pytrends")
log.setLevel(logging.INFO)

# Silenzia i log di dettaglio dell'ingestion_layer — solo errori
logging.getLogger("ingestion_layer").setLevel(logging.WARNING)

log.info("Logging configurato correttamente")

# Signal handler per flushare i log su interruzione
def signal_handler(sig, frame):
    _file_handler.flush()
    logging.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------
TIMEFRAME    = "now 7-d"   # ultimi 7 giorni con risoluzione oraria
REQUEST_DELAY = 30.0       # secondi tra richieste (evita 429)
SLEEP_MIN    = 5           # sleep minimo aggiuntivo tra ticker
SLEEP_MAX    = 10          # sleep massimo aggiuntivo tra ticker

# ---------------------------------------------------------------------------
# Aziende monitorate — keyword ottimizzate per Google Trends
# ---------------------------------------------------------------------------
COMPANIES = [
    {"ticker": "NVDA",  "keyword": "NVIDIA",        "name": "NVIDIA"},
    {"ticker": "TSLA",  "keyword": "Tesla",          "name": "Tesla"},
    {"ticker": "AAPL",  "keyword": "Apple",          "name": "Apple"},
    {"ticker": "META",  "keyword": "Meta Facebook",  "name": "Meta"},
    {"ticker": "GOOGL", "keyword": "Google",         "name": "Google"},
    {"ticker": "MSFT",  "keyword": "Microsoft",      "name": "Microsoft"},
    {"ticker": "DIS",   "keyword": "Disney",         "name": "Disney"},
    {"ticker": "NFLX",  "keyword": "Netflix",        "name": "Netflix"},
    {"ticker": "AMZN",  "keyword": "Amazon",         "name": "Amazon"},
    {"ticker": "JPM",   "keyword": "JPMorgan",       "name": "JPMorgan"},
    {"ticker": "PYPL",  "keyword": "PayPal",         "name": "PayPal"},
    {"ticker": "COIN",  "keyword": "Coinbase",       "name": "Coinbase"},
    {"ticker": "XOM",   "keyword": "Exxon",          "name": "Exxon"},
    {"ticker": "BA",    "keyword": "Boeing",         "name": "Boeing"},
    {"ticker": "RACE",  "keyword": "Ferrari",        "name": "Ferrari"},
]


# ---------------------------------------------------------------------------
# Creazione nodi Topic e relazioni in Neo4j
# ---------------------------------------------------------------------------

def create_topic_relationships(ticker: str, topics: list[str]) -> None:
    """
    Crea in Neo4j nodi Topic collegati alla Company tramite [:TRENDING_WITH].
    Struttura: (:Company {ticker})-[:TRENDING_WITH]->(:Topic {name})
    Permette di capire PERCHÉ c'è un picco di interesse su un'azienda.
    """
    if not topics:
        return

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            for topic in topics:
                if not topic or len(topic.strip()) < 2:
                    continue
                session.run("""
                    MATCH (c:Company {ticker: $ticker})
                    MERGE (t:Topic {name: $topic})
                    MERGE (c)-[:TRENDING_WITH]->(t)
                """,
                    ticker = ticker,
                    topic  = topic.strip(),
                )
        driver.close()
        log.info("Topic Neo4j → %s: %s", ticker, topics[:5])
    except Neo4jError as e:
        log.error("Errore Topic Neo4j → %s", e)


# ---------------------------------------------------------------------------
# Fetch dati Google Trends per una singola azienda
# ---------------------------------------------------------------------------

def fetch_trends(tr: Trends, company: dict) -> dict | None:
    """
    Recupera interesse nel tempo e query correlate per una singola azienda.
    Restituisce un dizionario con i dati o None in caso di errore.
    """
    ticker  = company["ticker"]
    keyword = company["keyword"]

    try:
        log.info("%s → fetch interesse nel tempo...", ticker)

        # 1. Interesse nel tempo (7 giorni, risoluzione oraria)
        interest_df = tr.interest_over_time(
            [keyword],
            timeframe = TIMEFRAME,
        )

        if interest_df is None or interest_df.empty:
            log.warning("%s → nessun dato di interesse nel tempo", ticker)
            interest_data = []
        else:
            interest_data = [
                {
                    "timestamp": str(ts),
                    "value":     int(row[keyword]) if keyword in row else 0,
                    "partial":   bool(row.get("isPartial", False)),
                }
                for ts, row in interest_df.iterrows()
            ]

        # Sleep tra le due chiamate
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

        # 2. Query correlate
        log.info("%s → fetch query correlate...", ticker)
        rising_queries = []
        top_queries    = []

        try:
            related = tr.related_queries(keyword)
            if related and keyword in related:
                rising_df = related[keyword].get("rising")
                top_df    = related[keyword].get("top")
                if rising_df is not None and not rising_df.empty:
                    rising_queries = rising_df["query"].tolist()[:10]
                if top_df is not None and not top_df.empty:
                    top_queries = top_df["query"].tolist()[:10]
        except Exception as e:
            log.warning("%s → query correlate non disponibili → %s", ticker, e)

        return {
            "ticker":         ticker,
            "keyword":        keyword,
            "timeframe":      TIMEFRAME,
            "interest_data":  interest_data,
            "rising_queries": rising_queries,
            "top_queries":    top_queries,
            "peak_value":     max((d["value"] for d in interest_data), default=0),
            "avg_value":      round(
                sum(d["value"] for d in interest_data) / len(interest_data), 2
            ) if interest_data else 0,
            "fetched_at":     datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        log.error("%s → errore fetch trends → %s", ticker, e)
        return None


# ---------------------------------------------------------------------------
# Job principale
# ---------------------------------------------------------------------------

def ingestion_job() -> None:
    """
    Job principale schedulato una volta al giorno alle 06:00.
    Per ogni azienda:
      1. Fetch interesse nel tempo (7 giorni)
      2. Fetch query correlate (rising + top)
      3. Dual-write MongoDB + Neo4j
      4. Crea nodi Topic in Neo4j collegati alla Company
    """
    log.info("=" * 60)
    log.info("Avvio job ingestion Google Trends — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    if not GOOGLE_TRENDS_COOKIE:
        log.warning("GOOGLE_TRENDS_COOKIE non trovato nel .env — possibile 429")

    # Inizializza trendspy con cookie e delay
    cookies = {}
    if GOOGLE_TRENDS_COOKIE:
        # Parsa il cookie string in dizionario
        for part in GOOGLE_TRENDS_COOKIE.split(";"):
            part = part.strip()
            if "=" in part:
                key, _, value = part.partition("=")
                cookies[key.strip()] = value.strip()
        log.info("Cookie caricato → %d valori", len(cookies))

    tr = Trends(
        request_delay = REQUEST_DELAY,
        cookies       = cookies if cookies else None,
    )

    total_saved  = 0
    total_failed = 0

    with IngestionLayer() as il:
        for company in COMPANIES:
            ticker = company["ticker"]
            log.info("--- %s (%s) ---", ticker, company["keyword"])

            data = fetch_trends(tr, company)

            if data is None:
                total_failed += 1
                continue

            all_topics = list(set(data["rising_queries"] + data["top_queries"]))

            payload = {
                "ticker":         ticker,
                "keyword":        data["keyword"],
                "timeframe":      data["timeframe"],
                "interest_data":  data["interest_data"],
                "rising_queries": data["rising_queries"],
                "top_queries":    data["top_queries"],
                "peak_value":     data["peak_value"],
                "avg_value":      data["avg_value"],
                "fetched_at":     data["fetched_at"],
            }

            mongo_id = il.dual_write(
                source     = "google_trends",
                data_type  = "trend",
                node_label = "Topic",
                payload    = payload,
            )

            if mongo_id:
                create_topic_relationships(ticker, all_topics)
                total_saved += 1
                log.info(
                    "✓ %s | peak: %d | avg: %.1f | topics: %d | rising: %s",
                    ticker,
                    data["peak_value"],
                    data["avg_value"],
                    len(all_topics),
                    data["rising_queries"][:3],
                )
            else:
                total_failed += 1

            # Sleep aggiuntivo tra ticker
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    log.info("=" * 60)
    log.info("Job completato → salvati: %d | falliti: %d", total_saved, total_failed)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Avvio ingest_pytrends.py")
    log.info("Aziende monitorate: %d", len(COMPANIES))
    log.info("Timeframe: %s", TIMEFRAME)
    log.info("Request delay: %.0fs", REQUEST_DELAY)

    # Esegui il job una volta e poi esci
    ingestion_job()
    log.info("Job completato. Uscita.")
