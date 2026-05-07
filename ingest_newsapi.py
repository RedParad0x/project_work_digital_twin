"""
ingest_newsapi.py
=================
Script di ingestion per articoli giornalistici tramite NewsAPI.

Logica:
  1. Per ogni azienda cerca notizie per nome E ticker
  2. Salva articolo grezzo in MongoDB (collezione raw_data)
  3. Crea nodo Event in Neo4j
  4. Crea arco [:MENZIONATA_IN] verso ogni Company menzionata
     nel titolo o nella descrizione dell'articolo
  5. Schedulato ogni ora (max 100 req/giorno piano gratuito)

Limiti NewsAPI piano gratuito:
  - 100 richieste/giorno
  - Articoli ultimi 30 giorni
  - Solo titolo + descrizione (no testo completo)

Autori : Massimo Mazzini, Rafaele Rossi
Corso  : Big Data Engineer & Solution Architect — 2° anno
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import requests
import schedule
from dotenv import load_dotenv

# Aggiunge la cartella scripts al path per importare ingestion_layer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingestion_layer import IngestionLayer

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente
# ---------------------------------------------------------------------------
load_dotenv()

NEWS_API_KEY   = os.getenv("NEWS_API_KEY")
NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# ---------------------------------------------------------------------------
# Configurazione logging (terminale + file)
# ---------------------------------------------------------------------------
start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_DIR    = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "ingestion_newsapi")
LOG_FILE   = os.path.join(LOG_DIR, f"newsapi_{start_time}.log")
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

log = logging.getLogger("ingest_newsapi")
log.setLevel(logging.INFO)

log.info("Logging configurato correttamente")

# Signal handler per flushare i log su interruzione
def signal_handler(sig, frame):
    _file_handler.flush()
    logging.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Endpoint NewsAPI
# ---------------------------------------------------------------------------
NEWSAPI_URL = "https://newsapi.org/v2/everything"

# ---------------------------------------------------------------------------
# Aziende monitorate — nome + ticker per query e rilevamento menzioni
# ---------------------------------------------------------------------------
COMPANIES = [
    {"ticker": "NVDA",  "name": "NVIDIA",    "category": "Big Tech"},
    {"ticker": "TSLA",  "name": "Tesla",     "category": "Big Tech"},
    {"ticker": "AAPL",  "name": "Apple",     "category": "Big Tech"},
    {"ticker": "DIS",   "name": "Disney",    "category": "Consumer & Entertainment"},
    {"ticker": "NFLX",  "name": "Netflix",   "category": "Consumer & Entertainment"},
    {"ticker": "AMZN",  "name": "Amazon",    "category": "Consumer & Entertainment"},
    {"ticker": "JPM",   "name": "JPMorgan",  "category": "Financial & Fintech"},
    {"ticker": "PYPL",  "name": "PayPal",    "category": "Financial & Fintech"},
    {"ticker": "COIN",  "name": "Coinbase",  "category": "Financial & Fintech"},
    {"ticker": "XOM",   "name": "Exxon",     "category": "Industrials & Energy"},
    {"ticker": "BA",    "name": "Boeing",    "category": "Industrials & Energy"},
    {"ticker": "RACE",  "name": "Ferrari",   "category": "Industrials & Energy"},
]

# Pausa tra una richiesta e l'altra per rispettare i rate limit
SLEEP_BETWEEN_REQUESTS = 3  # secondi

# Numero massimo di articoli per query
MAX_ARTICLES_PER_QUERY = 5


# ---------------------------------------------------------------------------
# Rilevamento menzioni — controlla se un'azienda è citata nel testo
# ---------------------------------------------------------------------------

def detect_mentions(text: str) -> list[str]:
    """
    Analizza il testo (titolo + descrizione) e restituisce la lista
    dei ticker delle aziende menzionate.
    Cerca sia il nome che il ticker di ogni azienda.
    """
    text_lower = text.lower()
    mentioned = []
    for company in COMPANIES:
        if (
            company["name"].lower() in text_lower
            or company["ticker"].lower() in text_lower
        ):
            mentioned.append(company["ticker"])
    return mentioned


# ---------------------------------------------------------------------------
# Creazione archi [:MENZIONATA_IN] in Neo4j
# ---------------------------------------------------------------------------

def create_mentions_relationships(mongo_id: str, tickers: list[str]) -> None:
    """
    Crea in Neo4j gli archi:
    (:Event {mongo_id}) -[:MENZIONATA_IN]-> (:Company {ticker})
    per ogni ticker rilevato nel testo dell'articolo.
    """
    if not tickers:
        return

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            for ticker in tickers:
                session.run("""
                    MATCH (e:Event {mongo_id: $mongo_id})
                    MATCH (c:Company {ticker: $ticker})
                    MERGE (e)-[:MENZIONATA_IN]->(c)
                """,
                    mongo_id = mongo_id,
                    ticker   = ticker,
                )
                log.info("Arco [:MENZIONATA_IN] → Event %s → Company %s", mongo_id[:8], ticker)
        driver.close()

    except Neo4jError as e:
        log.error("Errore creazione archi Neo4j → %s", e)


# ---------------------------------------------------------------------------
# Fetch articoli da NewsAPI
# ---------------------------------------------------------------------------

def fetch_articles(query: str) -> list[dict]:
    """
    Esegue una chiamata a NewsAPI con la query fornita.
    Restituisce la lista degli articoli o lista vuota in caso di errore.
    """
    try:
        params = {
            "q":        query,
            "language": "en",
            "sortBy":   "publishedAt",
            "pageSize": MAX_ARTICLES_PER_QUERY,
            "apiKey":   NEWS_API_KEY,
        }
        response = requests.get(NEWSAPI_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "ok":
            log.warning("NewsAPI risposta non ok per query '%s' → %s", query, data.get("message"))
            return []

        articles = data.get("articles", [])
        log.info("NewsAPI → query: '%s' | articoli ricevuti: %d", query, len(articles))
        return articles

    except requests.exceptions.RequestException as e:
        log.error("Errore richiesta NewsAPI → %s", e)
        return []


# ---------------------------------------------------------------------------
# Contatore richieste giornaliere
# ---------------------------------------------------------------------------

_daily_request_count = 0
_last_reset_date     = datetime.now(timezone.utc).date()
MAX_DAILY_REQUESTS   = 90  # teniamo un margine di 10 rispetto al limite di 100


def _check_and_increment_request_count() -> bool:
    """
    Verifica che non si superino le 90 richieste giornaliere.
    Resetta il contatore a mezzanotte UTC.
    Restituisce True se si può procedere, False se il limite è raggiunto.
    """
    global _daily_request_count, _last_reset_date

    today = datetime.now(timezone.utc).date()
    if today > _last_reset_date:
        log.info("Reset contatore richieste giornaliere (nuovo giorno)")
        _daily_request_count = 0
        _last_reset_date     = today

    if _daily_request_count >= MAX_DAILY_REQUESTS:
        log.warning("Limite giornaliero raggiunto (%d/%d) — job saltato", _daily_request_count, MAX_DAILY_REQUESTS)
        return False

    _daily_request_count += 1
    return True


# ---------------------------------------------------------------------------
# Job principale
# ---------------------------------------------------------------------------

def ingestion_job() -> None:
    """
    Job principale schedulato ogni ora.
    Per ogni azienda:
      1. Controlla il limite giornaliero di richieste
      2. Fetch articoli da NewsAPI (query: "Nome" OR "TICKER")
      3. Per ogni articolo:
         a. Rileva menzioni di altre aziende nel testo
         b. Dual-write su MongoDB + Neo4j (nodo Event)
         c. Crea archi [:MENZIONATA_IN] verso le Company menzionate
    """
    log.info("=" * 60)
    log.info("Avvio job ingestion NewsAPI — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # Set per tracciare gli URL già processati in questo ciclo (evita duplicati)
    processed_urls: set[str] = set()

    with IngestionLayer() as il:
        for company in COMPANIES:
            ticker = company["ticker"]
            name   = company["name"]

            # Query combinata: nome OR ticker
            query = f'"{name}" OR "{ticker}"'

            # Verifica limite giornaliero
            if not _check_and_increment_request_count():
                log.warning("Limite giornaliero raggiunto — interruzione job")
                break

            articles = fetch_articles(query)
            time.sleep(SLEEP_BETWEEN_REQUESTS)

            for article in articles:
                url = article.get("url", "")

                # Salta articoli già processati in questo ciclo
                if url in processed_urls:
                    log.info("Articolo duplicato saltato → %s", url[:60])
                    continue
                processed_urls.add(url)

                # Testo completo disponibile (titolo + descrizione)
                title       = article.get("title", "") or ""
                description = article.get("description", "") or ""
                full_text   = f"{title} {description}"

                # Rileva tutte le aziende menzionate nel testo
                mentioned_tickers = detect_mentions(full_text)

                # Assicurati che l'azienda della query sia sempre inclusa
                if ticker not in mentioned_tickers:
                    mentioned_tickers.append(ticker)

                payload = {
                    "title":        title,
                    "description":  description,
                    "url":          url,
                    "source_name":  article.get("source", {}).get("name", ""),
                    "author":       article.get("author", ""),
                    "published_at": article.get("publishedAt", ""),
                    "query":        query,
                    "mentions":     mentioned_tickers,
                }

                # Dual-write MongoDB + Neo4j
                mongo_id = il.dual_write(
                    source     = "newsapi",
                    data_type  = "event",
                    node_label = "Event",
                    payload    = payload,
                )

                if mongo_id:
                    # Crea archi [:MENZIONATA_IN] per ogni azienda rilevata
                    create_mentions_relationships(mongo_id, mentioned_tickers)
                    log.info(
                        "Articolo salvato → %s | menzioni: %s",
                        title[:60], mentioned_tickers
                    )

    log.info("Job completato. Richieste oggi: %d/%d", _daily_request_count, MAX_DAILY_REQUESTS)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Avvio ingest_newsapi.py")
    log.info("Aziende monitorate: %d", len(COMPANIES))
    log.info("Max articoli per query: %d", MAX_ARTICLES_PER_QUERY)
    log.info("Max richieste giornaliere: %d", MAX_DAILY_REQUESTS)

    # Esegui il job una volta e poi esci
    ingestion_job()
    log.info("Job completato. Uscita.")