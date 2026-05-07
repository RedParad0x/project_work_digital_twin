"""
ingest_yfinance.py
==================
Script di ingestion per dati finanziari tramite yfinance.

Logica:
  1. Health check ogni 30 min su AAPL per rilevare apertura mercato
  2. Se mercato aperto → fetch OHLCV per tutti i 12 ticker
  3. Metadati (Settore/Industria) → solo se il nodo Company NON esiste in Neo4j
  4. Dual-write tramite ingestion_layer.py (MongoDB + Neo4j)
  5. Gerarchia Neo4j: (Company) -[:BELONGS_TO]-> (Industry) -[:PART_OF]-> (Sector)
  6. Nodi predisposti per ricevere [:MENZIONATA_IN] da Reddit e NewsAPI

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

import yfinance as yf
from dotenv import load_dotenv

# Aggiunge la cartella scripts al path per importare ingestion_layer
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from ingestion_layer import IngestionLayer

# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente
# ---------------------------------------------------------------------------
load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# ---------------------------------------------------------------------------
# Configurazione logging (terminale + file)
# ---------------------------------------------------------------------------
start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_DIR    = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "ingestion_yfinance")
LOG_FILE   = os.path.join(LOG_DIR, f"yfinance_{start_time}.log")
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

log = logging.getLogger("ingest_yfinance")
log.setLevel(logging.INFO)

log.info("Logging configurato correttamente")

# Signal handler per flushare i log su interruzione
def signal_handler(sig, frame):
    _file_handler.flush()
    logging.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Configurazione ticker divisi per categoria
# ---------------------------------------------------------------------------
TICKERS = {
    "Big Tech":                ["NVDA", "TSLA", "AAPL", "META", "GOOGL", "MSFT"],
    "Consumer & Entertainment": ["DIS", "NFLX", "AMZN"],
    "Financial & Fintech":     ["JPM", "PYPL", "COIN"],
    "Industrials & Energy":    ["XOM", "BA", "RACE"],
}

# Ticker usato per l'health check
HEALTH_CHECK_TICKER = "AAPL"

# Intervallo di campionamento prezzi OHLCV
OHLCV_INTERVAL   = "1h"   # granularità oraria
OHLCV_PERIOD     = "1d"   # ultimi dati disponibili (1 giorno)

# Pausa tra un ticker e l'altro per non sovraccaricare yfinance
SLEEP_BETWEEN_TICKERS = 2  # secondi


# ---------------------------------------------------------------------------
# Health Check — verifica se il mercato è aperto
# ---------------------------------------------------------------------------

def is_market_open() -> bool:
    """
    Interroga yfinance su AAPL per capire se il mercato è aperto.
    Restituisce True se arrivano dati validi, False in tutti gli altri casi
    (mercato chiuso, festività, risposta None, rate limiting, ecc.).
    """
    try:
        ticker = yf.Ticker(HEALTH_CHECK_TICKER)
        df = ticker.history(period="1d", interval="1h")

        # Difesa esplicita contro risposta None da yfinance
        if df is None:
            log.info("Health check → risposta None da yfinance (mercato chiuso o rate limit)")
            return False

        if df.empty:
            log.info("Health check → mercato CHIUSO (nessun dato da %s)", HEALTH_CHECK_TICKER)
            return False

        # Verifica che le colonne essenziali esistano e abbiano valori validi
        if "Close" not in df.columns or df["Close"].dropna().empty:
            log.info("Health check → dati incompleti da yfinance (mercato chiuso o festività)")
            return False

        log.info("Health check → mercato APERTO (%s righe ricevute da %s)", len(df), HEALTH_CHECK_TICKER)
        return True

    except TypeError as e:
        # Cattura esplicitamente 'NoneType' object is not subscriptable
        log.info("Health check → yfinance ha restituito dati non validi (mercato chiuso o festività): %s", e)
        return False
    except Exception as e:
        log.warning("Health check → errore durante controllo: %s", e)
        # Se c'è un errore, considera il mercato chiuso per questa iterazione
        # ma non fallire tutto il job
        return False


# ---------------------------------------------------------------------------
# Verifica esistenza nodo Company in Neo4j
# ---------------------------------------------------------------------------

def company_node_exists(ticker: str) -> bool:
    """
    Controlla se il nodo Company esiste già in Neo4j.
    Se esiste, i metadati non vengono riscaricati da yfinance.
    """
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            result = session.run(
                "MATCH (c:Company {ticker: $ticker}) RETURN c LIMIT 1",
                ticker=ticker
            )
            exists = result.single() is not None
        driver.close()
        return exists
    except Neo4jError as e:
        log.error("Errore verifica nodo Company (%s) → %s", ticker, e)
        return False


# ---------------------------------------------------------------------------
# Fetch metadati (Settore/Industria) — solo se nodo non esiste
# ---------------------------------------------------------------------------

def fetch_and_write_metadata(ticker: str, category: str, il: IngestionLayer) -> None:
    """
    Scarica i metadati del ticker da yfinance e costruisce la gerarchia
    Neo4j: (Company) -[:BELONGS_TO]-> (Industry) -[:PART_OF]-> (Sector).
    Viene chiamato SOLO se il nodo Company non esiste ancora in Neo4j.
    """
    try:
        info = yf.Ticker(ticker).info

        sector   = info.get("sector",   "Unknown")
        industry = info.get("industry", "Unknown")
        name     = info.get("longName", ticker)
        market   = info.get("exchange", "Unknown")

        log.info("Metadati %s → %s | %s | %s", ticker, name, industry, sector)

        # Salva metadati grezzi in MongoDB
        mongo_id = il.dual_write(
            source     = "yfinance",
            data_type  = "metadata",
            node_label = "Company",
            payload    = {
                "ticker":   ticker,
                "name":     name,
                "market":   market,
                "sector":   sector,
                "industry": industry,
                "category": category,
            }
        )

        if mongo_id:
            # Crea la gerarchia completa in Neo4j
            _create_company_hierarchy(ticker, name, market, sector, industry, category, mongo_id)

    except Exception as e:
        log.error("Errore fetch metadati %s → %s", ticker, e)


def _create_company_hierarchy(
    ticker: str,
    name: str,
    market: str,
    sector: str,
    industry: str,
    category: str,
    mongo_id: str,
) -> None:
    """
    Crea in Neo4j la gerarchia:
    (Company) -[:BELONGS_TO]-> (Industry) -[:PART_OF]-> (Sector)

    Usa MERGE per evitare duplicati su tutti e tre i nodi.
    I nodi Company sono già predisposti per ricevere [:MENZIONATA_IN]
    dagli scraper di Reddit e NewsAPI.
    """
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            session.run("""
                // Crea o aggiorna il nodo Sector
                MERGE (s:Sector {name: $sector})

                // Crea o aggiorna il nodo Industry e collegalo al Sector
                MERGE (i:Industry {name: $industry})
                MERGE (i)-[:PART_OF]->(s)

                // Crea o aggiorna il nodo Company e collegalo all'Industry
                MERGE (c:Company {ticker: $ticker})
                ON CREATE SET
                    c.mongo_id      = $mongo_id,
                    c.name          = $name,
                    c.market        = $market,
                    c.category      = $category,
                    c.created_at    = $created_at,
                    c.last_close    = null,
                    c.last_volume   = null,
                    c.updated_at    = $created_at
                MERGE (c)-[:BELONGS_TO]->(i)
            """,
                ticker     = ticker,
                name       = name,
                market     = market,
                sector     = sector,
                industry   = industry,
                category   = category,
                mongo_id   = mongo_id,
                created_at = datetime.now(timezone.utc).isoformat(),
            )
        driver.close()
        log.info("Gerarchia Neo4j creata → %s → %s → %s", ticker, industry, sector)

    except Neo4jError as e:
        log.error("Errore creazione gerarchia Neo4j (%s) → %s", ticker, e)


# ---------------------------------------------------------------------------
# Fetch OHLCV — prezzi orari
# ---------------------------------------------------------------------------

def fetch_and_write_ohlcv(ticker: str, il: IngestionLayer) -> None:
    """
    Scarica i dati OHLCV orari del ticker e li scrive su MongoDB + Neo4j
    tramite dual_write(). Aggiorna anche last_close e last_volume sul nodo
    Company in Neo4j.
    """
    try:
        df = yf.Ticker(ticker).history(period=OHLCV_PERIOD, interval=OHLCV_INTERVAL)

        if df.empty:
            log.warning("OHLCV vuoto per %s — mercato probabilmente chiuso", ticker)
            return

        # Prende l'ultima riga disponibile (dato più recente)
        latest = df.iloc[-1]
        ts     = df.index[-1].isoformat()

        payload = {
            "ticker":    ticker,
            "timestamp": ts,
            "open":      round(float(latest["Open"]),   4),
            "high":      round(float(latest["High"]),   4),
            "low":       round(float(latest["Low"]),    4),
            "close":     round(float(latest["Close"]),  4),
            "volume":    int(latest["Volume"]),
        }

        mongo_id = il.dual_write(
            source     = "yfinance",
            data_type  = "ohlcv",
            node_label = "Company",
            payload    = payload,
        )

        if mongo_id:
            # Aggiorna last_close e last_volume sul nodo Company in Neo4j
            _update_company_price(ticker, payload["close"], payload["volume"], ts)
            log.info("OHLCV %s → close: %.2f | volume: %d", ticker, payload["close"], payload["volume"])

    except Exception as e:
        log.error("Errore fetch OHLCV %s → %s", ticker, e)


def _update_company_price(ticker: str, close: float, volume: int, ts: str) -> None:
    """
    Aggiorna last_close e last_volume sul nodo Company esistente in Neo4j.
    Non crea un nuovo nodo — aggiorna solo gli attributi di prezzo.
    """
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            session.run("""
                MATCH (c:Company {ticker: $ticker})
                SET c.last_close  = $close,
                    c.last_volume = $volume,
                    c.updated_at  = $ts
            """,
                ticker = ticker,
                close  = close,
                volume = volume,
                ts     = ts,
            )
        driver.close()
    except Neo4jError as e:
        log.error("Errore aggiornamento prezzo Neo4j (%s) → %s", ticker, e)


# ---------------------------------------------------------------------------
# Job principale — eseguito ogni 30 minuti da schedule
# ---------------------------------------------------------------------------

def ingestion_job() -> None:
    """
    Job principale schedulato ogni 30 minuti.
    1. Health check → se mercato chiuso esce subito
    2. Per ogni ticker:
       a. Se nodo Company non esiste → scarica metadati e crea gerarchia
       b. Scarica OHLCV e aggiorna prezzi
    """
    log.info("=" * 60)
    log.info("Avvio job ingestion yfinance — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # 1. Health check
    if not is_market_open():
        log.info("Mercato chiuso — job saltato. Prossimo tentativo tra 30 minuti.")
        return

    # 2. Ingestion per ogni ticker
    with IngestionLayer() as il:
        for category, tickers in TICKERS.items():
            log.info("--- Categoria: %s ---", category)
            for ticker in tickers:
                try:
                    # Metadati solo se nodo Company non esiste
                    if not company_node_exists(ticker):
                        log.info("%s → nodo Company assente, scarico metadati...", ticker)
                        fetch_and_write_metadata(ticker, category, il)
                        time.sleep(SLEEP_BETWEEN_TICKERS)
                    else:
                        log.info("%s → nodo Company già presente, salto metadati", ticker)

                    # OHLCV sempre (mercato aperto)
                    fetch_and_write_ohlcv(ticker, il)
                    time.sleep(SLEEP_BETWEEN_TICKERS)

                except Exception as e:
                    log.error("Errore inatteso per %s → %s", ticker, e)
                    continue

    log.info("Job completato. Prossimo aggiornamento tra 30 minuti.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Avvio ingest_yfinance.py")
    log.info("Ticker monitorati: %d", sum(len(v) for v in TICKERS.values()))

    # Esegui il job una volta e poi esci
    ingestion_job()
    log.info("Job completato. Uscita.")