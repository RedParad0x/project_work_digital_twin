"""
ingest_gdelt.py
===============
Script di ingestion per eventi e notizie da GDELT Global Knowledge Graph v2.

Logica:
  1. Ogni 15 minuti scarica l'ultimo file GKG da GDELT (via lastupdate.txt)
  2. Download in RAM (io.BytesIO) — nessun file temporaneo su disco
  3. Parsing selettivo con pandas usecols — solo le colonne necessarie
  4. Filtro a monte su V2Organizations per le 15 aziende monitorate
  5. Estrae V2Tone pre-calcolato (sentiment già disponibile, no NLP necessario)
  6. Dual-write su MongoDB + Neo4j tramite ingestion_layer.py
  7. Crea archi [:MENZIONATA_IN] verso i nodi Company esistenti
  8. Anti-duplicati su URL già processati

Nota: i record GDELT hanno già il sentiment (V2Tone) pre-calcolato,
quindi NON passano per il layer NLP — risparmio significativo di CPU.

Autori : Massimo Mazzini, Rafaele Rossi
Corso  : Big Data Engineer & Solution Architect — 2° anno
"""

from __future__ import annotations

import io
import logging
import os
import signal
import sys
import time
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests
import schedule
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# Aggiunge la cartella scripts al path per importare ingestion_layer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
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
LOG_DIR    = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "ingestion_gdelt")
LOG_FILE   = os.path.join(LOG_DIR, f"gdelt_{start_time}.log")
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

log = logging.getLogger("ingest_gdelt")
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
logging.getLogger("ingestion_layer").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Configurazione GDELT
# ---------------------------------------------------------------------------
GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Colonne GKG da caricare (parsing selettivo per risparmiare RAM)
GKG_COLUMNS_ALL = [
    "GKGRECORDID", "DATE", "SourceCollectionIdentifier",
    "SourceCommonName", "DocumentIdentifier", "Counts",
    "V2Counts", "Themes", "V2Themes", "Locations",
    "V2Locations", "Persons", "V2Persons", "Organizations",
    "V2Organizations", "V2Tone",
]
GKG_COLUMNS_USE = [
    "GKGRECORDID", "DATE", "DocumentIdentifier",
    "SourceCommonName", "V2Organizations", "V2Tone",
]

# ---------------------------------------------------------------------------
# Aziende monitorate — nomi come appaiono nel campo V2Organizations di GDELT
# GDELT usa nomi in maiuscolo, a volte con varianti
# ---------------------------------------------------------------------------
COMPANIES = {
    "NVDA":  ["NVIDIA", "NVIDIA CORP", "NVIDIA CORPORATION"],
    "TSLA":  ["TESLA", "TESLA INC", "TESLA MOTORS"],
    "AAPL":  ["APPLE", "APPLE INC"],
    "META":  ["META", "META PLATFORMS", "FACEBOOK"],
    "GOOGL": ["GOOGLE", "ALPHABET", "ALPHABET INC"],
    "MSFT":  ["MICROSOFT", "MICROSOFT CORP", "MICROSOFT CORPORATION"],
    "DIS":   ["DISNEY", "WALT DISNEY", "THE WALT DISNEY"],
    "NFLX":  ["NETFLIX"],
    "AMZN":  ["AMAZON", "AMAZON.COM"],
    "JPM":   ["JPMORGAN", "JP MORGAN", "JPMORGAN CHASE"],
    "PYPL":  ["PAYPAL", "PAYPAL HOLDINGS"],
    "COIN":  ["COINBASE", "COINBASE GLOBAL"],
    "XOM":   ["EXXON", "EXXONMOBIL", "EXXON MOBIL"],
    "BA":    ["BOEING", "THE BOEING COMPANY"],
    "RACE":  ["FERRARI", "FERRARI NV"],
}

# Set per tracciare URL già processati nella sessione corrente
_processed_urls: set[str] = set()


# ---------------------------------------------------------------------------
# Recupero URL ultimo file GKG
# ---------------------------------------------------------------------------

def get_latest_gkg_url() -> str | None:
    """
    Recupera l'URL dell'ultimo file GKG disponibile da lastupdate.txt.
    Il file ha 3 righe: Events, Mentions, GKG — prendiamo la terza (indice 2).
    """
    try:
        response = requests.get(GDELT_LASTUPDATE_URL, timeout=10)
        response.raise_for_status()
        lines   = response.text.strip().split("\n")
        gkg_url = lines[2].split(" ")[2].strip()
        log.info("URL GKG recuperato → %s", gkg_url)
        return gkg_url
    except Exception as e:
        log.error("Errore recupero lastupdate.txt → %s", e)
        return None


# ---------------------------------------------------------------------------
# Download e parsing del file GKG
# ---------------------------------------------------------------------------

def download_and_parse_gkg(gkg_url: str) -> pd.DataFrame | None:
    """
    Scarica il file GKG zip in RAM e lo parsa con pandas.
    Carica solo le colonne necessarie per risparmiare memoria.
    Restituisce un DataFrame filtrato o None in caso di errore.
    """
    try:
        log.info("Download file GKG in corso...")
        response = requests.get(gkg_url, timeout=60)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                # Legge senza nomi fissi — GKG v2 ha numero colonne variabile per file
                df_raw = pd.read_csv(
                    f,
                    sep="\t",
                    header=None,
                    dtype=str,
                    on_bad_lines="skip",
                )

        # Mappa colonne per posizione (indici fissi nel formato GKG 2.0)
        col_map = {0: "GKGRECORDID", 1: "DATE", 3: "SourceCommonName",
                   4: "DocumentIdentifier", 14: "V2Organizations", 15: "V2Tone"}
        available = {k: v for k, v in col_map.items() if k < len(df_raw.columns)}
        df = df_raw[list(available.keys())].rename(columns=available)

        log.info("File GKG parsato → %d righe totali", len(df))
        return df

    except zipfile.BadZipFile:
        log.warning("File GKG non ancora disponibile (BadZipFile) — riprovo al prossimo ciclo")
        return None
    except Exception as e:
        log.error("Errore download/parsing GKG → %s", e)
        return None


# ---------------------------------------------------------------------------
# Rilevamento aziende menzionate
# ---------------------------------------------------------------------------

def detect_mentions(orgs_text: str) -> list[str]:
    """
    Analizza il campo V2Organizations e restituisce i ticker
    delle aziende menzionate nell'articolo.
    """
    orgs_upper   = orgs_text.upper()
    mentioned    = []
    for ticker, aliases in COMPANIES.items():
        if any(alias in orgs_upper for alias in aliases):
            mentioned.append(ticker)
    return mentioned


# ---------------------------------------------------------------------------
# Parsing del V2Tone
# ---------------------------------------------------------------------------

def parse_tone(tone_str: str) -> tuple[float, str]:
    """
    Estrae il valore di sentiment dal campo V2Tone di GDELT.
    Formato: "Tone,PositiveScore,NegativeScore,Polarity,..."
    Tone è un float tra -100 (molto negativo) e +100 (molto positivo).
    Restituisce (score, label) dove label è positive/negative/neutral.
    """
    try:
        score = float(str(tone_str).split(",")[0])
    except (ValueError, AttributeError):
        score = 0.0

    if score > 1.0:
        label = "positive"
    elif score < -1.0:
        label = "negative"
    else:
        label = "neutral"

    return round(score, 4), label


# ---------------------------------------------------------------------------
# Creazione archi [:MENZIONATA_IN] in Neo4j
# ---------------------------------------------------------------------------

def create_mentions_relationships(mongo_id: str, tickers: list[str]) -> None:
    """
    Crea archi (:Event)-[:MENZIONATA_IN]->(:Company) in Neo4j
    per ogni ticker rilevato nel record GDELT.
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
        driver.close()
        log.info("Archi [:MENZIONATA_IN] → %s", tickers)
    except Neo4jError as e:
        log.error("Errore archi Neo4j → %s", e)


# ---------------------------------------------------------------------------
# Job principale
# ---------------------------------------------------------------------------

def ingestion_job() -> None:
    """
    Job principale schedulato ogni 15 minuti.
    1. Recupera URL ultimo file GKG
    2. Scarica e parsa il CSV in RAM
    3. Filtra le righe che menzionano le nostre aziende
    4. Per ogni record rilevante:
       a. Estrae V2Tone (sentiment pre-calcolato)
       b. Dual-write MongoDB + Neo4j
       c. Crea archi [:MENZIONATA_IN]
    """
    log.info("=" * 60)
    log.info("Avvio job ingestion GDELT — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # 1. Recupera URL
    gkg_url = get_latest_gkg_url()
    if not gkg_url:
        log.warning("URL GKG non disponibile — job saltato")
        return

    # Evita di riscaricare lo stesso file se il job gira più veloce dell'aggiornamento
    if gkg_url in _processed_urls:
        log.info("File GKG già processato in questo ciclo — salto")
        return

    # 2. Download e parsing
    df = download_and_parse_gkg(gkg_url)
    if df is None or df.empty:
        log.warning("DataFrame vuoto — job saltato")
        return

    # 3. Filtra righe senza organizzazioni
    df = df.dropna(subset=["V2Organizations"])

    total_saved    = 0
    total_filtered = 0

    with IngestionLayer() as il:
        for _, row in df.iterrows():
            url = str(row.get("DocumentIdentifier", "")).strip()

            # Anti-duplicati su URL
            if url in _processed_urls:
                total_filtered += 1
                continue

            # Rileva aziende menzionate
            mentioned_tickers = detect_mentions(str(row.get("V2Organizations", "")))
            if not mentioned_tickers:
                total_filtered += 1
                continue

            # Parsing V2Tone
            sentiment_score, sentiment_label = parse_tone(str(row.get("V2Tone", "0")))

            # Parsing data pubblicazione
            try:
                pub_date = datetime.strptime(
                    str(row["DATE"]), "%Y%m%d%H%M%S"
                ).replace(tzinfo=timezone.utc).isoformat()
            except (ValueError, KeyError):
                pub_date = datetime.now(timezone.utc).isoformat()

            payload = {
                "gdelt_id":        str(row.get("GKGRECORDID", "")),
                "url":             url,
                "publisher":       str(row.get("SourceCommonName", "")),
                "published_at":    pub_date,
                "mentions":        mentioned_tickers,
                "sentiment_score": sentiment_score,
                "sentiment_label": sentiment_label,
                "language":        "en",
                "gkg_url":         gkg_url,
            }

            # Dual-write MongoDB + Neo4j
            mongo_id = il.dual_write(
                source     = "gdelt",
                data_type  = "event",
                node_label = "Event",
                payload    = payload,
            )

            if mongo_id:
                create_mentions_relationships(mongo_id, mentioned_tickers)
                _processed_urls.add(url)
                total_saved += 1
                log.info(
                    "✓ GDELT | tone: %.2f (%s) | menzioni: %s | %s",
                    sentiment_score,
                    sentiment_label,
                    mentioned_tickers,
                    url[:60],
                )

    # Segna il file GKG come processato
    _processed_urls.add(gkg_url)

    log.info("=" * 60)
    log.info(
        "Job completato → salvati: %d | filtrati: %d",
        total_saved, total_filtered
    )
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Avvio ingest_gdelt.py")
    log.info("Aziende monitorate: %d", len(COMPANIES))

    # Esegui il job una volta e poi esci
    ingestion_job()
    log.info("Job completato. Uscita.")
