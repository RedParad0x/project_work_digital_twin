"""
nlp_layer.py
============
Worker NLP del Media Digital Twin.

Legge i documenti non ancora processati da MongoDB (sentiment_label = null),
applica il modello di sentiment corretto in base alla lingua e alla sorgente,
e aggiorna i nodi Event in Neo4j con sentiment_label e sentiment_score.

Modelli utilizzati:
  - MilaNLProc/feel-it-italian-sentiment → testi in italiano (IT)
  - cardiffnlp/twitter-roberta-base-sentiment-latest → testi in inglese social (EN)
  - ProsusAI/finbert → articoli finanziari in inglese (NewsAPI)

Sorgenti gestite:
  - reddit   → titolo + commenti aggregati
  - newsapi  → titolo + descrizione
  - youtube  → titolo + commenti aggregati
  - gdelt    → già ha sentiment (V2Tone), skippa NLP
  - yfinance → dati numerici, skippa NLP
  - google_trends → dati numerici, skippa NLP

Ottimizzazioni CPU:
  - Modelli caricati una volta sola all'avvio
  - Batch size 16 per inferenza
  - Limite 500 documenti per ciclo
  - Bulk write su Neo4j

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

import schedule
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from transformers import pipeline

# ---------------------------------------------------------------------------
# Configurazione logging (terminale + file)
# ---------------------------------------------------------------------------
start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_DIR    = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "nlp_layer")
LOG_FILE   = os.path.join(LOG_DIR, f"nlp_{start_time}.log")
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

log = logging.getLogger("nlp_layer")
log.setLevel(logging.INFO)

log.info("Logging configurato correttamente")

# Signal handler per flushare i log su interruzione
def signal_handler(sig, frame):
    _file_handler.flush()
    logging.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente
# ---------------------------------------------------------------------------
load_dotenv()

MONGO_URI      = os.getenv("MONGO_URI")
MONGO_DB_NAME  = os.getenv("MONGO_DB_NAME")
NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
HF_TOKEN       = os.getenv("HF_TOKEN")

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------
BATCH_SIZE      = 16    # documenti per batch di inferenza
MAX_DOCS_CYCLE  = 500   # documenti max per ciclo
MAX_TEXT_LENGTH = 512   # caratteri max per testo (limite modelli transformer)

# Sorgenti da skippare — hanno già sentiment o non hanno testo
SKIP_SOURCES = {"gdelt", "yfinance", "google_trends"}

# Modelli HuggingFace
# Modello multilingua per testi social (IT, EN, ES, FR, DE, PT, ...)
MODEL_MULTI = "tabularisai/multilingual-sentiment-analysis"
# Modello specializzato per testi finanziari formali (NewsAPI)
MODEL_FIN   = "ProsusAI/finbert"

# ---------------------------------------------------------------------------
# Caricamento modelli — una volta sola all'avvio
# ---------------------------------------------------------------------------

def load_models() -> dict:
    """
    Carica i due modelli NLP in memoria.
    - tabularisai/multilingual-sentiment-analysis → testi social (IT, EN, ES, FR, DE, PT)
    - ProsusAI/finbert → articoli finanziari formali (NewsAPI)

    Su CPU il caricamento richiede circa 1 minuto al primo avvio.
    """
    log.info("Caricamento modelli NLP in corso (CPU)...")

    models = {}

    try:
        log.info("Caricamento modello multilingua (tabularisai)...")
        models["multi"] = pipeline(
            "text-classification",
            model     = MODEL_MULTI,
            tokenizer = MODEL_MULTI,
            token     = HF_TOKEN,
            device    = -1,  # CPU
        )
        log.info("Modello multilingua caricato ✓")
    except Exception as e:
        log.error("Errore caricamento modello multilingua → %s", e)
        models["multi"] = None

    try:
        log.info("Caricamento FinBERT (inglese finanziario)...")
        models["fin"] = pipeline(
            "text-classification",
            model     = MODEL_FIN,
            tokenizer = MODEL_FIN,
            token     = HF_TOKEN,
            device    = -1,
        )
        log.info("FinBERT caricato ✓")
    except Exception as e:
        log.error("Errore caricamento FinBERT → %s", e)
        models["fin"] = None

    log.info("Modelli caricati: %d/2", sum(1 for m in models.values() if m))
    return models


# ---------------------------------------------------------------------------
# Estrazione testo dal documento
# ---------------------------------------------------------------------------

def extract_text(doc: dict) -> str:
    """
    Estrae e aggrega il testo dal documento MongoDB.
    Combina titolo/testo principale + commenti.
    Tronca a MAX_TEXT_LENGTH caratteri per rispettare i limiti dei modelli.
    """
    payload = doc.get("payload", {})
    source  = doc.get("source", "")
    parts   = []

    # Testo principale
    for field in ["title", "body", "description", "content"]:
        val = payload.get(field, "")
        if val and isinstance(val, str):
            parts.append(val.strip())
            break

    # Commenti aggregati
    comments = payload.get("comments", [])
    if comments:
        if isinstance(comments[0], dict):
            comment_texts = [c.get("text", "") for c in comments if c.get("text")]
        else:
            comment_texts = [str(c) for c in comments if c]

        # Prendi i primi 5 commenti per non superare il limite
        combined_comments = " ".join(comment_texts[:5])
        if combined_comments:
            parts.append(combined_comments)

    full_text = " ".join(parts)

    # Tronca al limite del modello
    return full_text[:MAX_TEXT_LENGTH].strip()


# ---------------------------------------------------------------------------
# Selezione modello per documento
# ---------------------------------------------------------------------------

def select_model_key(doc: dict) -> str:
    """
    Seleziona il modello corretto in base alla sorgente.

    Logica:
    - newsapi → FinBERT (testo finanziario formale in inglese)
    - tutto il resto → tabularisai multilingua (IT, EN, ES, FR, DE, PT...)
    """
    source = doc.get("source", "")

    if source == "newsapi":
        return "fin"

    return "multi"


# ---------------------------------------------------------------------------
# Normalizzazione label
# ---------------------------------------------------------------------------

def normalize_label(label: str, model_key: str) -> str:
    """
    Normalizza le label dei diversi modelli in un formato uniforme:
    positive / negative / neutral

    tabularisai labels: Very Negative, Negative, Neutral, Positive, Very Positive
    FinBERT labels:     positive / negative / neutral
    """
    label_lower = label.lower()

    # tabularisai usa 5 livelli — li mappiamo a 3
    if "very negative" in label_lower or label_lower == "very negative":
        return "negative"
    if "very positive" in label_lower or label_lower == "very positive":
        return "positive"

    # Normalizzazione standard per tutti i modelli
    if "pos" in label_lower:
        return "positive"
    if "neg" in label_lower:
        return "negative"
    if "neu" in label_lower:
        return "neutral"

    # Fallback LABEL_0/1/2 (TweetEval legacy)
    if label_lower == "label_0":
        return "negative"
    if label_lower == "label_1":
        return "neutral"
    if label_lower == "label_2":
        return "positive"

    return "neutral"


# ---------------------------------------------------------------------------
# Inferenza batch
# ---------------------------------------------------------------------------

def run_inference_batch(
    texts: list[str],
    model_pipeline,
    model_key: str,
) -> list[tuple[str, float]]:
    """
    Esegue l'inferenza su un batch di testi.
    Restituisce lista di (label, score) normalizzati.
    """
    try:
        results = model_pipeline(
            texts,
            batch_size     = BATCH_SIZE,
            truncation     = True,
            max_length     = 512,
            padding        = True,
        )
        return [
            (normalize_label(r["label"], model_key), round(r["score"], 4))
            for r in results
        ]
    except Exception as e:
        log.error("Errore inferenza batch → %s", e)
        return [("neutral", 0.0)] * len(texts)


# ---------------------------------------------------------------------------
# Aggiornamento Neo4j bulk
# ---------------------------------------------------------------------------

def update_neo4j_bulk(updates: list[dict]) -> None:
    """
    Aggiorna in bulk i nodi Event in Neo4j con sentiment_label e sentiment_score.
    Usa UNWIND per aggiornare tutti i nodi in una singola transazione.
    """
    if not updates:
        return

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            session.run("""
                UNWIND $updates AS upd
                MATCH (e:Event {mongo_id: upd.mongo_id})
                SET e.sentiment_label = upd.sentiment_label,
                    e.sentiment_score = upd.sentiment_score,
                    e.nlp_model       = upd.nlp_model,
                    e.nlp_processed_at = upd.processed_at
            """, updates=updates)
        driver.close()
        log.info("Neo4j bulk update → %d nodi aggiornati", len(updates))
    except Neo4jError as e:
        log.error("Errore Neo4j bulk update → %s", e)


# ---------------------------------------------------------------------------
# Aggiornamento MongoDB
# ---------------------------------------------------------------------------

def update_mongo_bulk(collection, updates: list[dict]) -> None:
    """
    Aggiorna in bulk i documenti MongoDB con sentiment_label e sentiment_score
    nel payload, per mantenere la coerenza tra i due DB.
    """
    if not updates:
        return

    try:
        from pymongo import UpdateOne
        operations = [
            UpdateOne(
                {"_id": upd["mongo_id"]},
                {"$set": {
                    "payload.sentiment_label": upd["sentiment_label"],
                    "payload.sentiment_score": upd["sentiment_score"],
                    "payload.nlp_model":       upd["nlp_model"],
                    "payload.nlp_processed_at": upd["processed_at"],
                }}
            )
            for upd in updates
        ]
        result = collection.bulk_write(operations)
        log.info("MongoDB bulk update → %d documenti aggiornati", result.modified_count)
    except PyMongoError as e:
        log.error("Errore MongoDB bulk update → %s", e)


# ---------------------------------------------------------------------------
# Job principale
# ---------------------------------------------------------------------------

def nlp_job(models: dict) -> None:
    """
    Job principale schedulato ogni 2 ore.
    1. Legge fino a MAX_DOCS_CYCLE documenti non processati da MongoDB
    2. Raggruppa per modello (it, en, fin)
    3. Esegue inferenza batch per ogni gruppo
    4. Aggiorna MongoDB e Neo4j in bulk
    """
    log.info("=" * 60)
    log.info("Avvio job NLP — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # Connessione MongoDB
    try:
        client     = MongoClient(MONGO_URI)
        db         = client[MONGO_DB_NAME]
        collection = db["raw_data"]
    except PyMongoError as e:
        log.error("Errore connessione MongoDB → %s", e)
        return

    # Recupera documenti non processati
    # Filtra per sorgenti che hanno testo da analizzare
    docs = list(collection.find(
        {
            "source":                  {"$nin": list(SKIP_SOURCES)},
            "payload.sentiment_label": {"$exists": False},
        },
        limit = MAX_DOCS_CYCLE,
    ))

    if not docs:
        log.info("Nessun documento da processare — job completato")
        client.close()
        return

    log.info("Documenti da processare: %d", len(docs))

    # Raggruppa per modello
    groups: dict[str, list[dict]] = {"multi": [], "fin": []}
    for doc in docs:
        model_key = select_model_key(doc)
        groups[model_key].append(doc)

    log.info(
        "Distribuzione → MULTI: %d | FIN: %d",
        len(groups["multi"]), len(groups["fin"])
    )

    all_updates = []

    # Processa ogni gruppo
    for model_key, group_docs in groups.items():
        if not group_docs:
            continue

        model_pipeline = models.get(model_key)
        if model_pipeline is None:
            log.warning("Modello %s non disponibile — salto %d documenti", model_key, len(group_docs))
            continue

        model_name = {
            "multi": MODEL_MULTI,
            "fin":   MODEL_FIN,
        }[model_key]

        log.info("Inferenza %s → %d documenti...", model_key.upper(), len(group_docs))

        # Estrai testi
        texts    = [extract_text(doc) for doc in group_docs]
        valid    = [(i, t) for i, t in enumerate(texts) if t.strip()]
        skipped  = len(texts) - len(valid)

        if skipped > 0:
            log.warning("%d documenti saltati per testo vuoto", skipped)

        if not valid:
            continue

        valid_indices, valid_texts = zip(*valid)

        # Inferenza batch
        results = run_inference_batch(list(valid_texts), model_pipeline, model_key)

        processed_at = datetime.now(timezone.utc).isoformat()

        for idx, (label, score) in zip(valid_indices, results):
            doc = group_docs[idx]
            all_updates.append({
                "mongo_id":       str(doc["_id"]),
                "sentiment_label": label,
                "sentiment_score": score,
                "nlp_model":       model_name,
                "processed_at":    processed_at,
            })

        log.info(
            "%s completato → pos: %d | neg: %d | neu: %d",
            model_key.upper(),
            sum(1 for u in all_updates if u["sentiment_label"] == "positive"),
            sum(1 for u in all_updates if u["sentiment_label"] == "negative"),
            sum(1 for u in all_updates if u["sentiment_label"] == "neutral"),
        )

    # Bulk update MongoDB e Neo4j
    if all_updates:
        update_mongo_bulk(collection, all_updates)
        update_neo4j_bulk(all_updates)

    client.close()

    log.info("=" * 60)
    log.info(
        "Job completato → processati: %d | aggiornati: %d",
        len(docs), len(all_updates)
    )
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Avvio nlp_layer.py")
    log.info("Device: CPU")
    log.info("Batch size: %d", BATCH_SIZE)
    log.info("Max documenti per ciclo: %d", MAX_DOCS_CYCLE)
    log.info("Sorgenti skippate (hanno già sentiment): %s", SKIP_SOURCES)

    # Carica modelli una volta sola
    models = load_models()

    if not any(models.values()):
        log.error("Nessun modello caricato — uscita")
        sys.exit(1)

    # Esegui il job una volta e poi esci
    nlp_job(models)
    log.info("Job completato. Uscita.")
