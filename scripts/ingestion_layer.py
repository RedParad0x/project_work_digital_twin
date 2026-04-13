"""
ingestion_layer.py
==================
Modulo core del Media Digital Twin.
Implementa la logica di Dual-Write verso:
  - MongoDB  (Data Lake  → collezione raw_data)
  - Neo4j    (Knowledge Graph → nodi Asset / Event)

Il campo _id viene generato in Python (UUID4) e usato come
chiave di coerenza tra i due database (mongo_id nel nodo Neo4j).

Autori : Massimo Mazzini, Rafaele Rossi
Corso  : Big Data Engineer & Solution Architect — 2° anno
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from dotenv import load_dotenv
import os

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ---------------------------------------------------------------------------
# Configurazione logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingestion_layer")

# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente dal file .env
# ---------------------------------------------------------------------------
load_dotenv()

MONGO_URI      = os.getenv("MONGO_URI")
MONGO_DB_NAME  = os.getenv("MONGO_DB_NAME")
NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")


# ---------------------------------------------------------------------------
# Tipi supportati
# ---------------------------------------------------------------------------

# Sorgenti dati gestite dal layer
SourceType = Literal["yfinance", "newsapi", "reddit", "gdelt", "youtube", "google_trends"]

# Label Neo4j: Asset per entità finanziarie, Event per notizie/post
NodeLabel = Literal["Asset", "Event"]


# ---------------------------------------------------------------------------
# Classe principale
# ---------------------------------------------------------------------------

class IngestionLayer:
    """
    Gestisce le connessioni a MongoDB e Neo4j ed espone
    il metodo dual_write() per la scrittura atomica su entrambi i DB.
    """

    def __init__(self) -> None:
        # -- Connessione MongoDB --
        self._mongo_client = MongoClient(MONGO_URI)
        self._db           = self._mongo_client[MONGO_DB_NAME]
        self._collection   = self._db["raw_data"]
        log.info("MongoDB connesso → DB: %s | Collezione: raw_data", MONGO_DB_NAME)

        # -- Connessione Neo4j --
        self._neo4j_driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        log.info("Neo4j connesso → %s", NEO4J_URI)

    # -----------------------------------------------------------------------
    # Metodo pubblico principale
    # -----------------------------------------------------------------------

    def dual_write(
        self,
        source: SourceType,
        data_type: str,
        node_label: NodeLabel,
        payload: dict[str, Any],
    ) -> str | None:
        """
        Scrive il dato grezzo su MongoDB e crea/aggiorna il nodo
        corrispondente in Neo4j. Restituisce il mongo_id generato,
        oppure None in caso di errore.

        Parametri
        ----------
        source      : sorgente del dato (es. "yfinance", "newsapi")
        data_type   : tipo semantico (es. "asset", "event", "social")
        node_label  : label del nodo Neo4j ("Asset" o "Event")
        payload     : dizionario con i dati specifici della sorgente
        """

        # 1. Genera l'ID univoco in Python — sarà la chiave di coerenza
        mongo_id    = str(uuid.uuid4())
        ingested_at = datetime.now(timezone.utc).isoformat()

        # 2. Costruisce il documento con l'envelope standard
        document = {
            "_id":         mongo_id,
            "source":      source,
            "data_type":   data_type,
            "ingested_at": ingested_at,
            "payload":     payload,
        }

        # 3. Scrittura su MongoDB
        mongo_ok = self._write_mongo(document)
        if not mongo_ok:
            # Se MongoDB fallisce non proviamo nemmeno Neo4j
            return None

        # 4. Scrittura su Neo4j (con rollback compensatorio se fallisce)
        neo4j_ok = self._write_neo4j(
            mongo_id    = mongo_id,
            node_label  = node_label,
            source      = source,
            ingested_at = ingested_at,
            payload     = payload,
        )
        if not neo4j_ok:
            # Rollback compensatorio: rimuove il documento da MongoDB
            self._rollback_mongo(mongo_id)
            return None

        log.info("Dual-write completato → mongo_id: %s | label: %s", mongo_id, node_label)
        return mongo_id

    # -----------------------------------------------------------------------
    # Metodi privati
    # -----------------------------------------------------------------------

    def _write_mongo(self, document: dict[str, Any]) -> bool:
        """Inserisce il documento nella collezione raw_data."""
        try:
            self._collection.insert_one(document)
            log.info("MongoDB ✓ → _id: %s", document["_id"])
            return True
        except PyMongoError as e:
            log.error("MongoDB ✗ → %s", e)
            return False

    def _write_neo4j(
        self,
        mongo_id: str,
        node_label: NodeLabel,
        source: str,
        ingested_at: str,
        payload: dict[str, Any],
    ) -> bool:
        """
        Crea o aggiorna un nodo Neo4j con MERGE (evita duplicati).
        I campi del nodo variano in base alla label:
          - Asset : ticker, name, market, last_close
          - Event : title, published_at, sentiment_label, sentiment_score
        """
        try:
            with self._neo4j_driver.session() as session:
                if node_label == "Asset":
                    session.execute_write(
                        self._merge_asset_node,
                        mongo_id, source, ingested_at, payload
                    )
                elif node_label == "Event":
                    session.execute_write(
                        self._merge_event_node,
                        mongo_id, source, ingested_at, payload
                    )
                else:
                    log.error("NodeLabel non supportata: %s", node_label)
                    return False

            log.info("Neo4j ✓ → nodo %s creato/aggiornato", node_label)
            return True

        except Neo4jError as e:
            log.error("Neo4j ✗ → %s", e)
            return False

    def _rollback_mongo(self, mongo_id: str) -> None:
        """
        Rollback compensatorio: elimina da MongoDB il documento
        appena inserito quando Neo4j ha fallito.
        """
        try:
            self._collection.delete_one({"_id": mongo_id})
            log.warning("Rollback MongoDB eseguito → _id: %s rimosso", mongo_id)
        except PyMongoError as e:
            log.error("Rollback MongoDB fallito → %s", e)

    # -----------------------------------------------------------------------
    # Transazioni Neo4j (metodi statici passati a execute_write)
    # -----------------------------------------------------------------------

    @staticmethod
    def _merge_asset_node(tx, mongo_id, source, ingested_at, payload):
        """
        MERGE su ticker: se il nodo esiste già ne aggiorna i prezzi,
        altrimenti lo crea. Evita nodi duplicati per lo stesso asset.
        """
        query = """
        MERGE (a:Asset {ticker: $ticker})
        ON CREATE SET
            a.mongo_id   = $mongo_id,
            a.name       = $name,
            a.market     = $market,
            a.source     = $source,
            a.created_at = $ingested_at,
            a.last_close = $last_close,
            a.updated_at = $ingested_at
        ON MATCH SET
            a.last_close = $last_close,
            a.updated_at = $ingested_at
        """
        tx.run(query,
            mongo_id    = mongo_id,
            ticker      = payload.get("ticker", "UNKNOWN"),
            name        = payload.get("name", ""),
            market      = payload.get("market", ""),
            source      = source,
            ingested_at = ingested_at,
            last_close  = payload.get("close", 0.0),
        )

    @staticmethod
    def _merge_event_node(tx, mongo_id, source, ingested_at, payload):
        """
        MERGE su mongo_id: ogni evento è unico, identificato dal suo ID.
        I campi sentiment partono a null e vengono popolati dal layer NLP.
        """
        query = """
        MERGE (e:Event {mongo_id: $mongo_id})
        ON CREATE SET
            e.source          = $source,
            e.title           = $title,
            e.published_at    = $published_at,
            e.ingested_at     = $ingested_at,
            e.sentiment_label = null,
            e.sentiment_score = null
        """
        tx.run(query,
            mongo_id     = mongo_id,
            source       = source,
            title        = payload.get("title", payload.get("body", "")[:80]),
            published_at = payload.get("published_at", payload.get("created_utc", ingested_at)),
            ingested_at  = ingested_at,
        )

    # -----------------------------------------------------------------------
    # Context manager — uso consigliato: with IngestionLayer() as il:
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Chiude le connessioni ai database."""
        self._mongo_client.close()
        self._neo4j_driver.close()
        log.info("Connessioni chiuse.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ---------------------------------------------------------------------------
# Blocco __main__ — test rapido eseguibile direttamente
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    with IngestionLayer() as il:

        # -- Test 1: dato finanziario da yfinance (nodo Asset) --
        id_asset = il.dual_write(
            source     = "yfinance",
            data_type  = "asset",
            node_label = "Asset",
            payload    = {
                "ticker": "ENI.MI",
                "name":   "Eni S.p.A.",
                "market": "MIL",
                "date":   "2025-01-01",
                "open":   14.20,
                "high":   14.85,
                "low":    14.10,
                "close":  14.75,
                "volume": 9_200_000,
            }
        )
        print(f"\n[TEST 1] Asset inserito → mongo_id: {id_asset}")

        # -- Test 2: articolo da NewsAPI (nodo Event) --
        id_event = il.dual_write(
            source     = "newsapi",
            data_type  = "event",
            node_label = "Event",
            payload    = {
                "title":        "Eni annuncia nuovo piano industriale",
                "source_name":  "Il Sole 24 Ore",
                "author":       "Mario Rossi",
                "published_at": "2025-01-01T08:30:00Z",
                "url":          "https://ilsole24ore.com/esempio",
                "description":  "Descrizione breve...",
                "content":      "Testo completo dell'articolo...",
                "entities":     ["ENI", "MILANO"],
            }
        )
        print(f"[TEST 2] Event inserito → mongo_id: {id_event}")

        # -- Test 3: post Reddit (nodo Event) --
        id_reddit = il.dual_write(
            source     = "reddit",
            data_type  = "social",
            node_label = "Event",
            payload    = {
                "subreddit":    "italy",
                "post_id":      "abc123",
                "title":        "Cosa pensate di Eni?",
                "body":         "Testo del post...",
                "score":        145,
                "num_comments": 32,
                "created_utc":  "2025-01-01T09:00:00Z",
                "author":       "u/username",
            }
        )
        print(f"[TEST 3] Reddit post inserito → mongo_id: {id_reddit}")