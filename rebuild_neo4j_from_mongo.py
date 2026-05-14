from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
from neo4j import GraphDatabase

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "media_twin")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mediatwin1")

COMPANIES = {
    "NVDA": {"name": "NVIDIA", "industry": "Semiconductors", "sector": "Technology"},
    "TSLA": {"name": "Tesla", "industry": "Auto Manufacturers", "sector": "Consumer Cyclical"},
    "AAPL": {"name": "Apple", "industry": "Consumer Electronics", "sector": "Technology"},
    "META": {"name": "Meta Platforms", "industry": "Internet Content & Information", "sector": "Communication Services"},
    "GOOGL": {"name": "Alphabet", "industry": "Internet Content & Information", "sector": "Communication Services"},
    "MSFT": {"name": "Microsoft", "industry": "Software", "sector": "Technology"},
    "DIS": {"name": "Disney", "industry": "Entertainment", "sector": "Communication Services"},
    "NFLX": {"name": "Netflix", "industry": "Entertainment", "sector": "Communication Services"},
    "AMZN": {"name": "Amazon", "industry": "Internet Retail", "sector": "Consumer Cyclical"},
    "JPM": {"name": "JPMorgan Chase", "industry": "Banks", "sector": "Financial Services"},
    "PYPL": {"name": "PayPal", "industry": "Credit Services", "sector": "Financial Services"},
    "COIN": {"name": "Coinbase", "industry": "Financial Data & Stock Exchanges", "sector": "Financial Services"},
    "XOM": {"name": "Exxon Mobil", "industry": "Oil & Gas Integrated", "sector": "Energy"},
    "BA": {"name": "Boeing", "industry": "Aerospace & Defense", "sector": "Industrials"},
    "RACE": {"name": "Ferrari", "industry": "Auto Manufacturers", "sector": "Consumer Cyclical"},
}

def norm_mentions(value):
    if not value:
        return []
    if isinstance(value, list):
        return [str(x).upper().strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        return [value.upper().strip()]
    return []

def event_title(payload):
    return (
        payload.get("title")
        or payload.get("video_title")
        or payload.get("post_title")
        or payload.get("headline")
        or payload.get("description")
        or "Untitled event"
    )

def event_url(payload):
    return (
        payload.get("url")
        or payload.get("video_url")
        or payload.get("post_url")
        or ""
    )

def main():
    mongo = MongoClient(MONGO_URI)
    col = mongo[MONGO_DB_NAME]["raw_data"]

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
    )

    docs = list(col.find({}))
    print(f"Documenti MongoDB letti: {len(docs)}")

    with driver.session() as session:
        # Pulizia relazioni e nodi ricostruibili
        session.run("MATCH ()-[r]->() DELETE r")
        session.run("MATCH (n:Company) DELETE n")
        session.run("MATCH (n:Industry) DELETE n")
        session.run("MATCH (n:Sector) DELETE n")

        # Crea aziende + gerarchia settore
        for ticker, meta in COMPANIES.items():
            session.run(
                """
                MERGE (c:Company {ticker: $ticker})
                SET c.name = $name

                MERGE (i:Industry {name: $industry})
                MERGE (s:Sector {name: $sector})

                MERGE (c)-[:BELONGS_TO]->(i)
                MERGE (i)-[:PART_OF]->(s)
                """,
                ticker=ticker,
                name=meta["name"],
                industry=meta["industry"],
                sector=meta["sector"],
            )

        print("Company / Industry / Sector creati.")

        linked_events = 0
        mention_rels = 0

        # Crea/aggiorna Event e relazioni MENZIONATA_IN
        for doc in docs:
            payload = doc.get("payload", {}) or {}
            mentions = norm_mentions(payload.get("mentions"))

            mentions = [m for m in mentions if m in COMPANIES]

            if not mentions:
                continue

            mongo_id = str(doc.get("_id"))
            title = event_title(payload)
            url = event_url(payload)
            source = doc.get("source", "")
            data_type = doc.get("data_type", "")
            ingested_at = doc.get("ingested_at", "")

            session.run(
                """
                MERGE (e:Event {mongo_id: $mongo_id})
                SET e.title = $title,
                    e.url = $url,
                    e.source = $source,
                    e.data_type = $data_type,
                    e.ingested_at = $ingested_at
                """,
                mongo_id=mongo_id,
                title=title,
                url=url,
                source=source,
                data_type=data_type,
                ingested_at=ingested_at,
            )

            linked_events += 1

            for ticker in mentions:
                session.run(
                    """
                    MATCH (e:Event {mongo_id: $mongo_id})
                    MATCH (c:Company {ticker: $ticker})
                    MERGE (e)-[:MENZIONATA_IN]->(c)
                    """,
                    mongo_id=mongo_id,
                    ticker=ticker,
                )
                mention_rels += 1

        print(f"Event collegati: {linked_events}")
        print(f"Relazioni MENZIONATA_IN create: {mention_rels}")

        # Fallback topic: crea topic da Google Trends se esistono documenti
        trend_docs = list(col.find({"source": "google_trends"}))
        topic_rels = 0

        for doc in trend_docs:
            payload = doc.get("payload", {}) or {}

            ticker = str(payload.get("ticker") or "").upper().strip()
            if ticker not in COMPANIES:
                # fallback: prova mentions
                mentions = norm_mentions(payload.get("mentions"))
                ticker = next((m for m in mentions if m in COMPANIES), "")

            if ticker not in COMPANIES:
                continue

            possible_topics = []

            for key in ["keyword", "query", "name"]:
                if payload.get(key):
                    possible_topics.append(str(payload.get(key)))

            for key in ["top_queries", "rising_queries", "related_queries"]:
                value = payload.get(key)
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            q = item.get("query") or item.get("name") or item.get("keyword")
                            if q:
                                possible_topics.append(str(q))
                        elif isinstance(item, str):
                            possible_topics.append(item)

            for topic in set(t.strip() for t in possible_topics if t.strip()):
                session.run(
                    """
                    MATCH (c:Company {ticker: $ticker})
                    MERGE (t:Topic {name: $topic})
                    MERGE (c)-[:TRENDING_WITH]->(t)
                    """,
                    ticker=ticker,
                    topic=topic,
                )
                topic_rels += 1

        print(f"Relazioni TRENDING_WITH create: {topic_rels}")

        # Aggiorna last_close / volume da yfinance se disponibili
        yf_docs = list(col.find({"source": "yfinance"}).sort("ingested_at", -1))
        updated_prices = 0

        for doc in yf_docs:
            payload = doc.get("payload", {}) or {}
            ticker = str(payload.get("ticker") or "").upper().strip()
            if ticker not in COMPANIES:
                continue

            close = payload.get("close")
            volume = payload.get("volume")

            session.run(
                """
                MATCH (c:Company {ticker: $ticker})
                SET c.last_close = $close,
                    c.last_volume = $volume
                """,
                ticker=ticker,
                close=close,
                volume=volume,
            )
            updated_prices += 1

        print(f"Prezzi aggiornati da yfinance: {updated_prices}")

        # Verifica finale
        result = session.run(
            """
            MATCH (n)
            RETURN labels(n)[0] AS label, count(n) AS count
            ORDER BY count DESC
            """
        ).data()

        rels = session.run(
            """
            MATCH ()-[r]->()
            RETURN type(r) AS rel, count(r) AS count
            ORDER BY count DESC
            """
        ).data()

        print("\\nNODI:")
        for row in result:
            print(row)

        print("\\nRELAZIONI:")
        for row in rels:
            print(row)

    mongo.close()
    driver.close()

if __name__ == "__main__":
    main()