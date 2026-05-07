from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from neo4j import GraphDatabase
from pymongo import MongoClient

load_dotenv()

router = APIRouter(prefix="/compat", tags=["Streamlit compatibility"])

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "media_twin")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mediatwin1")

PROJECT_ROOT = Path(os.getenv("MEDIA_TWIN_ROOT", Path(__file__).resolve().parent.parent)).resolve()

SOURCES = ["gdelt", "newsapi", "reddit", "youtube", "yfinance", "google_trends"]
COMPANIES = ["NVDA","TSLA","AAPL","META","GOOGL","MSFT","DIS","NFLX","AMZN","JPM","PYPL","COIN","XOM","BA","RACE"]

MONGO_PRESETS: dict[str, list[dict[str, Any]]] = {
    "documents_by_source": [
        {"$group": {"_id": "$source", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ],
    "avg_sentiment_by_source": [
        {"$match": {"payload.sentiment_score": {"$exists": True}}},
        {"$group": {"_id": "$source", "avg_score": {"$avg": "$payload.sentiment_score"}, "count": {"$sum": 1}}},
        {"$sort": {"avg_score": -1}},
    ],
    "top_mentioned_tickers": [
        {"$unwind": "$payload.mentions"},
        {"$group": {"_id": "$payload.mentions", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 15},
    ],
    "documents_by_day": [
        {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
        {"$group": {"_id": "$day", "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
        {"$limit": 21},
    ],
    "sentiment_by_ticker": [
        {"$match": {"payload.mentions": {"$exists": True}, "payload.sentiment_label": {"$exists": True}}},
        {"$unwind": "$payload.mentions"},
        {"$group": {"_id": {"ticker": "$payload.mentions", "label": "$payload.sentiment_label"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 40},
    ],
}

CYPHER_PRESETS: dict[str, str] = {
    "node_counts": "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count ORDER BY count DESC",
    "relationship_counts": "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC",
    "top_companies": """
        MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company)
        RETURN c.ticker AS ticker, c.name AS name, count(e) AS mentions
        ORDER BY mentions DESC LIMIT 15
    """,
    "negative_companies": """
        MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company)
        WHERE e.sentiment_label = 'negative'
        RETURN c.ticker AS ticker, count(e) AS negative_events
        ORDER BY negative_events DESC LIMIT 15
    """,
    "comentions": """
        MATCH (c1:Company)<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
        WHERE c1.ticker < c2.ticker
        RETURN c1.ticker AS source, c2.ticker AS target, count(e) AS weight
        ORDER BY weight DESC LIMIT 60
    """,
    "sector_hierarchy": """
        MATCH (c:Company)-[:BELONGS_TO]->(i:Industry)-[:PART_OF]->(s:Sector)
        RETURN s.name AS sector, i.name AS industry, collect(c.ticker) AS companies
        ORDER BY sector, industry
    """,
    "trending_topics_by_company": """
        MATCH (c:Company)-[:TRENDING_WITH]->(t:Topic)
        RETURN c.ticker AS ticker, collect(t.name)[..8] AS topics, count(t) AS topic_count
        ORDER BY topic_count DESC
    """,
    "recent_sentiment_events": """
        MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company)
        WHERE e.sentiment_label IS NOT NULL
        RETURN e.source AS source, e.title AS title, e.sentiment_label AS sentiment, c.ticker AS ticker
        ORDER BY e.ingested_at DESC LIMIT 30
    """,
    "company_centrality": """
        MATCH (c:Company)
        OPTIONAL MATCH (e:Event)-[:MENZIONATA_IN]->(c)
        RETURN c.ticker AS ticker, c.name AS name, count(e) AS degree, c.last_close AS last_close
        ORDER BY degree DESC
    """,
}

def mongo_collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.server_info()
    return client, client[MONGO_DB_NAME]["raw_data"]

def neo_driver():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    return driver

def clean_doc(doc: dict[str, Any]) -> dict[str, Any]:
    doc["_id"] = str(doc.get("_id", ""))
    return doc

def flatten_mongo_result(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flat: list[dict[str, Any]] = []
    for r in rows:
        row: dict[str, Any] = {}
        for k, v in r.items():
            if k == "_id" and isinstance(v, dict):
                row.update(v)
            elif k == "_id":
                row["key"] = v
            else:
                row[k] = round(v, 4) if isinstance(v, float) else v
        flat.append(row)
    return flat

@router.get("/mongo/schema")
def mongo_schema():
    client, col = mongo_collection()
    out = []
    comparison = {
        "gdelt": ["gdelt_id","url","publisher","published_at","mentions","sentiment_score","sentiment_label"],
        "newsapi": ["title","description","url","source_name","author","published_at","mentions"],
        "reddit": ["subreddit","post_id","title","body","score","num_comments","author","created_utc"],
        "youtube": ["video_id","title","channel","published_at","views","likes","comments"],
        "yfinance": ["ticker","timestamp","open","high","low","close","volume"],
        "google_trends": ["ticker","keyword","timeframe","interest_data","rising_queries","top_queries"],
    }

    for source in SOURCES:
        doc = col.find_one({"source": source}, sort=[("ingested_at", -1)])
        if doc:
            doc = clean_doc(doc)
            payload = doc.get("payload", {})
            out.append({
                "source": source,
                "exists": True,
                "sample": doc,
                "payload_keys": list(payload.keys()),
                "expected_fields": comparison.get(source, []),
            })
        else:
            out.append({
                "source": source,
                "exists": False,
                "sample": None,
                "payload_keys": [],
                "expected_fields": comparison.get(source, []),
            })

    client.close()
    return {"sources": out}

@router.get("/mongo/aggregate/{preset}")
def mongo_aggregate(preset: str):
    if preset not in MONGO_PRESETS:
        raise HTTPException(404, f"Preset Mongo non trovato: {preset}")
    client, col = mongo_collection()
    rows = list(col.aggregate(MONGO_PRESETS[preset]))
    client.close()
    return {
        "preset": preset,
        "rows": flatten_mongo_result(rows),
        "pipeline": MONGO_PRESETS[preset],
    }

@router.get("/mongo/sentiment-timeline")
def sentiment_timeline(
    source: str = Query("all"),
    ticker: str = Query("all"),
    days: int = Query(30, ge=1, le=365),
):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    q: dict[str, Any] = {"payload.sentiment_label": {"$exists": True}, "ingested_at": {"$gte": since}}

    if source != "all":
        q["source"] = source
    if ticker != "all":
        q["payload.mentions"] = ticker

    client, col = mongo_collection()
    rows = list(col.aggregate([
        {"$match": q},
        {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
        {"$group": {"_id": {"day": "$day", "source": "$source", "label": "$payload.sentiment_label"}, "count": {"$sum": 1}, "avg_score": {"$avg": "$payload.sentiment_score"}}},
        {"$sort": {"_id.day": 1}},
    ]))
    client.close()

    return {
        "rows": [
            {
                "day": r["_id"]["day"],
                "source": r["_id"]["source"],
                "sentiment": r["_id"]["label"],
                "count": r["count"],
                "avg_score": round(r.get("avg_score") or 0, 4),
            }
            for r in rows
        ]
    }

@router.get("/neo4j/query/{preset}")
def neo4j_query(preset: str):
    if preset not in CYPHER_PRESETS:
        raise HTTPException(404, f"Preset Neo4j non trovato: {preset}")

    driver = neo_driver()
    with driver.session() as s:
        rows = s.run(CYPHER_PRESETS[preset]).data()
    driver.close()

    return {"preset": preset, "rows": rows, "cypher": CYPHER_PRESETS[preset]}

@router.get("/company/{ticker}/full")
def company_full(ticker: str, days: int = Query(30, ge=1, le=365)):
    ticker = ticker.upper()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    client, col = mongo_collection()

    ohlcv_docs = list(col.find(
        {"source": "yfinance", "data_type": "ohlcv", "payload.ticker": ticker},
        {"payload": 1, "ingested_at": 1}
    ).sort("payload.timestamp", -1).limit(120))

    sentiment_rows = list(col.aggregate([
        {"$match": {"payload.mentions": ticker, "payload.sentiment_label": {"$exists": True}, "ingested_at": {"$gte": since}}},
        {"$addFields": {"day": {"$substr": ["$ingested_at", 0, 10]}}},
        {"$group": {"_id": {"day": "$day", "source": "$source", "label": "$payload.sentiment_label"}, "count": {"$sum": 1}, "avg_score": {"$avg": "$payload.sentiment_score"}}},
        {"$sort": {"_id.day": 1}},
    ]))

    recent_events = list(col.find(
        {"payload.mentions": ticker},
        {"source": 1, "ingested_at": 1, "payload.title": 1, "payload.url": 1, "payload.sentiment_label": 1, "payload.sentiment_score": 1, "payload.publisher": 1, "payload.source_name": 1}
    ).sort("ingested_at", -1).limit(60))

    client.close()

    graph = {"info": {}, "comentions": [], "topics": [], "sentiment_by_source": []}
    try:
        driver = neo_driver()
        with driver.session() as s:
            info = s.run("""
                MATCH (c:Company {ticker:$t})
                OPTIONAL MATCH (c)-[:BELONGS_TO]->(i:Industry)-[:PART_OF]->(sec:Sector)
                RETURN c.name AS name, c.last_close AS close, c.last_volume AS volume,
                       c.updated_at AS updated, i.name AS industry, sec.name AS sector
            """, t=ticker).single()

            comentions = s.run("""
                MATCH (c1:Company {ticker:$t})<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
                WHERE c2.ticker <> $t
                RETURN c2.ticker AS ticker, count(e) AS count
                ORDER BY count DESC LIMIT 15
            """, t=ticker).data()

            topics = s.run("""
                MATCH (c:Company {ticker:$t})-[:TRENDING_WITH]->(topic:Topic)
                RETURN topic.name AS topic LIMIT 30
            """, t=ticker).data()

            neo_sent = s.run("""
                MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker:$t})
                WHERE e.sentiment_label IS NOT NULL
                RETURN e.source AS source, e.sentiment_label AS sentiment, count(e) AS count
                ORDER BY count DESC
            """, t=ticker).data()

        driver.close()
        graph = {
            "info": dict(info) if info else {},
            "comentions": comentions,
            "topics": [r["topic"] for r in topics],
            "sentiment_by_source": neo_sent,
        }
    except Exception as e:
        graph["error"] = str(e)

    return {
        "ticker": ticker,
        "ohlcv": [d.get("payload", {}) for d in reversed(ohlcv_docs)],
        "sentiment_timeline": [
            {
                "day": r["_id"]["day"],
                "source": r["_id"]["source"],
                "sentiment": r["_id"]["label"],
                "count": r["count"],
                "avg_score": round(r.get("avg_score") or 0, 4),
            }
            for r in sentiment_rows
        ],
        "recent_events": [clean_doc(d) for d in recent_events],
        "graph": graph,
    }

@router.get("/compare/{ticker}/full")
def compare_full(ticker: str, days: int = Query(7, ge=1, le=365)):
    ticker = ticker.upper()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    client, col = mongo_collection()

    mongo_total = col.count_documents({"payload.mentions": ticker, "ingested_at": {"$gte": since}})
    mongo_by_src = list(col.aggregate([
        {"$match": {"payload.mentions": ticker, "ingested_at": {"$gte": since}}},
        {"$group": {"_id": "$source", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]))
    mongo_sent = list(col.aggregate([
        {"$match": {"payload.mentions": ticker, "payload.sentiment_label": {"$exists": True}, "ingested_at": {"$gte": since}}},
        {"$group": {"_id": "$payload.sentiment_label", "count": {"$sum": 1}, "avg_score": {"$avg": "$payload.sentiment_score"}}},
    ]))
    client.close()

    neo = {"total": 0, "comentions": [], "topics": [], "hierarchy": None, "sentiment": []}
    try:
        driver = neo_driver()
        with driver.session() as s:
            total = s.run("""
                MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker:$t})
                RETURN count(e) AS count
            """, t=ticker).single()

            comentions = s.run("""
                MATCH (c1:Company {ticker:$t})<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
                WHERE c2.ticker <> $t
                RETURN c2.ticker AS ticker, count(e) AS count
                ORDER BY count DESC LIMIT 15
            """, t=ticker).data()

            topics = s.run("""
                MATCH (c:Company {ticker:$t})-[:TRENDING_WITH]->(topic:Topic)
                RETURN topic.name AS topic LIMIT 20
            """, t=ticker).data()

            hierarchy = s.run("""
                MATCH (c:Company {ticker:$t})-[:BELONGS_TO]->(i:Industry)-[:PART_OF]->(sec:Sector)
                RETURN i.name AS industry, sec.name AS sector
            """, t=ticker).single()

            sentiment = s.run("""
                MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker:$t})
                WHERE e.sentiment_label IS NOT NULL
                RETURN e.sentiment_label AS sentiment, count(e) AS count
                ORDER BY count DESC
            """, t=ticker).data()

        driver.close()
        neo = {
            "total": total["count"] if total else 0,
            "comentions": comentions,
            "topics": [r["topic"] for r in topics],
            "hierarchy": dict(hierarchy) if hierarchy else None,
            "sentiment": sentiment,
        }
    except Exception as e:
        neo["error"] = str(e)

    return {
        "ticker": ticker,
        "days": days,
        "mongo": {
            "total": mongo_total,
            "by_source": flatten_mongo_result(mongo_by_src),
            "sentiment": flatten_mongo_result(mongo_sent),
        },
        "neo4j": neo,
        "explanation": {
            "mongodb_strength": "volume, raw heterogeneous documents, aggregation pipelines, temporal analysis",
            "neo4j_strength": "relationships, traversals, co-mentions, sector hierarchy, graph patterns",
        },
    }
