from __future__ import annotations

import glob
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from pymongo import MongoClient
import yfinance as yf

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "media_twin")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "mediatwin1")

PROJECT_ROOT = Path(os.getenv("MEDIA_TWIN_ROOT", Path(__file__).resolve().parent.parent)).resolve()
LOGS_DIR = PROJECT_ROOT.parent / "logs"
if not LOGS_DIR.exists():
    LOGS_DIR = PROJECT_ROOT / "logs"

FRONTEND_RUN_LOGS = LOGS_DIR / "frontend_runs"
FRONTEND_RUN_LOGS.mkdir(parents=True, exist_ok=True)

PIPELINE = [
    {"key": "yfinance", "file": "ingest_yfinance.py", "label": "YFinance", "emoji": "📈", "desc": "Prezzi OHLCV Yahoo Finance", "log_dir": "ingestion_yfinance"},
    {"key": "gdelt", "file": "ingest_gdelt.py", "label": "GDELT", "emoji": "🌍", "desc": "Eventi globali GDELT", "log_dir": "ingestion_gdelt"},
    {"key": "newsapi", "file": "ingest_newsapi.py", "label": "NewsAPI", "emoji": "📰", "desc": "Articoli NewsAPI", "log_dir": "ingestion_newsapi"},
    {"key": "reddit", "file": "ingest_reddit.py", "label": "Reddit", "emoji": "💬", "desc": "Post e commenti Reddit", "log_dir": "ingestion_reddit"},
    {"key": "youtube", "file": "ingest_youtube.py", "label": "YouTube", "emoji": "▶️", "desc": "Video e commenti YouTube", "log_dir": "ingestion_youtube"},
    {"key": "google_trends", "file": "ingest_pytrends.py", "label": "Google Trends", "emoji": "🔍", "desc": "Trend e topic", "log_dir": "ingestion_pytrends"},
    {"key": "nlp", "file": "nlp_layer.py", "label": "NLP", "emoji": "🧠", "desc": "Sentiment analysis", "log_dir": "nlp_layer"},
]

app = FastAPI(title="Media Digital Twin API PRO")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def mongo_col():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2500)
    client.server_info()
    return client, client[MONGO_DB_NAME]["raw_data"]


def neo4j_driver():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    return driver


def clean_doc(doc: dict[str, Any]) -> dict[str, Any]:
    doc["_id"] = str(doc.get("_id"))
    return doc


def latest_log(log_dir: str) -> str | None:
    path = LOGS_DIR / log_dir
    files = sorted(glob.glob(str(path / "*.log")), reverse=True) if path.exists() else []
    return files[0] if files else None


def latest_frontend_run_log(key: str) -> str | None:
    files = sorted(glob.glob(str(FRONTEND_RUN_LOGS / f"{key}_*.log")), reverse=True)
    return files[0] if files else None


def read_tail(path: str | None, lines: int = 200) -> str:
    if not path:
        return "Nessun log disponibile."
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-lines:])
    except Exception as e:
        return f"Errore lettura log: {e}"


def pipeline_item(key: str) -> dict[str, Any] | None:
    return next((p for p in PIPELINE if p["key"] == key), None)


def spawn_script(item: dict[str, Any]) -> int:
    script = PROJECT_ROOT / item["file"]
    if not script.exists():
        raise HTTPException(404, f"Script non trovato: {script}")

    env = os.environ.copy()
    env["PIPELINE_SINGLE_RUN"] = "1"
    env["PYTHONUNBUFFERED"] = "1"

    log_path = FRONTEND_RUN_LOGS / f"{item['key']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file = open(log_path, "w", encoding="utf-8", errors="replace")

    proc = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=str(PROJECT_ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
    )
    return proc.pid


@app.get("/health")
def health():
    out = {"mongo": False, "neo4j": False, "project_root": str(PROJECT_ROOT), "logs_dir": str(LOGS_DIR)}
    try:
        client, _ = mongo_col()
        client.close()
        out["mongo"] = True
    except Exception:
        pass
    try:
        driver = neo4j_driver()
        driver.close()
        out["neo4j"] = True
    except Exception:
        pass
    return out


@app.get("/overview")
def overview():
    client, col = mongo_col()
    total = col.count_documents({})
    by_src = list(col.aggregate([{"$group": {"_id": "$source", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]))
    pending = col.count_documents({"source": {"$nin": ["yfinance", "google_trends", "gdelt"]}, "payload.sentiment_label": {"$exists": False}})
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    by_day = list(col.aggregate([
        {"$match": {"ingested_at": {"$gte": since}}},
        {"$addFields": {
    "event_date": {
        "$ifNull": [
            "$payload.published_at",
            {
                "$ifNull": [
                    "$payload.video_published_at",
                    {
                        "$ifNull": [
                            "$payload.date",
                            "$ingested_at"
                        ]
                    }
                ]
            }
        ]
    }
}},
{"$addFields": {"day": {"$substr": ["$event_date", 0, 10]}}},
        {"$group": {"_id": {"day": "$day", "src": "$source"}, "n": {"$sum": 1}}},
        {"$sort": {"_id.day": 1}},
    ]))
    client.close()

    graph = {"counts": {}, "relationships": 0}
    try:
        driver = neo4j_driver()
        with driver.session() as s:
            graph["counts"] = {r["label"]: r["n"] for r in s.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS n").data()}
            graph["relationships"] = s.run("MATCH ()-[r]->() RETURN count(r) AS n").single()["n"]
        driver.close()
    except Exception:
        pass

    counts = {r["_id"]: r["n"] for r in by_src}
    pipeline = []
    for p in PIPELINE:
        lp = latest_log(p["log_dir"])
        pipeline.append({**p, "docs": counts.get(p["key"], 0), "last_run": datetime.fromtimestamp(os.path.getmtime(lp)).isoformat() if lp else None})

    return {
        "total_documents": total,
        "pending_nlp": pending,
        "by_source": [{"source": r["_id"], "documents": r["n"]} for r in by_src],
        "ingestion_7d": [{"day": r["_id"]["day"], "source": r["_id"]["src"], "documents": r["n"]} for r in by_day],
        "graph": graph,
        "pipeline": pipeline,
    }


@app.get("/mongodb/documents")
def documents(source: str = "all", sentiment: str = "all", ticker: str = "all", limit: int = Query(100, ge=1, le=500)):
    client, col = mongo_col()
    q: dict[str, Any] = {}
    if source != "all":
        q["source"] = source
    if sentiment != "all":
        q["payload.sentiment_label"] = sentiment
    if ticker != "all":
        q["payload.mentions"] = ticker
    docs = list(col.find(q).sort("ingested_at", -1).limit(limit))
    client.close()
    return {"documents": [clean_doc(d) for d in docs]}


@app.get("/graph/stats")
def graph_stats():
    driver = neo4j_driver()
    with driver.session() as s:
        node_counts = s.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count ORDER BY count DESC").data()
        rel_counts = s.run("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC").data()
        comentions = s.run("""
            MATCH (c1:Company)<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
            WHERE c1.ticker < c2.ticker
            RETURN c1.ticker AS source, c2.ticker AS target, count(e) AS weight
            ORDER BY weight DESC LIMIT 80
        """).data()
    driver.close()
    return {"node_counts": node_counts, "relationship_counts": rel_counts, "comentions": comentions}


@app.get("/companies/{ticker}")
def company(ticker: str, days: int = 7):
    ticker = ticker.upper()
    client, col = mongo_col()
    events = list(col.find({"payload.mentions": ticker}).sort("ingested_at", -1).limit(5000))
    ohlcv = list(col.find({"source": "yfinance", "payload.ticker": ticker}, {"payload": 1}).sort("payload.timestamp", -1).limit(96))
    client.close()

    graph = {"info": {}, "co_mentions": [], "comentions": [], "topics": []}
    try:
        driver = neo4j_driver()
        with driver.session() as s:
            info = s.run("""
                MATCH (c:Company {ticker:$t})
                OPTIONAL MATCH (c)-[:BELONGS_TO]->(i:Industry)-[:PART_OF]->(sec:Sector)
                RETURN c.name AS name, c.last_close AS close, c.last_volume AS volume,
                       i.name AS industry, sec.name AS sector
            """, t=ticker).single()
            comentions = s.run("""
                MATCH (c1:Company {ticker:$t})<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
                WHERE c2.ticker <> $t
                RETURN c2.ticker AS ticker, count(e) AS count
                ORDER BY count DESC LIMIT 10
            """, t=ticker).data()
            topics = s.run("MATCH (c:Company {ticker:$t})-[:TRENDING_WITH]->(top:Topic) RETURN top.name AS topic LIMIT 20", t=ticker).data()
        driver.close()
        graph = {
            "info": dict(info) if info else {},
            "co_mentions": comentions,
            "comentions": comentions,
            "topics": [r["topic"] for r in topics],
        }
    except Exception:
        pass

    return {"ticker": ticker, "graph": graph, "events": [clean_doc(e) for e in events], "ohlcv": [x.get("payload", {}) for x in reversed(ohlcv)]}


@app.get("/compare/{ticker}")
def compare(ticker: str, days: int = 7):
    ticker = ticker.upper()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    client, col = mongo_col()
    mongo_total = col.count_documents({"payload.mentions": ticker, "ingested_at": {"$gte": since}})
    by_src = list(col.aggregate([
        {"$match": {"payload.mentions": ticker, "ingested_at": {"$gte": since}}},
        {"$group": {"_id": "$source", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]))
    client.close()

    neo = {"total": 0, "comentions": []}
    try:
        driver = neo4j_driver()
        with driver.session() as s:
            neo["total"] = s.run("MATCH (e:Event)-[:MENZIONATA_IN]->(c:Company {ticker:$t}) RETURN count(e) AS n", t=ticker).single()["n"]
            neo["comentions"] = s.run("""
                MATCH (c1:Company {ticker:$t})<-[:MENZIONATA_IN]-(e:Event)-[:MENZIONATA_IN]->(c2:Company)
                WHERE c2.ticker <> $t
                RETURN c2.ticker AS ticker, count(e) AS count
                ORDER BY count DESC LIMIT 10
            """, t=ticker).data()
        driver.close()
    except Exception:
        pass

    return {"ticker": ticker, "days": days, "mongo": {"total": mongo_total, "by_source": by_src}, "neo4j": neo}


@app.get("/pipeline/status")
def pipeline_status():
    try:
        client, col = mongo_col()
        counts = {r["_id"]: r["n"] for r in col.aggregate([{"$group": {"_id": "$source", "n": {"$sum": 1}}}])}
        client.close()
    except Exception:
        counts = {}

    rows = []
    for p in PIPELINE:
        lp = latest_log(p["log_dir"])
        fp = latest_frontend_run_log(p["key"])
        rows.append({
            **p,
            "documents": counts.get(p["key"], 0),
            "last_run": datetime.fromtimestamp(os.path.getmtime(lp)).isoformat() if lp else None,
            "has_log": bool(lp or fp),
            "frontend_log": fp,
        })
    return {"project_root": str(PROJECT_ROOT), "logs_dir": str(LOGS_DIR), "pipeline": rows}


@app.get("/pipeline/logs/{key}")
def pipeline_logs(key: str, lines: int = Query(200, ge=10, le=1000)):
    item = pipeline_item(key)
    if not item:
        raise HTTPException(404, "Pipeline non trovata")
    path = latest_frontend_run_log(key) or latest_log(item["log_dir"])
    return {"key": key, "path": path, "content": read_tail(path, lines)}


@app.post("/pipeline/run/{key}")
def pipeline_run(key: str):
    item = pipeline_item(key)
    if not item:
        raise HTTPException(404, "Pipeline non trovata")
    pid = spawn_script(item)
    return {"started": True, "pid": pid, "message": f"{item['label']} avviata."}


@app.post("/pipeline/run-all")
def pipeline_run_all():
    pids = []
    for item in PIPELINE:
        pids.append({"key": item["key"], "pid": spawn_script(item)})
    return {"started": True, "pids": pids, "message": "Pipeline completa avviata."}

from pipeline_pro import router as pipeline_pro_router
app.include_router(pipeline_pro_router)

@app.get("/history/{ticker}")
def history(
    ticker: str,
    days: int = Query(90, ge=1, le=365),
    market_mode: str = Query("live", pattern="^(live|mongo)$"),
):
    ticker = ticker.upper()

    now_dt = datetime.now(timezone.utc)
    since_dt = now_dt - timedelta(days=days)
    since_iso = since_dt.isoformat()

    client, col = mongo_col()

    company_names = {
        "NVDA": ["NVDA", "NVIDIA", "Nvidia"],
        "TSLA": ["TSLA", "TESLA", "Tesla"],
        "AAPL": ["AAPL", "APPLE", "Apple"],
        "META": ["META", "Meta", "Facebook"],
        "GOOGL": ["GOOGL", "GOOGLE", "Google", "Alphabet"],
        "MSFT": ["MSFT", "MICROSOFT", "Microsoft"],
        "DIS": ["DIS", "DISNEY", "Disney"],
        "NFLX": ["NFLX", "NETFLIX", "Netflix"],
        "AMZN": ["AMZN", "AMAZON", "Amazon"],
        "JPM": ["JPM", "JPMORGAN", "JPMorgan", "JPMorgan Chase"],
        "PYPL": ["PYPL", "PAYPAL", "PayPal"],
        "COIN": ["COIN", "Coinbase"],
        "XOM": ["XOM", "EXXON", "Exxon", "ExxonMobil"],
        "BA": ["BA", "BOEING", "Boeing"],
        "RACE": ["RACE", "Ferrari"],
    }

    terms = company_names.get(ticker, [ticker])
    regex_terms = "|".join(terms)

    ticker_match = {
        "$or": [
            {"payload.mentions": {"$in": terms}},
            {"payload.ticker": {"$in": terms}},
            {"payload.symbol": {"$in": terms}},
            {"payload.query": {"$in": terms}},
            {"payload.keyword": {"$in": terms}},
            {"payload.title": {"$regex": regex_terms, "$options": "i"}},
            {"payload.video_title": {"$regex": regex_terms, "$options": "i"}},
            {"payload.description": {"$regex": regex_terms, "$options": "i"}},
            {"payload.text": {"$regex": regex_terms, "$options": "i"}},
        ]
    }

    event_date_stage = {
        "$addFields": {
            "event_date": {
                "$ifNull": [
                    "$payload.published_at",
                    {
                        "$ifNull": [
                            "$payload.video_published_at",
                            {
                                "$ifNull": [
                                    "$payload.date",
                                    {
                                        "$ifNull": [
                                            "$payload.timestamp",
                                            "$ingested_at"
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    }

    event_date_filter = {"event_date": {"$gte": since_iso}}

    documents_by_day = list(col.aggregate([
        {"$match": ticker_match},
        event_date_stage,
        {"$match": event_date_filter},
        {"$addFields": {"day": {"$substr": ["$event_date", 0, 10]}}},
        {"$group": {"_id": "$day", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]))

    documents_by_source = list(col.aggregate([
        {"$match": ticker_match},
        event_date_stage,
        {"$match": event_date_filter},
        {"$group": {"_id": "$source", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]))

    sentiment_by_day = list(col.aggregate([
        {"$match": ticker_match},
        event_date_stage,
        {"$match": {
            "event_date": {"$gte": since_iso},
            "payload.sentiment_label": {"$exists": True},
        }},
        {"$addFields": {"day": {"$substr": ["$event_date", 0, 10]}}},
        {"$group": {
            "_id": {
                "day": "$day",
                "sentiment": "$payload.sentiment_label"
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.day": 1}},
    ]))

    recent_events = list(col.aggregate([
        {"$match": ticker_match},
        event_date_stage,
        {"$match": event_date_filter},
        {"$sort": {"event_date": -1}},
        {"$limit": 100},
    ]))

    ohlcv = []

    if market_mode == "mongo":
        raw_ohlcv = list(
            col.find(
                {
                    "source": "yfinance",
                    "payload.ticker": ticker,
                },
                {"payload": 1, "ingested_at": 1}
            )
            .sort("ingested_at", 1)
            .limit(2000)
        )

        for doc in raw_ohlcv:
            p = doc.get("payload", {}) or {}
            ingested_at = str(doc.get("ingested_at") or "")
            ts = str(p.get("timestamp") or p.get("date") or ingested_at)

            keep = True
            try:
                compare_date = ingested_at or ts
                clean_ts = compare_date.replace("Z", "+00:00")
                ts_dt = datetime.fromisoformat(clean_ts)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                keep = ts_dt >= since_dt
            except Exception:
                keep = True

            if keep:
                ohlcv.append({
                    **p,
                    "timestamp": ingested_at or ts,
                    "date": (ingested_at or ts)[:10],
                    "source_mode": "mongodb_pipeline",
                })

    else:
        period_map = {
            7: "7d",
            30: "1mo",
            90: "3mo",
            180: "6mo",
            365: "1y",
        }

        yf_period = period_map.get(days, "3mo")

        try:
            df = yf.Ticker(ticker).history(
                period=yf_period,
                interval="1d",
                auto_adjust=False,
                prepost=False,
            )

            if df is not None and not df.empty:
                for idx, row in df.iterrows():
                    if row[["Open", "High", "Low", "Close", "Volume"]].isna().any():
                        continue

                    ohlcv.append({
                        "ticker": ticker,
                        "timestamp": idx.isoformat(),
                        "date": idx.strftime("%Y-%m-%d"),
                        "open": round(float(row["Open"]), 4),
                        "high": round(float(row["High"]), 4),
                        "low": round(float(row["Low"]), 4),
                        "close": round(float(row["Close"]), 4),
                        "volume": int(row["Volume"]),
                        "source_mode": "yfinance_live_history",
                    })
        except Exception as e:
            print("YFinance history error:", e)

    client.close()

    latest = ohlcv[-1] if ohlcv else {}
    first = ohlcv[0] if ohlcv else {}

    latest_close = latest.get("close")
    first_close = first.get("close")

    change_pct = None
    try:
        if latest_close and first_close:
            change_pct = ((float(latest_close) - float(first_close)) / float(first_close)) * 100
    except Exception:
        change_pct = None

    volumes = []
    for p in ohlcv:
        try:
            if p.get("volume") is not None:
                volumes.append(float(p.get("volume")))
        except Exception:
            pass

    avg_volume = sum(volumes) / len(volumes) if volumes else None

    return {
        "ticker": ticker,
        "days": days,
        "market_mode": market_mode,
        "market_source": "mongodb_pipeline" if market_mode == "mongo" else "yfinance_live",
        "media_source": "mongodb_raw_data",
        "metrics": {
            "latest_close": latest_close,
            "first_close": first_close,
            "change_pct": change_pct,
            "avg_volume": avg_volume,
            "ohlcv_points": len(ohlcv),
            "events": len(recent_events),
        },
        "documents_by_day": [
            {"day": r["_id"], "count": r["count"]}
            for r in documents_by_day
        ],
        "documents_by_source": [
            {"source": r["_id"], "count": r["count"]}
            for r in documents_by_source
        ],
        "sentiment_by_day": [
            {
                "day": r["_id"]["day"],
                "sentiment": r["_id"]["sentiment"],
                "count": r["count"],
            }
            for r in sentiment_by_day
        ],
        "ohlcv": ohlcv,
        "events": [
            clean_doc(e)
            for e in recent_events
        ],
    }

@app.get("/graph/explore")
def graph_explore(ticker: str = "ALL", limit: int = Query(120, ge=20, le=300)):
    driver = neo4j_driver()

    nodes = {}
    links = []

    def add_node(node_id, label, node_type):
        if node_id and node_id not in nodes:
            nodes[node_id] = {
                "id": node_id,
                "label": label or node_id,
                "type": node_type,
            }

    def add_link(source, target, rel_type, weight=1):
        if source and target:
            links.append({
                "source": source,
                "target": target,
                "type": rel_type,
                "weight": weight,
            })

    with driver.session() as s:
        if ticker != "ALL":
            rows = s.run("""
                MATCH (c:Company {ticker:$ticker})
                OPTIONAL MATCH (c)<-[r1:MENZIONATA_IN]-(e:Event)
                OPTIONAL MATCH (c)-[r2:TRENDING_WITH]->(t:Topic)
                OPTIONAL MATCH (c)-[r3:BELONGS_TO]->(i:Industry)
                OPTIONAL MATCH (i)-[r4:PART_OF]->(sec:Sector)
                RETURN c, e, t, i, sec
                LIMIT $limit
            """, ticker=ticker, limit=limit).data()
        else:
            rows = s.run("""
                MATCH (c:Company)
                OPTIONAL MATCH (c)<-[r1:MENZIONATA_IN]-(e:Event)
                OPTIONAL MATCH (c)-[r2:TRENDING_WITH]->(t:Topic)
                OPTIONAL MATCH (c)-[r3:BELONGS_TO]->(i:Industry)
                OPTIONAL MATCH (i)-[r4:PART_OF]->(sec:Sector)
                RETURN c, e, t, i, sec
                LIMIT $limit
            """, limit=limit).data()

    driver.close()

    for row in rows:
        c = row.get("c")
        e = row.get("e")
        t = row.get("t")
        i = row.get("i")
        sec = row.get("sec")

        if c:
            company_id = c.get("ticker")
            add_node(company_id, c.get("name") or company_id, "Company")

            if e:
                event_id = e.get("mongo_id") or e.get("title")
                add_node(event_id, e.get("title") or "Event", "Event")
                add_link(event_id, company_id, "MENZIONATA_IN")

            if t:
                topic_id = f"topic:{t.get('name')}"
                add_node(topic_id, t.get("name"), "Topic")
                add_link(company_id, topic_id, "TRENDING_WITH")

            if i:
                industry_id = f"industry:{i.get('name')}"
                add_node(industry_id, i.get("name"), "Industry")
                add_link(company_id, industry_id, "BELONGS_TO")

                if sec:
                    sector_id = f"sector:{sec.get('name')}"
                    add_node(sector_id, sec.get("name"), "Sector")
                    add_link(industry_id, sector_id, "PART_OF")

    return {
        "nodes": list(nodes.values()),
        "links": links,
    }