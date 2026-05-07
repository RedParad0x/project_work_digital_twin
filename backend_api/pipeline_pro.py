from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pymongo import MongoClient

router = APIRouter(prefix="/pipeline-pro", tags=["Pipeline PRO"])

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:mediatwin1@localhost:27018/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "media_twin")
PROJECT_ROOT = Path(os.getenv("MEDIA_TWIN_ROOT", Path(__file__).resolve().parent.parent)).resolve()
LOGS_DIR = PROJECT_ROOT.parent / "logs"
if not LOGS_DIR.exists():
    LOGS_DIR = PROJECT_ROOT / "logs"
RUN_DIR = LOGS_DIR / "frontend_runs"
RUN_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = RUN_DIR / "pipeline_state.json"
STATE_LOCK = threading.Lock()

PIPELINE = [
    {"key": "yfinance", "file": "ingest_yfinance.py", "label": "YFinance", "emoji": "📈", "desc": "Prezzi OHLCV Yahoo Finance", "log_dir": "ingestion_yfinance"},
    {"key": "google_trends", "file": "ingest_pytrends.py", "label": "Google Trends", "emoji": "🔍", "desc": "Trend e query correlate", "log_dir": "ingestion_pytrends"},
    {"key": "gdelt", "file": "ingest_gdelt.py", "label": "GDELT", "emoji": "🌍", "desc": "Eventi globali GDELT", "log_dir": "ingestion_gdelt"},
    {"key": "newsapi", "file": "ingest_newsapi.py", "label": "NewsAPI", "emoji": "📰", "desc": "Articoli giornalistici", "log_dir": "ingestion_newsapi"},
    {"key": "reddit", "file": "ingest_reddit.py", "label": "Reddit", "emoji": "💬", "desc": "Post e commenti Reddit", "log_dir": "ingestion_reddit"},
    {"key": "youtube", "file": "ingest_youtube.py", "label": "YouTube", "emoji": "▶️", "desc": "Video e commenti YouTube", "log_dir": "ingestion_youtube"},
    {"key": "nlp", "file": "nlp_layer.py", "label": "NLP", "emoji": "🧠", "desc": "Analisi sentiment", "log_dir": "nlp_layer"},
]


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def initial_state() -> dict[str, Any]:
    return {
        "full": {"status": "idle", "current_step": None, "started_at": None, "finished_at": None, "message": ""},
        "steps": {
            p["key"]: {
                "status": "idle", "pid": None, "started_at": None, "finished_at": None,
                "duration_seconds": None, "exit_code": None, "log_path": None, "message": "",
            }
            for p in PIPELINE
        },
    }


def save_state_unlocked(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def load_state() -> dict[str, Any]:
    with STATE_LOCK:
        if not STATE_FILE.exists():
            s = initial_state()
            save_state_unlocked(s)
            return s
        try:
            s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            s = initial_state()
        base = initial_state()
        s.setdefault("full", base["full"])
        s.setdefault("steps", {})
        for key, val in base["steps"].items():
            s["steps"].setdefault(key, val)
        return s


def save_state(state: dict[str, Any]) -> None:
    with STATE_LOCK:
        save_state_unlocked(state)


def update_step(key: str, **patch: Any) -> None:
    s = load_state()
    s["steps"][key].update(patch)
    save_state(s)


def update_full(**patch: Any) -> None:
    s = load_state()
    s["full"].update(patch)
    save_state(s)


def item_by_key(key: str) -> dict[str, Any] | None:
    return next((p for p in PIPELINE if p["key"] == key), None)


def latest_log(log_dir: str) -> str | None:
    path = LOGS_DIR / log_dir
    files = sorted(glob.glob(str(path / "*.log")), reverse=True) if path.exists() else []
    return files[0] if files else None


def latest_run_log(key: str) -> str | None:
    files = sorted(glob.glob(str(RUN_DIR / f"{key}_*.log")), reverse=True)
    return files[0] if files else None


def read_tail(path: str | None, lines: int = 280) -> str:
    if not path:
        return "Nessun log disponibile."
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-lines:])
    except Exception as e:
        return f"Errore lettura log: {e}"


def source_counts() -> dict[str, int]:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2500)
        col = client[MONGO_DB_NAME]["raw_data"]
        counts = {r["_id"]: r["n"] for r in col.aggregate([{"$group": {"_id": "$source", "n": {"$sum": 1}}}])}
        client.close()
        return counts
    except Exception:
        return {}


def run_script_sync(item: dict[str, Any]) -> dict[str, Any]:
    key = item["key"]
    script = PROJECT_ROOT / item["file"]
    if not script.exists():
        update_step(key, status="failed", finished_at=now(), exit_code=-404, message=f"Script non trovato: {script}")
        return {"key": key, "exit_code": -404}

    log_path = RUN_DIR / f"{key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    env = os.environ.copy()
    env["PIPELINE_SINGLE_RUN"] = "1"
    env["PYTHONUNBUFFERED"] = "1"

    started = time.time()
    update_step(key, status="running", pid=None, started_at=now(), finished_at=None, duration_seconds=None, exit_code=None, log_path=str(log_path), message=f"Avvio {item['label']}...")

    with open(log_path, "w", encoding="utf-8", errors="replace") as log_file:
        proc = subprocess.Popen([sys.executable, str(script)], cwd=str(PROJECT_ROOT), stdout=log_file, stderr=subprocess.STDOUT, env=env, text=True)
        update_step(key, pid=proc.pid, message=f"{item['label']} in esecuzione. PID {proc.pid}")
        exit_code = proc.wait()

    duration = round(time.time() - started, 1)
    update_step(key, status="success" if exit_code == 0 else "failed", finished_at=now(), duration_seconds=duration, exit_code=exit_code, message=f"{item['label']} terminata con exit code {exit_code} in {duration}s")
    return {"key": key, "exit_code": exit_code, "duration_seconds": duration}


def run_single_worker(key: str) -> None:
    item = item_by_key(key)
    if item:
        run_script_sync(item)


def run_full_worker() -> None:
    update_full(status="running", started_at=now(), finished_at=None, current_step=None, message="Pipeline completa avviata in sequenza")
    failed = False
    for item in PIPELINE:
        update_full(current_step=item["key"], message=f"Esecuzione: {item['label']}")
        result = run_script_sync(item)
        if result.get("exit_code") != 0:
            failed = True
    update_full(status="failed" if failed else "success", finished_at=now(), current_step=None, message="Pipeline completata con errori" if failed else "Pipeline completata con successo")


@router.get("/status")
def status():
    counts = source_counts()
    s = load_state()
    rows = []
    for p in PIPELINE:
        lp = latest_log(p["log_dir"])
        rows.append({
            **p,
            "documents": counts.get(p["key"], 0),
            "last_run": datetime.fromtimestamp(os.path.getmtime(lp)).isoformat() if lp else None,
            "run_state": s["steps"].get(p["key"], {}),
        })
    return {"project_root": str(PROJECT_ROOT), "logs_dir": str(LOGS_DIR), "state_file": str(STATE_FILE), "full": s["full"], "pipeline": rows}


@router.get("/logs/{key}")
def logs(key: str, lines: int = Query(280, ge=10, le=1000)):
    item = item_by_key(key)
    if not item:
        raise HTTPException(404, "Pipeline non trovata")
    state = load_state()["steps"].get(key, {})
    path = state.get("log_path") or latest_run_log(key) or latest_log(item["log_dir"])
    return {"key": key, "path": path, "state": state, "content": read_tail(path, lines)}


@router.post("/run/{key}")
def run_one(key: str):
    item = item_by_key(key)
    if not item:
        raise HTTPException(404, "Pipeline non trovata")
    s = load_state()
    if s["full"].get("status") == "running":
        return {"started": False, "message": "Pipeline completa già in esecuzione"}
    if s["steps"].get(key, {}).get("status") == "running":
        return {"started": False, "message": f"{item['label']} è già in esecuzione"}
    threading.Thread(target=run_single_worker, args=(key,), daemon=True).start()
    return {"started": True, "message": f"{item['label']} avviata"}


@router.post("/run-full")
def run_full():
    s = load_state()
    if s["full"].get("status") == "running":
        return {"started": False, "message": "Pipeline completa già in esecuzione"}
    if any(step.get("status") == "running" for step in s["steps"].values()):
        return {"started": False, "message": "Uno script singolo è già in esecuzione"}
    threading.Thread(target=run_full_worker, daemon=True).start()
    return {"started": True, "message": "Pipeline completa avviata in sequenza"}


@router.post("/reset")
def reset():
    save_state(initial_state())
    return {"reset": True, "message": "Stato pipeline resettato"}
