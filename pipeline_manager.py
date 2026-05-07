"""
pipeline_manager.py
===================
Orchestratore interattivo della pipeline Media Digital Twin.

Menu principale:
  [1] Avvia pipeline completa
  [2] Avvia solo ingestion (senza NLP)
  [3] Avvia solo NLP layer
  [4] Avvia singolo script
  [5] Stato pipeline
  [0] Esci

Funzionalità:
  - Pre-selezione script da includere nel ciclo
  - Conferma prima di ogni script [Y/n/skip]
  - In caso di errore: [R]iprova / [S]kip / [F]erma
  - Modalità singola o loop con intervallo personalizzabile
  - Log su file per ogni sessione

Ordine pipeline:
  yfinance → pytrends → gdelt → newsapi → reddit → youtube → nlp

Autori : Massimo Mazzini, Rafaele Rossi
Corso  : Big Data Engineer & Solution Architect — 2° anno
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ---------------------------------------------------------------------------
# Configurazione logging
# ---------------------------------------------------------------------------
LOG_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "pipeline_manager")
LOG_FILE = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
os.makedirs(LOG_DIR, exist_ok=True)

_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(message)s",
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

log = logging.getLogger("pipeline_manager")

# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente
# ---------------------------------------------------------------------------
load_dotenv()

MONGO_URI     = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

# ---------------------------------------------------------------------------
# Configurazione script
# ---------------------------------------------------------------------------
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON      = sys.executable

# Ordine pipeline con descrizione
PIPELINE_ORDER = [
    {"key": "yfinance",  "file": "ingest_yfinance.py",  "desc": "Prezzi finanziari (yfinance)"},
    {"key": "pytrends",  "file": "ingest_pytrends.py",  "desc": "Google Trends (pytrends)"},
    {"key": "gdelt",     "file": "ingest_gdelt.py",     "desc": "Eventi globali (GDELT)"},
    {"key": "newsapi",   "file": "ingest_newsapi.py",   "desc": "Articoli news (NewsAPI)"},
    {"key": "reddit",    "file": "ingest_reddit.py",    "desc": "Post e commenti (Reddit)"},
    {"key": "youtube",   "file": "ingest_youtube.py",   "desc": "Video e commenti (YouTube)"},
    {"key": "nlp",       "file": "nlp_layer.py",        "desc": "Analisi sentiment (NLP)"},
]

# Timestamp ultima esecuzione
_last_run: dict[str, str] = {s["key"]: "Mai eseguito" for s in PIPELINE_ORDER}


# ---------------------------------------------------------------------------
# Utilità UI
# ---------------------------------------------------------------------------

def clear():
    os.system("cls" if os.name == "nt" else "clear")


def sep(char="─", width=55):
    print(char * width)


def header():
    clear()
    sep("═")
    print("   🔷 MEDIA DIGITAL TWIN — PIPELINE MANAGER")
    print("   Massimo Mazzini & Rafaele Rossi")
    sep("═")


def ask(prompt: str, options: list[str], default: str = "") -> str:
    """Chiede input all'utente e restituisce la risposta in minuscolo."""
    while True:
        reply = input(f"\n  {prompt} ").strip().lower()
        if not reply and default:
            return default
        if reply in options:
            return reply
        print(f"  ⚠️  Inserisci: {' / '.join(options)}")


# ---------------------------------------------------------------------------
# Esecuzione script singolo
# ---------------------------------------------------------------------------

def run_script(script_info: dict) -> bool:
    """
    Esegue uno script Python come sottoprocesso.
    Passa PIPELINE_SINGLE_RUN=1 per disabilitare il loop interno degli script.
    Restituisce True se completato con successo.
    """
    key         = script_info["key"]
    script_path = os.path.join(SCRIPTS_DIR, script_info["file"])

    if not os.path.exists(script_path):
        log.error("File non trovato: %s", script_path)
        return False

    env                        = os.environ.copy()
    env["PIPELINE_SINGLE_RUN"] = "1"

    start = time.time()

    try:
        result = subprocess.run(
            [PYTHON, script_path],
            env     = env,
            timeout = 3600,
        )
        elapsed = round(time.time() - start, 1)

        if result.returncode == 0:
            _last_run[key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log.info("✓ %s completato in %ss", key.upper(), elapsed)
            return True
        else:
            log.error("✗ %s exit code: %d", key.upper(), result.returncode)
            return False

    except subprocess.TimeoutExpired:
        log.error("✗ %s timeout (3600s)", key.upper())
        return False
    except Exception as e:
        log.error("✗ %s errore → %s", key.upper(), e)
        return False


# ---------------------------------------------------------------------------
# Logica pipeline con conferma e gestione errori
# ---------------------------------------------------------------------------

def run_pipeline_with_confirm(scripts: list[dict]) -> None:
    """
    Esegue una pipeline con:
    - Conferma prima di ogni script [Y/n/skip]
    - Gestione errori [R]iprova / [S]kip / [F]erma
    """
    results = {}
    total   = len(scripts)

    for i, script in enumerate(scripts, 1):
        key  = script["key"]
        desc = script["desc"]

        sep()
        print(f"\n  [{i}/{total}] {desc}")

        # Conferma prima di avviare
        confirm = ask(f"Avvio {key.upper()}? [Y/n/skip]:", ["y", "n", "skip", "s"], default="y")

        if confirm in ("n", "skip", "s"):
            log.info("⏭  %s saltato dall'utente", key.upper())
            results[key] = "saltato"
            continue

        # Esecuzione con retry
        while True:
            print(f"\n  ▶ Avvio {key.upper()}...")
            success = run_script(script)

            if success:
                results[key] = "ok"
                break
            else:
                # Gestione errore
                print(f"\n  ⚠️  {key.upper()} ha fallito.")
                action = ask(
                    "[R]iprova / [S]kip / [F]erma pipeline:",
                    ["r", "riprova", "s", "skip", "f", "ferma"],
                    default="s"
                )

                if action in ("r", "riprova"):
                    log.info("↩  Riprovo %s...", key.upper())
                    continue
                elif action in ("f", "ferma"):
                    log.warning("🛑 Pipeline fermata dall'utente dopo errore su %s", key.upper())
                    results[key] = "errore"
                    print_pipeline_summary(results)
                    return
                else:
                    log.info("⏭  %s saltato dopo errore", key.upper())
                    results[key] = "errore"
                    break

        if i < total:
            time.sleep(2)

    print_pipeline_summary(results)


def print_pipeline_summary(results: dict[str, str]) -> None:
    """Stampa il riepilogo finale della pipeline."""
    sep()
    print("\n  📊 RIEPILOGO PIPELINE\n")

    icons = {"ok": "✓", "errore": "✗", "saltato": "⏭"}
    for key, status in results.items():
        icon = icons.get(status, "?")
        print(f"    {icon} {key.upper():<12} → {status.upper()}")

    ok      = sum(1 for v in results.values() if v == "ok")
    skipped = sum(1 for v in results.values() if v == "saltato")
    errors  = sum(1 for v in results.values() if v == "errore")

    print(f"\n    Completati: {ok} | Saltati: {skipped} | Errori: {errors}")
    sep()
    input("\n  Premi INVIO per tornare al menu...")


# ---------------------------------------------------------------------------
# Pre-selezione script
# ---------------------------------------------------------------------------

def select_scripts(available: list[dict]) -> list[dict]:
    """
    Chiede all'utente quali script includere nella pipeline.
    Restituisce la lista filtrata in ordine.
    """
    sep()
    print("\n  📋 PRE-SELEZIONE SCRIPT\n")
    print("  Scegli quali script includere [Y/n]:\n")

    selected = []
    for script in available:
        key  = script["key"]
        desc = script["desc"]
        ans  = ask(f"  Includi {key.upper()} ({desc})? [Y/n]:", ["y", "n"], default="y")
        if ans == "y":
            selected.append(script)

    if not selected:
        print("\n  ⚠️  Nessuno script selezionato.")
        input("  Premi INVIO per tornare al menu...")
        return []

    print(f"\n  ✅ Script selezionati: {len(selected)}/{len(available)}")
    for s in selected:
        print(f"    → {s['key'].upper()}")

    return selected


# ---------------------------------------------------------------------------
# Stato pipeline
# ---------------------------------------------------------------------------

def show_status() -> None:
    """Mostra lo stato completo della pipeline."""
    header()
    print("\n  📊 STATO PIPELINE\n")

    try:
        client     = MongoClient(MONGO_URI)
        db         = client[MONGO_DB_NAME]
        collection = db["raw_data"]

        total = collection.count_documents({})
        print(f"  Documenti totali in MongoDB: {total}\n")

        # Documenti per sorgente
        sep("─", 45)
        print(f"  {'Sorgente':<15} {'Totale':>8} {'Con sentiment':>14}")
        sep("─", 45)

        sources = ["yfinance", "newsapi", "reddit", "gdelt", "google_trends", "youtube"]
        for source in sources:
            count = collection.count_documents({"source": source})
            if count == 0:
                continue

            if source in ("yfinance", "google_trends"):
                sentiment_count = "N/A"
            elif source == "gdelt":
                sentiment_count = str(collection.count_documents({
                    "source": "gdelt",
                    "payload.sentiment_label": {"$exists": True},
                }))
            else:
                sentiment_count = str(collection.count_documents({
                    "source":                  source,
                    "payload.sentiment_label": {"$exists": True},
                }))

            print(f"  {source:<15} {count:>8} {sentiment_count:>14}")

        sep("─", 45)

        # Sentiment globale
        processed   = collection.count_documents({
            "source":                  {"$nin": ["yfinance", "google_trends"]},
            "payload.sentiment_label": {"$exists": True},
        })
        unprocessed = collection.count_documents({
            "source":                  {"$nin": ["yfinance", "google_trends"]},
            "payload.sentiment_label": {"$exists": False},
        })

        print(f"\n  NLP processati:     {processed}")
        print(f"  NLP da processare:  {unprocessed}")

        client.close()

    except PyMongoError as e:
        print(f"  ⚠️  MongoDB non raggiungibile: {e}")
        print("  Verifica che Docker sia attivo con: docker compose up -d")

    # Ultima esecuzione per script
    sep("─", 45)
    print(f"\n  {'Script':<12} {'Ultima esecuzione'}")
    sep("─", 45)
    for key, ts in _last_run.items():
        print(f"  {key:<12} {ts}")

    sep()
    input("\n  Premi INVIO per tornare al menu...")


# ---------------------------------------------------------------------------
# Menu singolo script
# ---------------------------------------------------------------------------

def menu_single_script() -> None:
    """Menu per lanciare un singolo script."""
    header()
    print("\n  Scegli lo script:\n")

    for i, script in enumerate(PIPELINE_ORDER, 1):
        print(f"  [{i}] {script['key'].upper():<12} — {script['desc']}")

    print("  [0] Torna al menu\n")

    choice = input("  Scelta: ").strip()
    if choice == "0":
        return

    try:
        idx = int(choice) - 1
        if 0 <= idx < len(PIPELINE_ORDER):
            script = PIPELINE_ORDER[idx]
            print(f"\n  ▶ Avvio {script['key'].upper()}...")
            success = run_script(script)
            if success:
                print(f"\n  ✓ {script['key'].upper()} completato.")
            else:
                print(f"\n  ✗ {script['key'].upper()} fallito.")
            input("\n  Premi INVIO per continuare...")
        else:
            print("  ⚠️  Scelta non valida.")
    except ValueError:
        print("  ⚠️  Inserisci un numero.")


# ---------------------------------------------------------------------------
# Modalità loop
# ---------------------------------------------------------------------------

def ask_loop_mode() -> tuple[bool, int]:
    """
    Chiede all'utente se vuole eseguire in modalità singola o loop.
    Restituisce (is_loop, interval_minutes).
    """
    sep()
    print("\n  Modalità di esecuzione:\n")
    print("  [1] Esecuzione singola")
    print("  [2] Loop continuo\n")

    mode = ask("Scegli [1/2]:", ["1", "2"], default="1")

    if mode == "1":
        return False, 0

    interval_str = input("\n  Intervallo tra cicli (minuti, default 60): ").strip()
    try:
        interval = int(interval_str) if interval_str else 60
        interval = max(5, interval)  # minimo 5 minuti
    except ValueError:
        interval = 60

    print(f"\n  ✅ Loop attivo ogni {interval} minuti. CTRL+C per fermare.")
    return True, interval


# ---------------------------------------------------------------------------
# Menu principale
# ---------------------------------------------------------------------------

def main():
    log.info("Avvio Pipeline Manager — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    ingestion_only = [s for s in PIPELINE_ORDER if s["key"] != "nlp"]
    nlp_only       = [s for s in PIPELINE_ORDER if s["key"] == "nlp"]

    while True:
        header()
        print("""
  [1] Avvia pipeline completa
  [2] Avvia solo ingestion (senza NLP)
  [3] Avvia solo NLP layer
  [4] Avvia singolo script
  [5] Stato pipeline
  [0] Esci
""")
        sep()
        choice = input("  Scelta: ").strip()

        # ── Pipeline completa ──────────────────────────────────────────────
        if choice == "1":
            header()
            selected = select_scripts(PIPELINE_ORDER)
            if not selected:
                continue

            is_loop, interval = ask_loop_mode()
            cycle = 1

            while True:
                if is_loop:
                    log.info("═══ CICLO %d ═══", cycle)
                run_pipeline_with_confirm(selected)

                if not is_loop:
                    break

                log.info("Prossimo ciclo tra %d minuti. CTRL+C per fermare.", interval)
                print(f"\n  ⏳ Prossimo ciclo tra {interval} minuti...")
                time.sleep(interval * 60)
                cycle += 1

        # ── Solo ingestion ─────────────────────────────────────────────────
        elif choice == "2":
            header()
            selected = select_scripts(ingestion_only)
            if not selected:
                continue

            is_loop, interval = ask_loop_mode()
            cycle = 1

            while True:
                if is_loop:
                    log.info("═══ CICLO %d ═══", cycle)
                run_pipeline_with_confirm(selected)

                if not is_loop:
                    break

                log.info("Prossimo ciclo tra %d minuti.", interval)
                time.sleep(interval * 60)
                cycle += 1

        # ── Solo NLP ───────────────────────────────────────────────────────
        elif choice == "3":
            header()
            is_loop, interval = ask_loop_mode()
            cycle = 1

            while True:
                if is_loop:
                    log.info("═══ CICLO NLP %d ═══", cycle)
                run_pipeline_with_confirm(nlp_only)

                if not is_loop:
                    break

                time.sleep(interval * 60)
                cycle += 1

        # ── Singolo script ─────────────────────────────────────────────────
        elif choice == "4":
            menu_single_script()

        # ── Stato pipeline ─────────────────────────────────────────────────
        elif choice == "5":
            show_status()

        # ── Esci ───────────────────────────────────────────────────────────
        elif choice == "0":
            log.info("Pipeline Manager terminato.")
            print("\n  👋 Arrivederci!\n")
            sys.exit(0)

        else:
            print("  ⚠️  Scelta non valida.")
            time.sleep(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  🛑 Interruzione manuale (CTRL+C). Uscita.")
        log.info("Pipeline Manager interrotto da CTRL+C")
        sys.exit(0)