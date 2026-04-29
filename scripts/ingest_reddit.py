"""
ingest_reddit.py
================
Script di ingestion per post e commenti da Reddit tramite scraping
su old.reddit.com con BeautifulSoup (nessuna API key necessaria).

Logica:
  1. Per ogni subreddit scarica i post più recenti
  2. Filtro post: solo link esterni OPPURE score > soglia
  3. Per ogni post valido scarica i 5 commenti con più upvote
     (score > 5, lunghezza > 50 caratteri)
  4. Per ogni commento top scarica la risposta con più upvote
  5. Rileva menzioni aziende nel testo
  6. Dual-write su MongoDB + Neo4j
  7. Crea archi [:MENZIONATA_IN] verso i nodi Company rilevati
  8. Schedulato ogni 2 ore

Subreddit monitorati: 26
Lingua: IT (subreddit italiani) + EN (subreddit internazionali)
Modello NLP: FEEL-IT per IT, FinBERT per EN (layer NLP separato)

Autori : Massimo Mazzini, Rafaele Rossi
Corso  : Big Data Engineer & Solution Architect — 2° anno
"""

from __future__ import annotations

import logging
import os
import random
import re
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
import schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# Aggiunge la cartella scripts al path per importare ingestion_layer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingestion_layer import IngestionLayer

# ---------------------------------------------------------------------------
# Caricamento lista parolacce
# ---------------------------------------------------------------------------
_PROFANITY_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "profanity_list.txt"
)

def _load_profanity_list() -> list[str]:
    """
    Carica la lista di parolacce dal file profanity_list.txt.
    Ignora righe vuote e commenti (che iniziano con #).
    """
    if not os.path.exists(_PROFANITY_FILE):
        log.warning("File profanity_list.txt non trovato in data/ — filtro disabilitato")
        return []
    with open(_PROFANITY_FILE, encoding="utf-8") as f:
        words = [
            line.strip().lower()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]
    log.info("Profanity filter caricato → %d parole", len(words))
    return words

# PROFANITY_LIST = _load_profanity_list()  # Spostato dopo configurazione logging


def censor_text(text: str) -> str:
    """
    Sostituisce le parole offensive nel testo con ***.
    Il confronto è case-insensitive e rispetta i confini di parola
    per evitare falsi positivi (es. 'ass' dentro 'class').
    """
    if not PROFANITY_LIST or not text:
        return text

    import re
    censored = text
    for word in PROFANITY_LIST:
        # Usa word boundary \b per parole singole
        # Per frasi composte (es. "figlio di puttana") cerca senza boundary
        if " " in word:
            pattern = re.compile(re.escape(word), re.IGNORECASE)
        else:
            pattern = re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE)
        censored = pattern.sub("***", censored)
    return censored


# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente
# ---------------------------------------------------------------------------
load_dotenv()

NEO4J_URI         = os.getenv("NEO4J_URI")
NEO4J_USER        = os.getenv("NEO4J_USER")
NEO4J_PASSWORD    = os.getenv("NEO4J_PASSWORD")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "python:MediaDigitalTwin:v1.0")

# ---------------------------------------------------------------------------
# Configurazione logging (terminale + file)
# ---------------------------------------------------------------------------
start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_DIR    = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "ingestion_reddit")
LOG_FILE   = os.path.join(LOG_DIR, f"reddit_{start_time}.log")
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

log = logging.getLogger("ingest_reddit")
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

# Caricamento lista parolacce dopo configurazione logging
PROFANITY_LIST = _load_profanity_list()

# ---------------------------------------------------------------------------
# Funzioni di supporto per scraping
# ---------------------------------------------------------------------------

def get_random_user_agent() -> str:
    """Restituisce un User-Agent casuale per evitare blocchi."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    return random.choice(user_agents)


def get_browser_headers() -> dict:
    """Restituisce headers che simulano un browser reale."""
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

# ---------------------------------------------------------------------------
# Configurazione scraping
# ---------------------------------------------------------------------------
BASE_URL            = "https://old.reddit.com/r"
MAX_POSTS_PER_SUB   = 25   # post da analizzare per subreddit (prima del filtro)
MAX_TOP_COMMENTS    = 5    # commenti top da salvare per post
MIN_COMMENT_SCORE   = 5    # score minimo commento
MIN_COMMENT_LENGTH  = 50   # lunghezza minima commento in caratteri
SLEEP_BETWEEN_PAGES = 8    # secondi tra richieste pagine (aumentato)
SLEEP_BETWEEN_SUBS  = 12   # secondi tra subreddit (aumentato)
MAX_RETRIES         = 3    # tentativi massimi per richiesta
RETRY_BACKOFF       = 30   # secondi di attesa tra retry per 403

HEADERS = get_browser_headers()  # Headers dinamici per evitare blocchi

# ---------------------------------------------------------------------------
# Subreddit monitorati con soglia score
# ---------------------------------------------------------------------------
SUBREDDITS = {
    "italian": {
        "subs":      ["italy", "italiapersonalfinance", "italiacareeradvice"],
        "min_score": 20,
    },
    "finance": {
        "subs":      ["wallstreetbets", "stocks", "investing", "personalfinance", "StockMarket"],
        "min_score": 100,
    },
    "tech": {
        "subs":      ["technology", "apple", "nvidia", "hardware", "programming", "artificialIntelligence"],
        "min_score": 100,
    },
    "entertainment": {
        "subs":      ["soccer", "movies", "television", "sports", "games", "videogames"],
        "min_score": 100,
    },
    "general": {
        "subs":      ["worldnews", "business", "economics", "economy", "finance", "news"],
        "min_score": 100,
    },
}

# Subreddit italiani per tagging lingua
ITALIAN_SUBREDDITS = {"italy", "italia", "italiapersonalfinance", "italiacareeradvice"}

# Subreddit business-oriented per rilevamento menzioni aziende
BUSINESS_SUBREDDITS = {
    "finance":      {"wallstreetbets", "stocks", "investing", "personalfinance", "StockMarket"},
    "tech":        {"technology", "apple", "nvidia", "hardware", "programming", "artificialIntelligence"},
    "general":     {"worldnews", "business", "economics", "economy", "finance", "news"},
    "italian":      {"italy", "italia", "italiapersonalfinance"},
}

MAX_POST_AGE_DAYS = 7

ENTERTAINMENT_KEYWORDS = {
    "serie", "film", "movie", "movies", "episodio", "episodi", "stagione", "season", "trailer",
    "cast", "recensione", "spoiler", "streaming", "show", "musical", "concert", "tour", "gioco", "games",
    "partita", "match", "sport", "campionato", "serie a", "serie b", "premiere", "festival", "attore", "attrice",
    "telefilm", "collana", "episodio", "stagioni", "unboxing", "gaming",
}

COMPANY_CONFIRM_KEYWORDS = {
    "stock", "stocks", "shares", "share", "stock price", "market cap", "earnings", "earnings call",
    "profit", "loss", "revenue", "quarter", "quarterly", "fiscal", "dividend", "dividends", "eps", "guidance",
    "analyst", "analysts", "valuation", "trading", "ipo", "merger", "acquisition", "buyout",
    "forecast", "financials", "results", "report", "earnings per share", "revenue guidance",
    "borsa", "azioni", "utile", "perdita", "ricavi", "dividendo", "trimestre", "trimestrale", "fatturato",
    "analisti", "quotazione", "margine", "risultati", "report",
}

# Keyword per filtrare post business-oriented (non entertainment)
BUSINESS_KEYWORDS = {
    # Finanza e business
    "finance", "financial", "stock", "stocks", "market", "markets", "trading", "trade", "invest", "investment", "investor", "investors",
    "earnings", "revenue", "profit", "loss", "quarter", "quarterly", "annual", "yearly", "fiscal", "dividend", "dividends",
    "ipo", "merger", "acquisition", "buyout", "bankruptcy", "layoff", "layoffs", "hiring", "hire", "job", "jobs", "employment",
    "ceo", "cfo", "cto", "executive", "executives", "board", "shareholder", "shareholders", "stakeholder", "stakeholders",
    # Tecnologia e innovazione
    "tech", "technology", "software", "hardware", "ai", "artificial intelligence", "machine learning", "ml", "data", "cloud",
    "cybersecurity", "blockchain", "crypto", "cryptocurrency", "startup", "venture", "funding", "venture capital", "vc",
    "patent", "patents", "innovation", "research", "development", "r&d",
    # Economia generale
    "economy", "economic", "economics", "recession", "inflation", "deflation", "growth", "gdp", "unemployment",
    "policy", "regulation", "regulatory", "compliance", "tax", "taxes", "tariff", "tariffs",
    # Aziende e corporate
    "company", "companies", "corporate", "corporation", "business", "enterprise", "firm", "venture",
    "partnership", "alliance", "deal", "contract", "agreement", "negotiation",
    # Mercati e industria
    "industry", "sector", "market share", "competition", "competitor", "competitors", "monopoly", "oligopoly",
    "supply chain", "logistics", "manufacturing", "production", "operations", "strategy", "strategic",
    # Italiano
    "finanza", "finanziario", "borsa", "azioni", "mercato", "investimento", "investitori", "guadagno", "perdita",
    "trimestre", "annuale", "dividendo", "ipo", "fusione", "acquisizione", "fallimento", "licenziamento", "assunzione",
    "ceo", "amministratore", "azionista", "stakeholder", "tecnologia", "software", "hardware", "intelligenza artificiale",
    "machine learning", "dati", "cloud", "cybersecurity", "blockchain", "cripto", "startup", "brevetto", "innovazione",
    "ricerca", "sviluppo", "economia", "economico", "recessione", "inflazione", "crescita", "disoccupazione",
    "politica", "regolamentazione", "tasse", "tariffe", "azienda", "aziende", "corporate", "business", "impresa",
    "partnership", "alleanza", "accordo", "negoziazione", "industria", "settore", "concorrenza", "competitore",
    "catena di fornitura", "logistica", "produzione", "operazioni", "strategia"
}

# ---------------------------------------------------------------------------
# Aziende monitorate per rilevamento menzioni
# ---------------------------------------------------------------------------
COMPANIES = [
    {"ticker": "NVDA",  "name": "NVIDIA"},
    {"ticker": "TSLA",  "name": "Tesla"},
    {"ticker": "AAPL",  "name": "Apple"},
    {"ticker": "META",  "name": "Meta"},
    {"ticker": "GOOGL", "name": "Google"},
    {"ticker": "MSFT",  "name": "Microsoft"},
    {"ticker": "DIS",   "name": "Disney"},
    {"ticker": "NFLX",  "name": "Netflix"},
    {"ticker": "AMZN",  "name": "Amazon"},
    {"ticker": "JPM",   "name": "JPMorgan"},
    {"ticker": "PYPL",  "name": "PayPal"},
    {"ticker": "COIN",  "name": "Coinbase"},
    {"ticker": "XOM",   "name": "Exxon"},
    {"ticker": "BA",    "name": "Boeing"},
    {"ticker": "RACE",  "name": "Ferrari"},
]


# ---------------------------------------------------------------------------
# Rilevamento menzioni aziende nel testo
# ---------------------------------------------------------------------------

def contains_confirmation_keyword(text: str) -> bool:
    """Verifica che il testo contenga parole chiave di contesto business per disambiguare i ticker."""
    normalized = text.lower()
    return any(keyword in normalized for keyword in COMPANY_CONFIRM_KEYWORDS)


def detect_mentions(text: str, subreddit: str) -> list[str]:
    """
    Analizza il testo e restituisce i ticker delle aziende menzionate.
    Usa disambiguazione per ticker ambigui.
    """
    normalized = text.lower()
    tickers = []
    for company in COMPANIES:
        name = company["name"].lower()
        ticker = company["ticker"].lower()
        if re.search(rf"\b{re.escape(name)}\b", normalized):
            tickers.append(company["ticker"])
            continue

        if re.search(rf"\b{re.escape(ticker)}\b", normalized) and contains_confirmation_keyword(normalized):
            tickers.append(company["ticker"])

    return tickers


def is_recent_post(post: dict) -> bool:
    """Verifica che il post non sia più vecchio di MAX_POST_AGE_DAYS."""
    created_utc = post.get("created_utc", "")
    if not created_utc:
        return False

    try:
        created_dt = datetime.fromisoformat(created_utc)
    except ValueError:
        return False

    if created_dt.tzinfo is None:
        created_dt = created_dt.replace(tzinfo=timezone.utc)

    return created_dt >= datetime.now(timezone.utc) - timedelta(days=MAX_POST_AGE_DAYS)


def contains_company_reference(text: str, url: Optional[str] = None) -> bool:
    """Controlla se il testo o l'URL menzionano almeno una azienda monitorata con disambiguazione ticker."""
    normalized = text.lower()
    normalized_url = (url or "").lower()
    for company in COMPANIES:
        name = company["name"].lower()
        ticker = company["ticker"].lower()

        if re.search(rf"\b{re.escape(name)}\b", normalized):
            return True

        ticker_in_text = re.search(rf"\b{re.escape(ticker)}\b", normalized)
        ticker_in_url = re.search(rf"\b{re.escape(ticker)}\b", normalized_url)
        name_in_url = name in normalized_url

        if name_in_url:
            return True

        if (ticker_in_text or ticker_in_url) and (
            contains_confirmation_keyword(normalized) or contains_confirmation_keyword(normalized_url)
        ):
            return True

    return False


def contains_entertainment_reference(text: str) -> bool:
    """Esclude i post che parlano di contenuti entertainment generici."""
    normalized = text.lower()
    return any(keyword in normalized for keyword in ENTERTAINMENT_KEYWORDS)


# ---------------------------------------------------------------------------
# Filtro post
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

def is_post_valid(post: dict, min_score: int) -> bool:
    """
    Un post è valido se:
    1. È recente (max una settimana)
    2. Menziona una delle aziende monitorate nel titolo o nell'URL
    3. Non è un contenuto entertainment generico
    """
    if not is_recent_post(post):
        return False

    title = post.get("title", "")
    url = post.get("url", "")
    if not contains_company_reference(title, url):
        return False

    if contains_entertainment_reference(title):
        return False

    is_external = (
        url
        and "reddit.com" not in url
        and url.startswith("http")
    )

    return is_external or post.get("score", 0) >= min_score


# ---------------------------------------------------------------------------
# Creazione archi [:MENZIONATA_IN] in Neo4j
# ---------------------------------------------------------------------------

def create_mentions_relationships(mongo_id: str, tickers: list[str]) -> None:
    """
    Crea archi (:Event)-[:MENZIONATA_IN]->(:Company) in Neo4j
    per ogni ticker rilevato nel testo del post.
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
# Scraping post da old.reddit.com
# ---------------------------------------------------------------------------

def scrape_posts(subreddit: str) -> list[dict]:
    """
    Scarica i post più recenti da old.reddit.com/r/{subreddit}/new.
    Restituisce lista di dizionari con i dati del post.
    """
    url   = f"{BASE_URL}/{subreddit}/new"
    posts = []

    for attempt in range(MAX_RETRIES):
        try:
            # Aggiorna headers per ogni tentativo (User-Agent casuale)
            headers = get_browser_headers()
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 403:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF * (attempt + 1)
                    log.warning("403 Forbidden su r/%s (tentativo %d/%d) — attendo %d secondi",
                              subreddit, attempt + 1, MAX_RETRIES, wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    log.error("403 Forbidden persistente su r/%s — salto dopo %d tentativi",
                            subreddit, MAX_RETRIES)
                    return []

            if response.status_code == 429:
                log.warning("Rate limit su r/%s — attendo 60 secondi", subreddit)
                time.sleep(60)
                return []

            if response.status_code != 200:
                log.warning("r/%s → status %d — salto", subreddit, response.status_code)
                return []

            soup          = BeautifulSoup(response.text, "html.parser")
            post_elements = soup.select("div.thing.link")[:MAX_POSTS_PER_SUB]

            for post in post_elements:
                try:
                    title_el = post.select_one("a.title")
                    title    = title_el.get_text(strip=True) if title_el else ""
                    post_url = title_el["href"] if title_el else ""

                    if post_url.startswith("/"):
                        post_url = f"https://old.reddit.com{post_url}"

                    # Score
                    score_el = post.select_one("div.score.unvoted")
                    score    = score_el.get_text(strip=True) if score_el else "0"
                    try:
                        score = int(score)
                    except ValueError:
                        score = 0

                    # Numero commenti
                    comments_el      = post.select_one("a.comments")
                    num_comments_txt = comments_el.get_text(strip=True) if comments_el else "0"
                    try:
                        num_comments = int(num_comments_txt.split()[0])
                    except (ValueError, IndexError):
                        num_comments = 0

                    # Autore
                    author_el = post.select_one("a.author")
                    author    = author_el.get_text(strip=True) if author_el else "unknown"

                    # ID e timestamp
                    post_id   = post.get("data-fullname", "")
                    timestamp = post.get("data-timestamp", "")
                    try:
                        created_utc = datetime.fromtimestamp(
                            int(timestamp) / 1000, tz=timezone.utc
                        ).isoformat()
                    except (ValueError, TypeError):
                        created_utc = datetime.now(timezone.utc).isoformat()

                    # Permalink alla pagina commenti
                    permalink_el = post.select_one("a.comments")
                    permalink    = permalink_el["href"] if permalink_el else ""
                    if permalink.startswith("/"):
                        permalink = f"https://old.reddit.com{permalink}"

                    if title:
                        posts.append({
                            "post_id":      post_id,
                            "subreddit":    subreddit,
                            "title":        title,
                            "url":          post_url,
                            "permalink":    permalink,
                            "score":        score,
                            "num_comments": num_comments,
                            "author":       author,
                            "created_utc":  created_utc,
                        })
                except Exception as e:
                    log.warning("Errore parsing post in r/%s → %s", subreddit, e)
                    continue

            # Se siamo arrivati qui, abbiamo avuto successo
            log.info("r/%s → %d post trovati", subreddit, len(posts))
            return posts

        except requests.exceptions.RequestException as e:
            log.error("Errore richiesta r/%s → %s", subreddit, e)
            if attempt == MAX_RETRIES - 1:
                return []


# ---------------------------------------------------------------------------
# Scraping commenti e risposte
# ---------------------------------------------------------------------------

def scrape_comments_and_replies(permalink: str) -> list[dict]:
    """
    Scarica dalla pagina del post:
    - I 5 commenti top per upvote (score > 5, lunghezza > 50)
    - Per ogni commento top: la risposta con più upvote

    Restituisce lista di dizionari con testo, score e tipo (comment/reply).
    """
    if not permalink:
        return []

    if "reddit.com" in permalink and "old.reddit.com" not in permalink:
        permalink = permalink.replace("reddit.com", "old.reddit.com")

    results = []

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(SLEEP_BETWEEN_PAGES)
            headers = get_browser_headers()
            response = requests.get(permalink, headers=headers, timeout=10)

            if response.status_code == 403:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF * (attempt + 1)
                    log.warning("403 Forbidden commenti (tentativo %d/%d) — attendo %d secondi",
                              attempt + 1, MAX_RETRIES, wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    log.error("403 Forbidden persistente commenti — salto dopo %d tentativi", MAX_RETRIES)
                    return []

            if response.status_code == 429:
                log.warning("Rate limit commenti — attendo 60 secondi")
                time.sleep(60)
                return []

            if response.status_code != 200:
                return []

            soup = BeautifulSoup(response.text, "html.parser")

            # Seleziona commenti di primo livello
            top_level_comments = soup.select(
                "div.commentarea > div.sitetable > div.thing.comment"
            )

            valid_comments = []

            for comment_el in top_level_comments:
                body_el = comment_el.select_one("div.usertext-body")
                if not body_el:
                    continue
                text = body_el.get_text(strip=True)

                if len(text) < MIN_COMMENT_LENGTH:
                    continue

                score = None
                score_el = comment_el.select_one("span.score")
                if score_el:
                    score_text = score_el.get_text(strip=True)
                    if score_text and "punteggio nascosto" not in score_text.lower():
                        try:
                            score = int(score_text.split()[0])
                        except (ValueError, IndexError):
                            score = 0

                if score is not None and score < MIN_COMMENT_SCORE:
                    continue

                valid_comments.append({
                    "text":    text[:600],
                    "score":   score or 0,
                    "element": comment_el,
                    "type":    "comment",
                })

            # Ordina per score e prendi i top 5
            valid_comments.sort(key=lambda x: x["score"], reverse=True)
            top_comments = valid_comments[:MAX_TOP_COMMENTS]

            log_top_pairs = []
            for comment in top_comments:
                results.append({
                    "text":  comment["text"],
                    "score": comment["score"],
                    "type":  "comment",
                })

                # Risposta con più upvote al commento top
                reply_els        = comment["element"].select(
                    "div.child > div.sitetable > div.thing.comment"
                )
                best_reply       = None
                best_reply_score = -1

                for reply_el in reply_els:
                    reply_body = reply_el.select_one("div.usertext-body")
                    if not reply_body:
                        continue
                    reply_text = reply_body.get_text(strip=True)

                    if len(reply_text) < MIN_COMMENT_LENGTH:
                        continue

                    reply_score_el = reply_el.select_one("span.score.unvoted")
                    try:
                        reply_score = int(
                            reply_score_el.get_text(strip=True).split()[0]
                        ) if reply_score_el else 0
                    except (ValueError, IndexError):
                        reply_score = 0

                    if reply_score > best_reply_score:
                        best_reply_score = reply_score
                        best_reply       = reply_text[:600]

                if best_reply:
                    log_top_pairs.append(f"'{comment['text'][:60]}' -> '{best_reply[:60]}'")
                    results.append({
                        "text":  best_reply,
                        "score": best_reply_score,
                        "type":  "reply",
                    })

            if log_top_pairs:
                log.info("Top 5 commenti + top reply per r/%s: %s", permalink, " | ".join(log_top_pairs))

            # Se siamo arrivati qui, abbiamo avuto successo
            return results

        except requests.exceptions.RequestException as e:
            log.error("Errore scraping commenti → %s", e)
            if attempt == MAX_RETRIES - 1:
                return []


# ---------------------------------------------------------------------------
# Job principale
# ---------------------------------------------------------------------------

def ingestion_job() -> None:
    """
    Job principale schedulato ogni 2 ore.
    Per ogni subreddit:
      1. Scraping post
      2. Filtro: link esterno OR score >= soglia
      3. Per ogni post valido:
         a. Scraping 5 commenti top + 1 risposta top per commento
         b. Rileva menzioni aziende
         c. Dual-write MongoDB + Neo4j
         d. Crea archi [:MENZIONATA_IN]
    """
    log.info("=" * 60)
    log.info("Avvio job ingestion Reddit — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    processed_post_ids: set[str] = set()
    # Test mode: limit subreddit, max posts per sub, for quick local runs
    test_sub = os.getenv("TEST_ONLY_SUBREDDIT")
    test_max   = os.getenv("TEST_MAX_POSTS")
    if test_max:
        try:
            global MAX_POSTS_PER_SUB
            MAX_POSTS_PER_SUB = int(test_max)
        except ValueError:
            pass
    total_saved    = 0
    total_filtered = 0

    # Test mode: optional skip of real DB writes to allow quick verification
    if os.getenv("TEST_SKIP_LAYER"):
        log.info("Test mode: skipping IngestionLayer (no DB writes).")
        log.info("Test complete (no DB writes).")
        return
    with IngestionLayer() as il:
        for category, config in SUBREDDITS.items():
            min_score = config["min_score"]
            log.info("--- Categoria: %s (min_score: %d) ---", category, min_score)

            for subreddit in config["subs"]:
                if test_sub and subreddit != test_sub:
                    log.info("Test mode: skipping r/%s", subreddit)
                    continue
                language = "it" if subreddit in ITALIAN_SUBREDDITS else "en"
                posts    = scrape_posts(subreddit)
                time.sleep(SLEEP_BETWEEN_PAGES)

                for post in posts:
                    post_id = post["post_id"]

                    if post_id in processed_post_ids:
                        continue
                    processed_post_ids.add(post_id)

                    # Applica filtro
                    if not is_post_valid(post, min_score):
                        total_filtered += 1
                        continue

                    # Scraping commenti e risposte
                    comments = scrape_comments_and_replies(post["permalink"])
                    # Log top 5 commenti + top reply (se presenti) per verifica rapida
                    if comments:
                        top_comments = [c for c in comments if c.get("type") == "comment"][:5]
                        pairs = []
                        for tc in top_comments:
                            br = tc.get("best_reply")
                            if br:
                                pairs.append(f"'{tc.get('text','')[:60]}' -> '{br[:60]}'")
                        if pairs:
                            log.info("Top 5 commenti + top reply per r/%s: %s", subreddit, " | ".join(pairs))

                    # Censura parolacce nel titolo e nei commenti
                    clean_title    = censor_text(post["title"])
                    clean_comments = [
                        {**c, "text": censor_text(c["text"])}
                        for c in comments
                    ]

                    # Testo completo per rilevamento menzioni (su testo censurato)
                    full_text         = clean_title + " " + " ".join(c["text"] for c in clean_comments)
                    mentioned_tickers = detect_mentions(full_text, subreddit)

                    # Determina se è link esterno
                    is_external = (
                        "reddit.com" not in post["url"]
                        and post["url"].startswith("http")
                    )

                    payload = {
                        "subreddit":    subreddit,
                        "post_id":      post_id,
                        "title":        clean_title,
                        "url":          post["url"],
                        "permalink":    post["permalink"],
                        "is_external":  is_external,
                        "score":        post["score"],
                        "num_comments": post["num_comments"],
                        "author":       post["author"],
                        "created_utc":  post["created_utc"],
                        "language":     language,
                        "comments":     clean_comments,
                        "mentions":     mentioned_tickers,
                    }

                    if os.getenv("TEST_SKIP_DB"):
                        mongo_id = "test-mongo-id"
                    else:
                        mongo_id = il.dual_write(
                            source     = "reddit",
                            data_type  = "social",
                            node_label = "Event",
                            payload    = payload,
                        )

                    if mongo_id:
                        create_mentions_relationships(mongo_id, mentioned_tickers)
                        total_saved += 1
                        log.info(
                            "✓ r/%s | score: %d | ext: %s | commenti: %d | '%s'",
                            subreddit,
                            post["score"],
                            "sì" if is_external else "no",
                            len(comments),
                            post["title"][:50],
                        )

                time.sleep(SLEEP_BETWEEN_SUBS)

    log.info("=" * 60)
    log.info("Job completato → salvati: %d | filtrati: %d", total_saved, total_filtered)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Avvio ingest_reddit.py")
    log.info("Subreddit monitorati: %d", sum(len(v["subs"]) for v in SUBREDDITS.values()))
    log.info("Aziende monitorate: %d", len(COMPANIES))

    # Esegui il job una volta e poi esci
    ingestion_job()
    log.info("Job completato. Uscita.")
