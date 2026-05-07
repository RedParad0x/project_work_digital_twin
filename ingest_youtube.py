"""
ingest_youtube.py
=================
Script di ingestion per video e commenti da YouTube tramite YouTube Data API v3.

Logica:
  1. Canali news fissi (Bloomberg, CNBC, Reuters, ecc.) → ultimi 2 video
  2. Ricerca EN per keyword aziendale + termini finanziari
  3. Ricerca IT per keyword aziendale + termini finanziari in italiano
  4. Filtro: views >= 5.000 + hashtag finanziari (escluso canali fissi)
  5. Finestra temporale: ultimi 7 giorni
  6. Commenti filtrati per lingua: it, en, es, pt, de, fr
  7. Ogni commento taggato con lingua rilevata (langdetect)
  8. Dual-write MongoDB + Neo4j tramite ingestion_layer.py
  9. Archi [:MENZIONATA_IN] verso nodi Company
  10. Deduplicazione su video_id

Limiti YouTube API piano gratuito:
  - 10.000 unità/giorno
  - search.list: 100 unità per query
  - videos.list: 1 unità per richiesta
  - commentThreads.list: 1 unità per pagina

Autori : Massimo Mazzini, Rafaele Rossi
Corso  : Big Data Engineer & Solution Architect — 2° anno
"""

from __future__ import annotations

import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import schedule
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from langdetect import detect, LangDetectException
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# Aggiunge la cartella scripts al path per importare ingestion_layer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ingestion_layer import IngestionLayer

# ---------------------------------------------------------------------------
# Caricamento variabili d'ambiente
# ---------------------------------------------------------------------------
load_dotenv()

YOUTUBE_API_KEY = os.getenv("GOOGLE_API_KEY")
NEO4J_URI       = os.getenv("NEO4J_URI")
NEO4J_USER      = os.getenv("NEO4J_USER")
NEO4J_PASSWORD  = os.getenv("NEO4J_PASSWORD")

if not YOUTUBE_API_KEY:
    log.error("GOOGLE_API_KEY non trovato nel .env — uscita")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configurazione logging (terminale + file)
# ---------------------------------------------------------------------------
start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_DIR    = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "ingestion_youtube")
LOG_FILE   = os.path.join(LOG_DIR, f"youtube_{start_time}.log")
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

log = logging.getLogger("ingest_youtube")
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

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------
MAX_VIDEOS_PER_QUERY   = 5      # video per query di ricerca
MAX_VIDEOS_PER_CHANNEL = 2      # video per canale fisso
MAX_COMMENTS_PER_VIDEO = 50     # commenti per video
MIN_COMMENT_LENGTH     = 20     # lunghezza minima commento
MIN_VIEWS              = 5_000  # visualizzazioni minime
SLEEP_BETWEEN_REQUESTS = 1      # secondi tra richieste API
SLEEP_BETWEEN_QUERIES  = 3      # secondi tra query diverse
DAYS_WINDOW            = 7      # finestra temporale in giorni

# Lingue accettate per i commenti
ACCEPTED_LANGUAGES = {"it", "en", "es", "pt", "de", "fr"}

# ---------------------------------------------------------------------------
# Canali news fissi — Channel ID ufficiali
# ---------------------------------------------------------------------------
FIXED_CHANNELS = [
    {"channel_id": "UCIALMKvObZNtJ6AmdCLP7Lg", "name": "Bloomberg Television",  "language": "en"},
    {"channel_id": "UChirEOpgFCupRAk5etXqPaA", "name": "Bloomberg News",        "language": "en"},
    {"channel_id": "UCrp_UI8XtuYfpiqluWLD7Lw", "name": "CNBC Television",       "language": "en"},
    {"channel_id": "UCNye-wNBqNL5ZzHSJj3l8Bg", "name": "Reuters",               "language": "en"},
    {"channel_id": "UCK7tptUDHh-RYDsdxO1-5QQ", "name": "Financial Times",       "language": "en"},
    {"channel_id": "UCK7tptUDHh-RYDsdxO1-5QQ", "name": "Wall Street Journal",   "language": "en"},
    {"channel_id": "UCCjyq_K1Pd2Rg4Uh3qFHVvg", "name": "Yahoo Finance",         "language": "en"},
    {"channel_id": "UCddiUEpeqJcYeBxX1IVBKvQ", "name": "The Verge",             "language": "en"},
    {"channel_id": "UCBcRF18a7Qf58cCRy5xuWwQ", "name": "MKBHD",                 "language": "en"},
    {"channel_id": "UC16niRr50-MSBwiO3YDb3RA", "name": "BBC News",              "language": "en"},
    {"channel_id": "UCupvZG-5ko_eiXAupbDfxWw", "name": "CNN",                   "language": "en"},
    {"channel_id": "UCMqMBJjBJ1s5_ZfHhLTd-4g", "name": "Il Sole 24 Ore",       "language": "it"},
]

# ---------------------------------------------------------------------------
# Hashtag finanziari per filtro contesto
# Fonte: Contentworks Finance Hashtags 2026, canali news YouTube verificati
# ---------------------------------------------------------------------------
FINANCIAL_HASHTAGS = {
    # --- INGLESE — Mercati e Trading ---
    "#stocks", "#stock", "#stockmarket", "#stocknews", "#stockpicks",
    "#investing", "#investment", "#investingtips", "#investor",
    "#trading", "#daytrading", "#swingtrading", "#optionstrading",
    "#finance", "#financenews", "#financialnews", "#financialeducation",
    "#financialliteracy", "#financialfreedom",
    "#wallstreet", "#wallstreetbets", "#nasdaq", "#nyse", "#sp500",
    "#dowjones", "#russell2000", "#markets", "#marketwatch", "#marketnews",
    "#earnings", "#earningsreport", "#earningsseason",
    "#economy", "#economics", "#macroeconomics",
    "#business", "#businessnews", "#corporate",
    "#ipo", "#merger", "#acquisition", "#dividends", "#portfolio",
    "#bullmarket", "#bearmarket", "#recession", "#inflation",
    "#fed", "#federalreserve", "#interestrates",
    # --- INGLESE — Crypto ---
    "#crypto", "#cryptocurrency", "#cryptonews", "#cryptomarket",
    "#bitcoin", "#btc", "#ethereum", "#eth", "#defi", "#blockchain",
    "#altcoin", "#diamondhands", "#hodl",
    # --- INGLESE — Tech & AI ---
    "#tech", "#technews", "#technology", "#ai", "#artificialintelligence",
    "#semiconductor", "#chip", "#bigtech", "#techstock",
    # --- INGLESE — Fintech ---
    "#fintech", "#personalfinance", "#wealthmanagement",
    "#investmentbanking", "#hedgefund", "#venturecapital",
    # --- TICKER SPECIFICI ---
    "#nvda", "#tsla", "#aapl", "#meta", "#googl", "#msft",
    "#dis", "#nflx", "#amzn", "#jpm", "#pypl", "#coin",
    "#xom", "#ba", "#race",
    "#nvidia", "#tesla", "#apple", "#microsoft", "#amazon",
    "#disney", "#netflix", "#ferrari", "#boeing", "#exxon",
    "#paypal", "#coinbase", "#jpmorgan", "#google", "#alphabet",
    # --- ITALIANO ---
    "#borsa", "#borsaitaliana", "#azioni", "#azionario",
    "#finanza", "#finanzapersonale", "#mercati", "#mercatoazionario",
    "#investimenti", "#investire", "#trading", "#economia",
    "#notizie", "#notizieeconomiche", "#criptovalute", "#bitcoin",
    # --- SPAGNOLO ---
    "#bolsa", "#bolsadevalores", "#acciones", "#inversiones",
    "#mercados", "#economia", "#finanzas", "#trading",
    # --- FRANCESE ---
    "#bourse", "#finance", "#investissement", "#marches", "#trading",
    # --- TEDESCO ---
    "#aktien", "#boerse", "#finanzen", "#investieren", "#wirtschaft",
    # --- PORTOGHESE ---
    "#bolsadevalores", "#acoes", "#investimentos", "#mercadofinanceiro",
}

# Keyword finanziarie nei titoli — fallback se nessun hashtag trovato
FINANCIAL_TITLE_KEYWORDS = {
    # Inglese
    "stock", "stocks", "earnings", "market", "investing", "finance",
    "trading", "nasdaq", "nyse", "sp500", "crypto", "bitcoin",
    "economy", "recession", "inflation", "fed", "ipo", "merger",
    "acquisition", "revenue", "profit", "loss", "quarterly", "annual",
    "dividend", "analyst", "forecast", "guidance", "outlook",
    # Italiano
    "borsa", "azioni", "finanza", "mercato", "investimento", "economia",
    "utile", "ricavi", "trimestrale", "dividendo", "analista",
    # Spagnolo
    "bolsa", "acciones", "mercado", "economia", "inversion",
    # Francese
    "bourse", "actions", "marche", "economie", "investissement",
    # Tedesco
    "aktien", "boerse", "markt", "wirtschaft", "investition",
    # Portoghese
    "bolsa", "acoes", "mercado", "economia", "investimento",
}

# ---------------------------------------------------------------------------
# Aziende monitorate
# ---------------------------------------------------------------------------
COMPANIES = [
    {"ticker": "NVDA",  "name": "NVIDIA",     "category": "Big Tech"},
    {"ticker": "TSLA",  "name": "Tesla",      "category": "Big Tech"},
    {"ticker": "AAPL",  "name": "Apple",      "category": "Big Tech"},
    {"ticker": "META",  "name": "Meta",       "category": "Big Tech"},
    {"ticker": "GOOGL", "name": "Google",     "category": "Big Tech"},
    {"ticker": "MSFT",  "name": "Microsoft",  "category": "Big Tech"},
    {"ticker": "DIS",   "name": "Disney",     "category": "Consumer & Entertainment"},
    {"ticker": "NFLX",  "name": "Netflix",    "category": "Consumer & Entertainment"},
    {"ticker": "AMZN",  "name": "Amazon",     "category": "Consumer & Entertainment"},
    {"ticker": "JPM",   "name": "JPMorgan",   "category": "Financial & Fintech"},
    {"ticker": "PYPL",  "name": "PayPal",     "category": "Financial & Fintech"},
    {"ticker": "COIN",  "name": "Coinbase",   "category": "Financial & Fintech"},
    {"ticker": "XOM",   "name": "Exxon",      "category": "Industrials & Energy"},
    {"ticker": "BA",    "name": "Boeing",     "category": "Industrials & Energy"},
    {"ticker": "RACE",  "name": "Ferrari",    "category": "Industrials & Energy"},
]

# Query EN per ticker — focus su notizie finanziarie e aziendali
# Per brand generici (Netflix, Disney, Amazon, Google) la query include
# sempre termini finanziari per evitare clip di serie TV, tutorial, ecc.
QUERIES_EN = {
    "NVDA":  "NVIDIA stock OR NVIDIA earnings OR NVIDIA AI chips OR NVDA",
    "TSLA":  "Tesla stock OR Tesla earnings OR Tesla news OR TSLA",
    "AAPL":  "Apple stock OR Apple earnings OR Apple revenue OR AAPL",
    "META":  "Meta stock OR Meta earnings OR Meta layoffs OR META",
    "GOOGL": "Google stock OR Alphabet earnings OR Google revenue OR GOOGL",
    "MSFT":  "Microsoft stock OR Microsoft earnings OR Microsoft layoffs OR MSFT",
    "DIS":   "Disney stock OR Disney earnings OR Disney revenue OR DIS stock",
    "NFLX":  "Netflix stock OR Netflix earnings OR Netflix revenue OR NFLX",
    "AMZN":  "Amazon stock OR Amazon earnings OR Amazon revenue OR AMZN",
    "JPM":   "JPMorgan stock OR JPMorgan earnings OR JPM stock",
    "PYPL":  "PayPal stock OR PayPal earnings OR PYPL stock",
    "COIN":  "Coinbase stock OR Coinbase earnings OR COIN stock crypto",
    "XOM":   "Exxon stock OR Exxon earnings OR XOM stock oil",
    "BA":    "Boeing stock OR Boeing earnings OR Boeing news OR BA stock",
    "RACE":  "Ferrari stock OR Ferrari earnings OR RACE stock NYSE",
}

# Query IT per ticker — focus su borsa e notizie aziendali
QUERIES_IT = {
    "NVDA":  "NVIDIA azioni OR NVIDIA borsa OR NVIDIA notizie",
    "TSLA":  "Tesla azioni OR Tesla borsa OR Tesla notizie",
    "AAPL":  "Apple azioni OR Apple borsa OR Apple notizie",
    "META":  "Meta azioni OR Meta borsa OR Meta notizie",
    "GOOGL": "Google azioni OR Alphabet borsa OR Google notizie",
    "MSFT":  "Microsoft azioni OR Microsoft borsa OR Microsoft notizie",
    "DIS":   "Disney azioni OR Disney borsa OR Disney titolo",
    "NFLX":  "Netflix azioni OR Netflix borsa OR Netflix titolo",
    "AMZN":  "Amazon azioni OR Amazon borsa OR Amazon titolo",
    "JPM":   "JPMorgan azioni OR JPMorgan borsa",
    "PYPL":  "PayPal azioni OR PayPal borsa",
    "COIN":  "Coinbase azioni OR Coinbase borsa OR Coinbase crypto",
    "XOM":   "Exxon azioni OR Exxon borsa OR Exxon petrolio",
    "BA":    "Boeing azioni OR Boeing borsa OR Boeing notizie",
    "RACE":  "Ferrari azioni OR Ferrari borsa OR Ferrari titolo",
}

# Categorie YouTube per filtro contesto
# 25 = News & Politics, 28 = Science & Technology, 27 = Education
FINANCIAL_VIDEO_CATEGORIES = {"25", "28", "27"}

# ---------------------------------------------------------------------------
# Quota giornaliera
# ---------------------------------------------------------------------------
_daily_quota_used = 0
_last_reset_date  = datetime.now(timezone.utc).date()
MAX_DAILY_QUOTA   = 9_500


def _check_quota(cost: int) -> bool:
    global _daily_quota_used, _last_reset_date
    today = datetime.now(timezone.utc).date()
    if today > _last_reset_date:
        log.info("Reset quota giornaliera")
        _daily_quota_used = 0
        _last_reset_date  = today
    if _daily_quota_used + cost > MAX_DAILY_QUOTA:
        log.warning("Quota raggiunta (%d/%d)", _daily_quota_used, MAX_DAILY_QUOTA)
        return False
    _daily_quota_used += cost
    return True


# ---------------------------------------------------------------------------
# Costruzione client YouTube
# ---------------------------------------------------------------------------

def _build_youtube():
    return build(
        "youtube", "v3",
        developerKey   = YOUTUBE_API_KEY,
        cache_discovery = False,
        static_discovery = False,
    )


# ---------------------------------------------------------------------------
# Estrazione hashtag dalla descrizione
# ---------------------------------------------------------------------------

def extract_hashtags(text: str) -> set[str]:
    return {tag.lower() for tag in re.findall(r"#\w+", text)}


def has_financial_context(description: str, title: str = "") -> bool:
    """
    Verifica se il video ha contesto finanziario.
    Prima controlla gli hashtag nella descrizione (più affidabile),
    poi come fallback controlla le keyword nel titolo.
    """
    # 1. Hashtag nella descrizione (controllo principale)
    hashtags = extract_hashtags(description)
    if hashtags & FINANCIAL_HASHTAGS:
        return True

    # 2. Fallback: keyword finanziarie nel titolo
    title_lower = title.lower()
    if any(kw in title_lower for kw in FINANCIAL_TITLE_KEYWORDS):
        return True

    return False


# ---------------------------------------------------------------------------
# Rilevamento lingua commento
# ---------------------------------------------------------------------------

def detect_language(text: str) -> str:
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"


# ---------------------------------------------------------------------------
# Rilevamento menzioni aziende
# ---------------------------------------------------------------------------

def detect_mentions(text: str) -> list[str]:
    text_lower = text.lower()
    return [
        c["ticker"] for c in COMPANIES
        if c["name"].lower() in text_lower or c["ticker"].lower() in text_lower
    ]


# ---------------------------------------------------------------------------
# Creazione archi [:MENZIONATA_IN] in Neo4j
# ---------------------------------------------------------------------------

def create_mentions_relationships(mongo_id: str, tickers: list[str]) -> None:
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
                """, mongo_id=mongo_id, ticker=ticker)
        driver.close()
        log.info("Archi [:MENZIONATA_IN] → %s", tickers)
    except Neo4jError as e:
        log.error("Errore archi Neo4j → %s", e)


# ---------------------------------------------------------------------------
# Fetch statistiche video
# ---------------------------------------------------------------------------

def fetch_video_stats(video_id: str) -> dict:
    try:
        youtube  = _build_youtube()
        response = youtube.videos().list(id=video_id, part="statistics").execute()
        items    = response.get("items", [])
        if not items:
            return {}
        stats = items[0].get("statistics", {})
        return {
            "view_count":    int(stats.get("viewCount", 0)),
            "like_count":    int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
        }
    except Exception as e:
        log.error("Errore stats video %s → %s", video_id, e)
        return {}


# ---------------------------------------------------------------------------
# Fetch commenti con filtro lingua
# ---------------------------------------------------------------------------

def fetch_comments(video_id: str) -> list[dict]:
    try:
        youtube          = _build_youtube()
        comments         = []
        next_page_token  = None

        while len(comments) < MAX_COMMENTS_PER_VIDEO:
            response = youtube.commentThreads().list(
                videoId        = video_id,
                part           = "snippet,replies",
                maxResults     = min(20, MAX_COMMENTS_PER_VIDEO - len(comments)),
                pageToken      = next_page_token,
                textFormat     = "plainText",
                order          = "relevance",
            ).execute()

            items = response.get("items", [])
            if not items:
                break

            for item in items:
                top_snippet  = item["snippet"]["topLevelComment"]["snippet"]
                comment_text = top_snippet.get("textDisplay", "")[:500]

                if len(comment_text) < MIN_COMMENT_LENGTH:
                    continue

                # Filtro lingua
                lang = detect_language(comment_text)
                if lang not in ACCEPTED_LANGUAGES:
                    continue

                comments.append({
                    "author":       top_snippet.get("authorDisplayName", ""),
                    "text":         comment_text,
                    "likes":        top_snippet.get("likeCount", 0),
                    "published_at": top_snippet.get("publishedAt", ""),
                    "type":         "comment",
                    "language":     lang,
                })

                # Risposta con più like
                if item["snippet"].get("replyCount", 0) > 0 and item.get("replies"):
                    best_reply = max(
                        item["replies"]["comments"],
                        key=lambda r: r["snippet"].get("likeCount", 0),
                    )
                    reply_text = best_reply["snippet"].get("textDisplay", "")[:500]
                    reply_lang = detect_language(reply_text)

                    if len(reply_text) >= MIN_COMMENT_LENGTH and reply_lang in ACCEPTED_LANGUAGES:
                        comments.append({
                            "author":       best_reply["snippet"].get("authorDisplayName", ""),
                            "text":         reply_text,
                            "likes":        best_reply["snippet"].get("likeCount", 0),
                            "published_at": best_reply["snippet"].get("publishedAt", ""),
                            "type":         "reply",
                            "language":     reply_lang,
                        })

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        log.info("Video %s → %d commenti validi", video_id, len(comments))
        return comments

    except Exception as e:
        log.error("Errore commenti %s → %s", video_id, e)
        return []


# ---------------------------------------------------------------------------
# Ricerca video per query
# ---------------------------------------------------------------------------

def search_videos(
    query: str,
    max_results: int = MAX_VIDEOS_PER_QUERY,
    language: str | None = None,
    channel_id: str | None = None,
) -> list[dict]:
    """
    Cerca video su YouTube.
    Se channel_id è fornito, cerca solo in quel canale.
    Se language è fornito, filtra per lingua rilevante.
    Finestra temporale: ultimi 7 giorni.
    """
    if not _check_quota(100):
        return []

    try:
        youtube      = _build_youtube()
        published_after = (
            datetime.now(timezone.utc) - timedelta(days=DAYS_WINDOW)
        ).isoformat()

        params = {
            "q":              query,
            "part":           "snippet",
            "type":           "video",
            "maxResults":     min(max_results, 50),
            "order":          "relevance",
            "publishedAfter": published_after,
        }
        if language:
            params["relevanceLanguage"] = language
        if channel_id:
            params["channelId"] = channel_id

        response = youtube.search().list(**params).execute()

        # Raccogli tutti i video_id per fetch categoria in batch (1 unità)
        video_ids = [
            item["id"].get("videoId")
            for item in response.get("items", [])
            if item["id"].get("videoId")
        ]

        # Fetch categorie in batch — filtra per News/Tech/Education
        category_map = {}
        if video_ids and not channel_id:
            try:
                cat_response = youtube.videos().list(
                    id   = ",".join(video_ids),
                    part = "snippet",
                ).execute()
                for item in cat_response.get("items", []):
                    vid_id   = item["id"]
                    cat_id   = item["snippet"].get("categoryId", "")
                    category_map[vid_id] = cat_id
            except Exception as e:
                log.warning("Errore fetch categorie → %s", e)

        videos = []
        for item in response.get("items", []):
            video_id = item["id"].get("videoId")
            snippet  = item["snippet"]
            if not video_id:
                continue

            # Filtro categoria — solo per ricerche keyword, non canali fissi
            if not channel_id and category_map:
                cat_id = category_map.get(video_id, "")
                if cat_id and cat_id not in FINANCIAL_VIDEO_CATEGORIES:
                    log.info("Video scartato per categoria %s → '%s'", cat_id, snippet.get("title", "")[:50])
                    continue

            videos.append({
                "video_id":      video_id,
                "title":         snippet.get("title", ""),
                "description":   snippet.get("description", ""),
                "channel":       snippet.get("channelTitle", ""),
                "channel_id":    snippet.get("channelId", ""),
                "published_at":  snippet.get("publishedAt", ""),
                "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
            })

        log.info("Search '%s' → %d video trovati", query[:60], len(videos))
        return videos

    except HttpError as e:
        log.error("Errore ricerca YouTube → %s", e)
        return []
    except Exception as e:
        log.error("Errore inatteso ricerca → %s", e)
        return []


# ---------------------------------------------------------------------------
# Processa un singolo video
# ---------------------------------------------------------------------------

def process_video(
    video: dict,
    il: IngestionLayer,
    processed_ids: set[str],
    require_hashtag: bool = True,
    default_language: str = "en",
) -> bool:
    """
    Processa un video: statistiche, filtri, commenti, dual-write.
    require_hashtag=False per i canali fissi (contesto già garantito).
    Restituisce True se il video è stato salvato.
    """
    video_id = video["video_id"]

    if video_id in processed_ids:
        log.info("Video duplicato saltato → %s", video_id)
        return False
    processed_ids.add(video_id)

    # Statistiche (1 unità)
    if not _check_quota(1):
        return False

    stats = fetch_video_stats(video_id)
    time.sleep(SLEEP_BETWEEN_REQUESTS)

    # Filtro views minime
    if stats.get("view_count", 0) < MIN_VIEWS:
        log.info("Video saltato → views insufficienti (%d < %d)", stats.get("view_count", 0), MIN_VIEWS)
        return False

    # Filtro hashtag finanziari (solo per ricerche keyword, non canali fissi)
    if require_hashtag and not has_financial_context(video.get("description", ""), video.get("title", "")):
        log.info("Video saltato → nessun hashtag finanziario | '%s'", video["title"][:50])
        return False

    # Commenti (1 unità per pagina)
    if not _check_quota(MAX_COMMENTS_PER_VIDEO // 20):
        return False

    comments = fetch_comments(video_id)
    time.sleep(SLEEP_BETWEEN_REQUESTS)

    if not comments:
        log.info("Video saltato → nessun commento valido | '%s'", video["title"][:50])
        return False

    # Rilevamento menzioni
    full_text         = video["title"] + " " + video["description"] + " " + " ".join(c["text"] for c in comments)
    mentioned_tickers = detect_mentions(full_text)

    payload = {
        "video_id":      video_id,
        "video_title":   video["title"],
        "video_url":     f"https://www.youtube.com/watch?v={video_id}",
        "description":   video["description"],
        "channel":       video["channel"],
        "channel_id":    video.get("channel_id", ""),
        "published_at":  video["published_at"],
        "thumbnail_url": video["thumbnail_url"],
        "view_count":    stats.get("view_count", 0),
        "like_count":    stats.get("like_count", 0),
        "comment_count": stats.get("comment_count", 0),
        "language":      default_language,
        "comments":      comments,
        "mentions":      mentioned_tickers,
        "hashtags":      list(extract_hashtags(video.get("description", ""))),
    }

    mongo_id = il.dual_write(
        source     = "youtube",
        data_type  = "social",
        node_label = "Event",
        payload    = payload,
    )

    if mongo_id:
        create_mentions_relationships(mongo_id, mentioned_tickers)
        log.info(
            "✓ YouTube | views: %d | commenti: %d | lang: %s | '%s'",
            stats.get("view_count", 0),
            len(comments),
            default_language,
            video["title"][:50],
        )
        return True

    return False


# ---------------------------------------------------------------------------
# Job principale
# ---------------------------------------------------------------------------

def ingestion_job() -> None:
    log.info("=" * 60)
    log.info("Avvio job ingestion YouTube — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    processed_ids: set[str] = set()
    total_saved   = 0
    total_filtered = 0

    with IngestionLayer() as il:

        # -- 1. Canali news fissi --
        log.info("--- Canali news fissi (%d) ---", len(FIXED_CHANNELS))
        for channel in FIXED_CHANNELS:
            videos = search_videos(
                query      = "news OR earnings OR stock OR market",
                max_results = MAX_VIDEOS_PER_CHANNEL,
                channel_id  = channel["channel_id"],
                language    = channel["language"],
            )
            time.sleep(SLEEP_BETWEEN_QUERIES)

            for video in videos:
                saved = process_video(
                    video,
                    il,
                    processed_ids,
                    require_hashtag  = False,   # canali fissi — no filtro hashtag
                    default_language = channel["language"],
                )
                total_saved   += 1 if saved else 0
                total_filtered += 0 if saved else 1

        # -- 2. Ricerca EN per keyword aziendale --
        log.info("--- Ricerca EN per keyword aziendale ---")
        for company in COMPANIES:
            ticker = company["ticker"]
            query  = QUERIES_EN[ticker]

            videos = search_videos(query, MAX_VIDEOS_PER_QUERY, language="en")
            time.sleep(SLEEP_BETWEEN_QUERIES)

            for video in videos:
                saved = process_video(
                    video, il, processed_ids,
                    require_hashtag  = True,
                    default_language = "en",
                )
                total_saved    += 1 if saved else 0
                total_filtered += 0 if saved else 1

        # -- 3. Ricerca IT per keyword aziendale --
        log.info("--- Ricerca IT per keyword aziendale ---")
        for company in COMPANIES:
            ticker = company["ticker"]
            query  = QUERIES_IT[ticker]

            videos = search_videos(query, MAX_VIDEOS_PER_QUERY, language="it")
            time.sleep(SLEEP_BETWEEN_QUERIES)

            for video in videos:
                saved = process_video(
                    video, il, processed_ids,
                    require_hashtag  = True,
                    default_language = "it",
                )
                total_saved    += 1 if saved else 0
                total_filtered += 0 if saved else 1

    log.info("=" * 60)
    log.info(
        "Job completato → salvati: %d | filtrati: %d | quota: %d/%d",
        total_saved, total_filtered, _daily_quota_used, MAX_DAILY_QUOTA,
    )
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Avvio ingest_youtube.py")
    log.info("Aziende monitorate: %d", len(COMPANIES))
    log.info("Canali fissi: %d", len(FIXED_CHANNELS))
    log.info("Finestra temporale: %d giorni", DAYS_WINDOW)
    log.info("Views minime: %d", MIN_VIEWS)

    # Esegui subito al lancio
    ingestion_job()

    # Controllo modalità singola esecuzione
    if os.getenv("PIPELINE_SINGLE_RUN") == "1":
        log.info("Modalità singola esecuzione — uscita.")
        sys.exit(0)

    # Schedula ogni ora
    schedule.every(1).hours.do(ingestion_job)

    log.info("Scheduler attivo — job ogni ora. Premi CTRL+C per fermare.")

    while True:
        schedule.run_pending()
        time.sleep(1)
