# Media Digital Twin — Dashboard Streamlit

## Struttura file

```
PROGETTO/
├── Home.py                  ← entry point
└── pages/
    ├── 1_MongoDB.py         ← Data Lake Explorer
    ├── 2_Neo4j.py           ← Knowledge Graph Explorer
    ├── 3_Confronto.py       ← Confronto MongoDB vs Neo4j (tesina)
    ├── 4_Aziende.py         ← Analisi per singola azienda
    └── 5_Pipeline.py        ← Controllo pipeline + log live
```

## Setup

```bash
# 1. Installa dipendenze aggiuntive (oltre a requirements.txt)
pip install streamlit plotly

# 2. Avvia i database
docker compose up -d

# 3. Avvia la dashboard
streamlit run Home.py
```

La dashboard si apre su http://localhost:8501

## Pagine

| Pagina | Contenuto |
|--------|-----------|
| 🏠 Home | KPI sistema, volume 7gg, stato pipeline |
| 📦 MongoDB | Explorer documenti, JSON viewer, aggregazioni custom |
| 🕸️ Neo4j | Statistiche grafo, query Cypher, rete co-menzioni |
| ⚖️ Confronto | Stessa domanda → MongoDB vs Neo4j (per tesina) |
| 🏢 Aziende | OHLCV, sentiment, topic, eventi per singola azienda |
| ⚙️ Pipeline | Avvio script, log live, throughput |
