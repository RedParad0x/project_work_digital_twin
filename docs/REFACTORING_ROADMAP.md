# Refactoring roadmap

## Fase 1 — Frontend
- Next.js
- Tailwind
- componenti riusabili
- layout dark cockpit
- pagine equivalenti alla dashboard Streamlit

## Fase 2 — API adapter
- FastAPI
- endpoint overview
- endpoint MongoDB
- endpoint Neo4j
- endpoint aziende
- endpoint confronto
- endpoint pipeline

## Fase 3 — Data hardening
- `ingested_at` come datetime nativo
- indici unici per deduplica
- vincoli Neo4j
- schema validation MongoDB

## Fase 4 — Pipeline production
- sostituire subprocess con Celery/RQ/Prefect
- log streaming via WebSocket
- stato job persistente

## Fase 5 — Qualità
- test API
- error boundaries frontend
- autenticazione
- Docker compose completo
