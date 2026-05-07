# Spiegazione tecnica da esposizione orale

Il progetto realizza un Media Digital Twin: una rappresentazione digitale dinamica dell'ecosistema mediatico e finanziario intorno ad aziende quotate.

La pipeline raccoglie dati da sorgenti come news, Reddit, YouTube, Google Trends, GDELT e yfinance.

Ogni dato entra nel sistema tramite script dedicati. Il layer centrale è `ingestion_layer.py`, che scrive il documento grezzo in MongoDB e crea o aggiorna la rappresentazione semantica in Neo4j.

MongoDB svolge il ruolo di Data Lake: conserva documenti con payload diversi senza imporre uno schema rigido.

Neo4j svolge il ruolo di Knowledge Graph: rappresenta relazioni tra aziende, eventi, topic, industrie e settori.

Il layer NLP arricchisce i testi con sentiment label e score.

La dashboard mostra sei viste:
1. overview del sistema,
2. explorer MongoDB,
3. explorer Neo4j,
4. confronto diretto tra database,
5. analisi per azienda,
6. controllo pipeline.

La conclusione architetturale è che MongoDB e Neo4j non sono alternativi: sono complementari.
