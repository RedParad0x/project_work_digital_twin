# Review “da professore”

## Valutazione

Il progetto è sopra la media per una tesina perché non mostra solo grafici, ma un flusso completo end-to-end:

- ingestion
- data lake
- graph model
- NLP
- orchestrazione
- dashboard

## Cosa funziona molto bene

- Scelta motivata di due database.
- Dimostrazione concreta con la pagina di confronto.
- Ingestion multi-source.
- Buona separazione per sorgenti.
- Uso del grafo per co-menzioni e gerarchie.

## Critiche probabili

- Streamlit è valido per prototipo, meno per prodotto finale.
- Frontend e accesso DB sono troppo accoppiati.
- Date come stringhe ISO: meglio datetime nativi.
- Avvio pipeline da UI con subprocess: ok in locale, non ideale in produzione.
- Mancano test e vincoli/indici unici espliciti.

## Voto realistico

- Con dashboard attuale e buona esposizione: **27–29/30**
- Con redesign frontend + API layer + presentazione solida: **29–30/30**
