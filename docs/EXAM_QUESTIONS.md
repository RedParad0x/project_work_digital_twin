# Domande d'esame probabili

## Perché MongoDB?
Perché le sorgenti producono documenti eterogenei. MongoDB permette schema flessibile e aggregazioni rapide su grandi quantità di record.

## Perché Neo4j?
Perché le domande relazionali come co-menzioni, centralità, topic collegati e gerarchie sono naturali in un grafo.

## Perché non usare solo MongoDB?
Si potrebbe, ma traversal e relazioni multi-hop diventerebbero più complessi e meno leggibili.

## Perché non usare solo Neo4j?
Neo4j non è ideale come archivio grezzo di payload eterogenei e voluminosi.

## Cos'è il dual-write?
È la scrittura coordinata dello stesso evento logico in MongoDB e Neo4j, usando un UUID comune per tracciabilità.

## Come funziona l'NLP layer?
Legge documenti non ancora processati, estrae testo, applica modelli di sentiment e aggiorna MongoDB/Neo4j.

## Cosa miglioreresti?
API layer, job queue, WebSocket, indici unici, test, date native e autenticazione.
