# Project Work: Digital Twin - Analisi dei Trend Globali

## Descrizione del Progetto
Questo progetto consiste nella realizzazione di un Digital Twin finalizzato al monitoraggio e all'analisi dei trend di ricerca e delle dinamiche digitali su scala globale. Il sistema è progettato per raccogliere dati in tempo reale da diverse sorgenti web, processarli attraverso pipeline specifiche e memorizzarli in un ambiente strutturato per consentire analisi predittive e visualizzazioni accurate della loro evoluzione temporale.

# Architettura e Tecnologie
L'infrastruttura si basa su un ecosistema modulare che garantisce scalabilità e isolamento dei processi. Il linguaggio di riferimento è Python 3.13, scelto per la vasta disponibilità di librerie dedicate al data processing e all'integrazione API.

Data Ingestion: Utilizzo di librerie per l'interfacciamento con API esterne e tecniche di web scraping.

Data Storage: Implementazione di un database NoSQL MongoDB per la gestione di grandi volumi di dati non strutturati.

Orchestrazione: Docker e Docker Compose vengono impiegati per containerizzare il database e i servizi correlati, assicurando la portabilità dell'intero ambiente di sviluppo.

Analisi e Elaborazione: Impiego di Pandas per la manipolazione dei dati e Plotly per la generazione di output grafici interattivi.

# Organizzazione del Repository
Il repository è organizzato secondo una struttura gerarchica che separa la logica applicativa dai dati e dalle configurazioni di sistema:

scripts/: Contiene i file sorgente Python responsabili dell'acquisizione e della trasformazione dei dati.

data/: Directory dedicata allo storage locale dei dataset e dei file temporanei (esclusa dal versionamento per file di grandi dimensioni).

docker-compose.yml: File di configurazione per l'avvio automatico dei container necessari al progetto.

requirements.txt: Documento tecnico che elenca tutte le dipendenze Python necessarie per l'esecuzione del codice.

.gitignore: Configurazione per escludere file ridondanti, come l'ambiente virtuale venv, dal tracciamento di Git.

# Istruzioni per l'Installazione
Per configurare l'ambiente di lavoro locale, è necessario seguire la seguente procedura da terminale:

Clonare il repository:
git clone https://github.com/RedParad0x/project_work_digital_twin.git

Creare un ambiente virtuale dedicato:
python -m venv venv

Attivare l'ambiente virtuale:
source venv/Scripts/activate (per sistemi Windows)

Installare le librerie necessarie:
pip install -r requirements.txt

# Obiettivi di Sviluppo
La fase iniziale si concentra sulla stabilizzazione della pipeline di acquisizione dati. Successivamente, verrà implementato lo strato di persistenza su MongoDB e la logica di aggiornamento ciclico del Digital Twin per garantire che la rappresentazione virtuale sia costantemente allineata ai dati reali.