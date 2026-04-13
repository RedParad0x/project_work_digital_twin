from pymongo import MongoClient
import datetime

def test_connection():
    try:
        # 1. Connessione al server MongoDB su Docker
        # 'localhost' perché il container espone la porta sul tuo PC
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        
        # 2. Selezione del database e della collezione
        db = client['digital_twin_test']
        collection = db['connection_logs']

        # 3. Creazione di un documento di test
        test_data = {
            "status": "Infrastruttura Funzionante",
            "timestamp": datetime.datetime.now(),
            "message": "Il Digital Twin è online!"
        }

        # 4. Inserimento nel database
        insert_result = collection.insert_one(test_data)
        print(f"✅ Successo! Documento inserito con ID: {insert_result.inserted_id}")

        # 5. Lettura di verifica
        retrieved_data = collection.find_one({"_id": insert_result.inserted_id})
        print(f"📖 Dati letti dal DB: {retrieved_data['message']} alle {retrieved_data['timestamp']}")

    except Exception as e:
        print(f"❌ Errore di connessione: {e}")

if __name__ == "__main__":
    test_connection()