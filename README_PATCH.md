# PRO Pipeline Patch

Sostituisci questi file nel tuo progetto:

- `backend_api/main.py`
- `frontend/lib/api.ts`
- `frontend/app/(dashboard)/pipeline/page.tsx`

Poi riavvia backend e frontend.

## Avvio

Terminale 1:

```bash
cd /c/Users/Utente/PROGETTO
source venv/Scripts/activate
cd backend_api
uvicorn main:app --reload --port 8000
```

Terminale 2:

```bash
cd /c/Users/Utente/PROGETTO/frontend
npm run dev
```

Vai su:

```txt
http://localhost:3000/pipeline
```
