# PERM Processing Tracker

Competitor (permupdate.com) jaisi website — bilkul same architecture.

## Project Structure

```
perm-tracker/
├── backend/          ← Python FastAPI (Railway par deploy hoga)
│   ├── main.py       ← Full API server
│   ├── requirements.txt
│   ├── Procfile
│   └── railway.json
└── frontend/
    └── index.html    ← Complete dashboard (koi build step nahi)
```

## Backend Deploy on Railway (FREE)

### Step 1: GitHub Repo banao
```bash
cd backend/
git init
git add .
git commit -m "PERM Tracker Backend"
git remote add origin https://github.com/YOUR_USERNAME/perm-backend.git
git push -u origin main
```

### Step 2: Railway.app par deploy karo
1. railway.app par jao → Login with GitHub
2. "New Project" → "Deploy from GitHub Repo"
3. Apna repo select karo
4. Railway auto-detect karega Python aur deploy kar dega
5. "Settings" → "Generate Domain" → tumhara URL milega:
   `https://perm-backend-production.up.railway.app`

### Step 3: Frontend mein URL daalo
1. `index.html` browser mein kholo
2. Top mein "API URL" field mein apna Railway URL daalo
3. "Connect" click karo → Live data aana shuru!

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/data/dashboard?days=365&data_type=processed` | Main dashboard data (competitor jaise) |
| `GET /api/data/stats` | Current processing statistics |
| `GET /api/estimate?submission_date=2024-08-01&employer_initial=A` | Timeline estimate |
| `GET /api/data/processing-dates?months=12` | Historical processing dates |
| `GET /api/scraper/run` | Manually trigger DOL scrape |
| `GET /api/scraper/logs` | Scraper activity logs |

## DOL Data Source

Backend har 6 ghante mein DOL ka ye page scrape karta hai:
- https://flag.dol.gov/processingtimes

Ye PUBLIC data hai — koi API key nahi chahiye!

## Frontend Only (Bina Backend)

Agar sirf frontend run karna ho without backend:
- `index.html` directly browser mein open karo
- "DEMO MODE" mein chalega with realistic fake data
- Jab backend deploy ho, API URL enter karo

## Local Development

```bash
cd backend/
pip install -r requirements.txt
python main.py
# API: http://localhost:8000
# Docs: http://localhost:8000/docs
```

## Cost

- Railway Free Tier: $5 credit/month (enough for this)
- DOL Data: Free (public)
- Total: $0/month for small traffic
