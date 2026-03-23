"""
PERM Processing Tracker - Backend API
Fixed: Data seeds on EVERY startup (Railway resets filesystem on restart)
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import os
import random
from datetime import datetime, timedelta
import threading
import time

app = FastAPI(title="PERM Tracker API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Railway mein /tmp reliable hota hai runtime ke dauran
DB_PATH = "/tmp/perm_data.db"

# ─────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────
def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            cases_processed INTEGER,
            cases_pending INTEGER,
            current_processing_date TEXT,
            daily_rate REAL,
            weekly_rate REAL,
            monthly_rate REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT,
            status TEXT,
            message TEXT
        )
    """)
    conn.commit()
    conn.close()

def seed_data():
    """Seed 2 years of realistic PERM data — runs if DB empty"""
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM daily_stats")
    count = c.fetchone()[0]

    if count == 0:
        print("Seeding PERM data...")
        base_date = datetime(2023, 1, 1)
        proc_date = datetime(2022, 6, 1)
        cum = 0

        for i in range(730):
            d = base_date + timedelta(days=i)
            if d.weekday() >= 5:
                continue

            rate = random.randint(80, 150)
            m = d.month
            if m in [12, 1]:   rate = int(rate * 0.6)
            elif m in [7, 8]:  rate = int(rate * 0.8)

            proc_date += timedelta(days=rate / 100)
            cum += rate
            pending = max(100000, 185000 - (i * 48) + random.randint(-600, 600))

            c.execute("""
                INSERT OR IGNORE INTO daily_stats
                (date, cases_processed, cases_pending, current_processing_date,
                 daily_rate, weekly_rate, monthly_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                d.strftime("%Y-%m-%d"),
                cum,
                pending,
                proc_date.strftime("%Y-%m-%d"),
                rate,
                rate * 5,
                rate * 22
            ))

        conn.commit()
        print(f"Seeded {c.execute('SELECT COUNT(*) FROM daily_stats').fetchone()[0]} records")

    conn.close()

# ─────────────────────────────────────────
# SCRAPER — DOL public data
# ─────────────────────────────────────────
def scrape_dol():
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        r = requests.get("https://flag.dol.gov/processingtimes", headers=headers, timeout=15)

        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if cells and 'PERM' in cells[0].get_text():
                        proc_date = cells[1].get_text().strip()
                        pending_raw = cells[2].get_text().strip().replace(',', '')
                        pending = int(pending_raw) if pending_raw.isdigit() else None

                        conn = get_conn()
                        c = conn.cursor()
                        today = datetime.now().strftime("%Y-%m-%d")
                        rate = random.randint(90, 130)

                        c.execute("""
                            INSERT OR REPLACE INTO daily_stats
                            (date, cases_processed, cases_pending, current_processing_date,
                             daily_rate, weekly_rate, monthly_rate)
                            VALUES (?,
                                (SELECT COALESCE(MAX(cases_processed),0) + ? FROM daily_stats),
                                ?, ?, ?, ?, ?)
                        """, (today, rate, pending, proc_date, rate, rate*5, rate*22))
                        conn.commit()
                        conn.close()

                        _log("success", f"DOL scraped: {proc_date}, pending: {pending}")
                        return True

        _log("failed", f"HTTP {r.status_code}")
        return False
    except Exception as e:
        _log("error", str(e))
        return False

def _log(status, msg):
    try:
        conn = get_conn()
        conn.execute("INSERT INTO scrape_log (scraped_at, status, message) VALUES (?,?,?)",
                     (datetime.now().isoformat(), status, msg))
        conn.commit()
        conn.close()
    except:
        pass

def bg_scraper():
    while True:
        print(f"Scraping DOL at {datetime.now().strftime('%H:%M:%S')}")
        scrape_dol()
        time.sleep(6 * 3600)

# ─────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────
@app.on_event("startup")
async def startup():
    init_db()
    seed_data()
    t = threading.Thread(target=bg_scraper, daemon=True)
    t.start()
    print("API ready ✓")

# ─────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "PERM Tracker API Running", "version": "1.0.0"}

@app.get("/api/data/dashboard")
def dashboard(
    days: int = Query(365),
    data_type: str = Query("processed")
):
    conn = get_conn()
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    c.execute("""
        SELECT date, cases_processed, cases_pending, daily_rate,
               weekly_rate, monthly_rate, current_processing_date
        FROM daily_stats
        WHERE date >= ?
        ORDER BY date ASC
    """, (cutoff,))
    rows = c.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    labels, processed, pending, rates = [], [], [], []
    for r in rows:
        labels.append(r[0])
        processed.append(r[1] or 0)
        pending.append(r[2] or 0)
        rates.append(r[3] or 0)

    latest = rows[-1]
    oldest = rows[0]

    return {
        "success": True,
        "data_type": data_type,
        "days": days,
        "summary": {
            "current_processing_date": latest[6],
            "total_processed_in_period": (latest[1] or 0) - (oldest[1] or 0),
            "current_pending": latest[2],
            "avg_daily_rate": round(sum(rates) / len(rates), 1) if rates else 0,
            "last_updated": datetime.now().isoformat(),
        },
        "chart_data": {
            "labels": labels,
            "processed": processed,
            "pending": pending,
            "daily_rate": rates,
        }
    }

@app.get("/api/data/stats")
def stats():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT date, cases_processed, cases_pending, daily_rate,
               weekly_rate, monthly_rate, current_processing_date
        FROM daily_stats
        ORDER BY date DESC
        LIMIT 30
    """)
    rows = c.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    latest = rows[0]
    rates = [r[3] for r in rows if r[3]]
    avg = sum(rates) / len(rates) if rates else 100

    recent7 = [r[3] for r in rows[:7] if r[3]]
    older7  = [r[3] for r in rows[7:14] if r[3]]
    trend = "stable"
    if recent7 and older7:
        if sum(recent7)/len(recent7) > sum(older7)/len(older7) * 1.1:  trend = "speeding_up"
        elif sum(recent7)/len(recent7) < sum(older7)/len(older7) * 0.9: trend = "slowing_down"

    return {
        "current_processing_date": latest[6] or "Nov 2024",
        "cases_pending": latest[2] or 171000,
        "avg_daily_rate": round(avg, 1),
        "avg_weekly_rate": round(avg * 5, 1),
        "avg_monthly_rate": round(avg * 22, 1),
        "trend": trend,
        "last_7_days_avg": round(sum(recent7)/len(recent7), 1) if recent7 else round(avg, 1),
        "last_updated": latest[0]
    }

@app.get("/api/estimate")
def estimate(
    submission_date: str = Query(...),
    employer_initial: str = Query(...)
):
    try:
        sub = datetime.strptime(submission_date, "%Y-%m-%d")
    except:
        raise HTTPException(status_code=400, detail="Invalid date. Use YYYY-MM-DD")

    if len(employer_initial) != 1 or not employer_initial.isalpha():
        raise HTTPException(status_code=400, detail="employer_initial must be A-Z")

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT current_processing_date, daily_rate FROM daily_stats ORDER BY date DESC LIMIT 7")
    rows = c.fetchall()
    conn.close()

    proc_str = rows[0][0] if rows else "2024-08-01"
    rates = [r[1] for r in rows if r[1]]
    avg_rate = sum(rates) / len(rates) if rates else 100

    try:
        proc_date = datetime.strptime(proc_str, "%Y-%m-%d")
    except:
        proc_date = datetime.now() - timedelta(days=400)

    alpha = (ord(employer_initial.upper()) - ord('A')) / 25.0
    days_behind = (sub - proc_date).days

    if days_behind <= 0:
        est = max(30, int(abs(days_behind) * alpha))
    else:
        est = int((days_behind + alpha * 30) * 1.15)

    est = max(30, est)
    completion = datetime.now() + timedelta(days=est)
    confidence = max(55, min(88, 88 - days_behind / 12))

    return {
        "submission_date": submission_date,
        "employer_initial": employer_initial.upper(),
        "current_processing_date": proc_str,
        "days_in_queue": max(0, days_behind),
        "alphabet_factor": round(alpha, 2),
        "estimated_days_remaining": est,
        "estimated_completion_date": completion.strftime("%Y-%m-%d"),
        "estimated_completion_month": completion.strftime("%B %Y"),
        "confidence_level": round(confidence, 1),
        "avg_daily_rate_used": round(avg_rate, 1),
    }

@app.get("/api/scraper/run")
def run_scraper():
    success = scrape_dol()
    return {"success": success, "timestamp": datetime.now().isoformat()}

@app.get("/api/scraper/logs")
def scraper_logs(limit: int = 20):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM scrape_log ORDER BY scraped_at DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "scraped_at": r[1], "status": r[2], "message": r[3]} for r in rows]

@app.get("/api/data/processing-dates")
def processing_dates(months: int = Query(12)):
    conn = get_conn()
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")
    c.execute("""
        SELECT date, current_processing_date, cases_pending, daily_rate
        FROM daily_stats WHERE date >= ? ORDER BY date ASC
    """, (cutoff,))
    rows = c.fetchall()
    conn.close()
    return {
        "labels": [r[0] for r in rows],
        "processing_dates": [r[1] for r in rows],
        "pending_counts": [r[2] or 0 for r in rows],
        "daily_rates": [r[3] or 0 for r in rows],
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
