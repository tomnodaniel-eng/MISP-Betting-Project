from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import requests
import os
from datetime import datetime

app = FastAPI(title="MISP Betting API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    conn = sqlite3.connect('betting_data.db')
    conn.row_factory = sqlite3.Row
    return conn

# Initialize DATA-02 tables
def init_tables():
    conn = get_db()
    
    # Core DATA-02 tables
    conn.execute('''
        CREATE TABLE IF NOT EXISTS raw_fixtures (
            fixture_id TEXT PRIMARY KEY,
            sport_type TEXT NOT NULL,
            league TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            fixture_date TIMESTAMP NOT NULL,
            season TEXT NOT NULL,
            status TEXT DEFAULT 'upcoming',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS raw_odds_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            bookmaker TEXT NOT NULL,
            market_type TEXT NOT NULL,
            home_odds REAL,
            away_odds REAL,
            draw_odds REAL,
            snapshot_timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS engineered_features (
            feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            home_form_last_5 REAL,
            away_form_last_5 REAL,
            feature_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS model_predictions (
            prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            model_version TEXT NOT NULL,
            predicted_outcome TEXT,
            confidence REAL,
            prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS betting_ledger (
            bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            bet_type TEXT NOT NULL,
            selection TEXT NOT NULL,
            odds REAL NOT NULL,
            stake REAL NOT NULL,
            bet_status TEXT DEFAULT 'placed',
            bet_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Legacy table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS odds_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport_key TEXT,
            home_team TEXT,
            away_team TEXT,
            bookmaker TEXT,
            outcome_name TEXT,
            price REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

init_tables()

@app.get("/")
async def root():
    return {
        "message": "MISP Betting API - DATA-02 Ready",
        "database": "sqlite",
        "environment": "clean",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "database": "sqlite",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/data/init-schema")
async def init_schema():
    init_tables()
    return {
        "status": "success",
        "message": "DATA-02 schema initialized",
        "tables": ["raw_fixtures", "raw_odds_snapshots", "engineered_features", "model_predictions", "betting_ledger"],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/data/odds")
async def get_odds():
    try:
        API_KEY = os.getenv("ODDS_API_KEY", "77d4e7a1f17fcbb5d4c1f7a553daca15")
        
        response = requests.get(
            "https://api.the-odds-api.com/v4/sports/basketball_nba/odds",
            params={
                'api_key': API_KEY,
                'regions': 'us',
                'markets': 'h2h',
                'oddsFormat': 'decimal'
            }
        )
        
        if response.status_code != 200:
            return {
                "status": "error",
                "message": f"API error: {response.status_code}",
                "timestamp": datetime.now().isoformat()
            }
        
        games = response.json()
        
        # Store data
        conn = get_db()
        for game in games:
            sport_key = game.get('sport_key', '')
            home_team = game.get('home_team', '')
            away_team = game.get('away_team', '')
            commence_time = game.get('commence_time', '')
            
            fixture_id = f"{sport_key}_{commence_time}_{home_team}_vs_{away_team}".replace(' ', '_')
            
            # Store in DATA-02 tables
            conn.execute('''
                INSERT OR IGNORE INTO raw_fixtures 
                (fixture_id, sport_type, league, home_team, away_team, fixture_date, season, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (fixture_id, sport_key, 'NBA', home_team, away_team, commence_time, '2024_25', 'upcoming'))
            
            for bookmaker in game.get('bookmakers', []):
                bookmaker_name = bookmaker.get('key', '')
                for market in bookmaker.get('markets', []):
                    market_type = market.get('key', '')
                    for outcome in market.get('outcomes', []):
                        outcome_name = outcome.get('name', '')
                        price = outcome.get('price', 0)
                        
                        # Legacy table
                        conn.execute('''
                            INSERT INTO odds_data (sport_key, home_team, away_team, bookmaker, outcome_name, price)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (sport_key, home_team, away_team, bookmaker_name, outcome_name, price))
                        
                        # DATA-02 table
                        home_odds = price if outcome_name == home_team else None
                        away_odds = price if outcome_name == away_team else None
                        draw_odds = price if outcome_name == 'draw' else None
                        
                        conn.execute('''
                            INSERT INTO raw_odds_snapshots 
                            (fixture_id, bookmaker, market_type, home_odds, away_odds, draw_odds, snapshot_timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (fixture_id, bookmaker_name, market_type, home_odds, away_odds, draw_odds, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": f"Processed {len(games)} games",
            "games_count": len(games),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e), "timestamp": datetime.now().isoformat()}

@app.get("/data/schema-status")
async def schema_status():
    conn = get_db()
    cursor = conn.cursor()
    
    tables = ['raw_fixtures', 'raw_odds_snapshots', 'engineered_features', 'model_predictions', 'betting_ledger']
    status = {}
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
        count = cursor.fetchone()[0]
        status[table] = count
    
    conn.close()
    return {"status": "success", "data": status, "timestamp": datetime.now().isoformat()}

@app.get("/etl/status")
async def etl_status():
    return {"status": "ready", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)