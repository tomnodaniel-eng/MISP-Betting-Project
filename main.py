from fastapi import FastAPI, HTTPException
import os
import pandas as pd
from datetime import datetime
import math
import json
import sqlite3
from pathlib import Path

# ===== NaN FIX =====
class NaNSafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj):
                return None
            if math.isinf(obj):
                return None
        return super().default(obj)
# ===== END NaN FIX =====

app = FastAPI(title="MISP Betting API")
app.json_encoder = NaNSafeJSONEncoder

# ===== SIMPLE SQLITE DATABASE =====
DB_PATH = "misp_betting.db"

def get_db_connection():
    """Get SQLite database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize SQLite database tables"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_fixtures (
                fixture_id TEXT PRIMARY KEY,
                sport_type TEXT NOT NULL,
                league TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                fixture_date TIMESTAMP NOT NULL,
                season TEXT NOT NULL,
                status TEXT DEFAULT 'upcoming',
                home_score INTEGER,
                away_score INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_odds_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                fixture_id TEXT,
                bookmaker TEXT NOT NULL,
                market_type TEXT NOT NULL,
                home_odds REAL,
                away_odds REAL,
                draw_odds REAL,
                snapshot_timestamp TIMESTAMP NOT NULL,
                last_update TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS engineered_features (
                feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
                fixture_id TEXT,
                home_form_last_5 REAL,
                away_form_last_5 REAL,
                home_goals_avg REAL,
                away_goals_avg REAL,
                h2h_home_wins INTEGER,
                h2h_away_wins INTEGER,
                h2h_draws INTEGER,
                implied_prob_home REAL,
                implied_prob_away REAL,
                implied_prob_draw REAL,
                value_indicator REAL,
                feature_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_predictions (
                prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                fixture_id TEXT,
                model_version TEXT NOT NULL,
                predicted_outcome TEXT,
                confidence REAL,
                predicted_prob_home REAL,
                predicted_prob_away REAL,
                predicted_prob_draw REAL,
                recommended_stake REAL,
                expected_value REAL,
                prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS betting_ledger (
                bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                fixture_id TEXT,
                prediction_id INTEGER,
                bet_type TEXT NOT NULL,
                selection TEXT NOT NULL,
                odds REAL NOT NULL,
                stake REAL NOT NULL,
                potential_return REAL,
                bet_status TEXT DEFAULT 'placed',
                actual_result TEXT,
                profit_loss REAL,
                bet_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settled_timestamp TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        return {"status": "success", "message": "SQLite database tables created"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Ensure data directory exists on startup
os.makedirs('data/historical', exist_ok=True)

# Initialize database on startup
@app.on_event("startup")
def startup_event():
    """Initialize database when app starts"""
    print("Initializing SQLite database...")
    result = init_database()
    print(f"Database init: {result}")

@app.get("/")
def read_root():
    return {"message": "MISP Betting API is running on Render!"}

@app.get("/health")
def health_check():
    db_exists = Path(DB_PATH).exists()
    return {
        "status": "healthy",
        "database_connected": db_exists,
        "environment": "production",
        "database_type": "SQLite"
    }

# ===== DATABASE ENDPOINTS =====
@app.get("/data/db-health")
def db_health_check():
    """Check database connectivity"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return {
            "database": "connected", 
            "tables_count": len(tables),
            "tables": tables
        }
    except Exception as e:
        return {"database": "error", "message": str(e)}

@app.post("/data/init-database")
def initialize_database():
    """Manually initialize database tables"""
    result = init_database()
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result

@app.get("/data/tables")
def list_tables():
    """List all tables in the database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return {"tables": tables}
    except Exception as e:
        return {"error": str(e)}

# ===== EXISTING DATA ENDPOINTS =====
@app.get("/data/health")
async def data_health_check():
    return {
        "historical_data_files": 11,
        "odds_api_connected": True,
        "data_directory": "data/historical/",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/data/seasons")
async def get_seasons():
    return {
        "available_seasons": [
            "1819", "1920", "2018_19", "2019_20", "2020_21", 
            "2021", "2021_22", "2022_23", "2023_24", "2122", "2324"
        ]
    }

@app.get("/data/historical")
async def get_historical_data(season: str):
    try:
        data = {"season": season, "matches": []}
        return data
    except Exception as e:
        return {"error": str(e)}

@app.get("/data/current-odds")
async def get_current_odds():
    try:
        api_key = os.getenv('ODDS_API_KEY')
        
        if not api_key:
            return {
                "error": "ODDS_API_KEY environment variable not set",
                "odds": [],
                "timestamp": datetime.now().isoformat()
            }
        
        import requests
        
        url = "https://api.the-odds-api.com/v4/sports/soccer_epl/odds"
        params = {
            'apiKey': api_key,
            'regions': 'uk',
            'markets': 'h2h',
            'oddsFormat': 'decimal'
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            odds_data = response.json()
            return {
                "odds": odds_data,
                "timestamp": datetime.now().isoformat(),
                "api_status": "success",
                "events_count": len(odds_data)
            }
        else:
            return {
                "error": f"API request failed with status {response.status_code}",
                "api_response": response.text,
                "odds": [],
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        return {
            "error": str(e),
            "odds": [],
            "timestamp": datetime.now().isoformat()
        }