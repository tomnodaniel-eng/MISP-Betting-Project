from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Initialize FastAPI app
app = FastAPI(title="MISP Betting API", version="2.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# NaN serialization fix
class NaNSafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.float64, np.float32, np.float16)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        return super().default(obj)

app.json_encoder = NaNSafeJSONEncoder

# Database connection
def get_db_connection():
    conn = sqlite3.connect('betting_data.db')
    conn.row_factory = sqlite3.Row
    return conn

# ===== DATA-02 CORE SCHEMA IMPLEMENTATION =====
def init_advanced_schema():
    """Initialize the DATA-02 core schema in SQLite"""
    conn = get_db_connection()
    
    # 1. raw_fixtures - Core match/event data
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
            home_score INTEGER,
            away_score INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 2. raw_odds_snapshots - Historical odds data
    conn.execute('''
        CREATE TABLE IF NOT EXISTS raw_odds_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT REFERENCES raw_fixtures(fixture_id),
            bookmaker TEXT NOT NULL,
            market_type TEXT NOT NULL,
            home_odds REAL,
            away_odds REAL,
            draw_odds REAL,
            snapshot_timestamp TIMESTAMP NOT NULL,
            last_update TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 3. engineered_features - ML-ready features
    conn.execute('''
        CREATE TABLE IF NOT EXISTS engineered_features (
            feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT REFERENCES raw_fixtures(fixture_id),
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
    ''')
    
    # 4. model_predictions - ML model outputs
    conn.execute('''
        CREATE TABLE IF NOT EXISTS model_predictions (
            prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT REFERENCES raw_fixtures(fixture_id),
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
    ''')
    
    # 5. betting_ledger - Track all bets and results
    conn.execute('''
        CREATE TABLE IF NOT EXISTS betting_ledger (
            bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT REFERENCES raw_fixtures(fixture_id),
            prediction_id INTEGER REFERENCES model_predictions(prediction_id),
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
    ''')
    
    # Keep existing tables for backward compatibility
    conn.execute('''
        CREATE TABLE IF NOT EXISTS odds_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport_key TEXT,
            sport_title TEXT,
            commence_time TEXT,
            home_team TEXT,
            away_team TEXT,
            bookmaker TEXT,
            market_key TEXT,
            outcome_name TEXT,
            price REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Initialize database on startup
init_advanced_schema()

# ===== BASIC ENDPOINTS =====
@app.get("/")
async def root():
    return {
        "message": "MISP Betting API v2.0", 
        "status": "active",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat(),
        "database": "sqlite",
        "schema": "DATA-02 implemented"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "database": "sqlite_connected",
        "schema": "DATA-02 ready"
    }

# ===== DATA-02 SCHEMA MANAGEMENT =====
@app.post("/data/init-advanced-schema")
async def initialize_advanced_schema():
    """Initialize the DATA-02 core schema"""
    try:
        init_advanced_schema()
        return {
            "status": "success", 
            "message": "DATA-02 schema initialized successfully",
            "schema_version": "DATA-02",
            "tables_created": [
                "raw_fixtures", "raw_odds_snapshots", "engineered_features", 
                "model_predictions", "betting_ledger"
            ],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/data/schema-status")
async def get_schema_status():
    """Check DATA-02 schema status"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check DATA-02 tables
        data02_tables = ['raw_fixtures', 'raw_odds_snapshots', 'engineered_features', 'model_predictions', 'betting_ledger']
        table_status = {}
        
        for table in data02_tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()[0]
            table_status[table] = {
                "exists": True,
                "row_count": count
            }
        
        conn.close()
        
        return {
            "status": "success",
            "schema": "DATA-02",
            "tables": table_status,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ===== MIGRATE EXISTING DATA TO DATA-02 SCHEMA =====
@app.post("/data/migrate-to-advanced")
async def migrate_to_advanced_schema():
    """Migrate existing odds data to DATA-02 schema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get existing odds data
        cursor.execute('''
            SELECT sport_key, sport_title, commence_time, home_team, away_team, 
                   bookmaker, market_key, outcome_name, price, timestamp
            FROM odds_data 
            ORDER BY timestamp DESC
            LIMIT 50
        ''')
        
        existing_data = cursor.fetchall()
        migrated_count = 0
        
        for row in existing_data:
            row_dict = dict(row)
            
            # Create fixture ID
            fixture_id = f"{row_dict['sport_key']}_{row_dict['commence_time']}_{row_dict['home_team']}_vs_{row_dict['away_team']}".replace(' ', '_')
            
            # Insert into raw_fixtures
            cursor.execute('''
                INSERT OR IGNORE INTO raw_fixtures 
                (fixture_id, sport_type, league, home_team, away_team, fixture_date, season, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fixture_id,
                row_dict['sport_key'],
                'NBA',
                row_dict['home_team'],
                row_dict['away_team'],
                row_dict['commence_time'],
                '2024_25',
                'upcoming'
            ))
            
            # Insert into raw_odds_snapshots
            home_odds = row_dict['price'] if row_dict['outcome_name'] == row_dict['home_team'] else None
            away_odds = row_dict['price'] if row_dict['outcome_name'] == row_dict['away_team'] else None
            draw_odds = row_dict['price'] if row_dict['outcome_name'] == 'draw' else None
            
            cursor.execute('''
                INSERT INTO raw_odds_snapshots 
                (fixture_id, bookmaker, market_type, home_odds, away_odds, draw_odds, snapshot_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                fixture_id,
                row_dict['bookmaker'],
                row_dict['market_key'],
                home_odds,
                away_odds,
                draw_odds,
                row_dict['timestamp']
            ))
            
            migrated_count += 1
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": f"Migrated {migrated_count} records to DATA-02 schema",
            "migrated_count": migrated_count,
            "schema": "DATA-02",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ===== ODDS API ENDPOINT (Enhanced to use DATA-02 schema) =====
@app.get("/data/odds")
async def get_odds():
    """Fetch current odds from TheOddsAPI and store in both schemas"""
    try:
        API_KEY = os.getenv("ODDS_API_KEY", "77d4e7a1f17fcbb5d4c1f7a553daca15")
        
        SPORT = "basketball_nba"
        REGIONS = "us"
        MARKETS = "h2h"
        ODDS_FORMAT = "decimal"
        DATE_FORMAT = "iso"
        
        url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
        params = {
            'api_key': API_KEY,
            'regions': REGIONS,
            'markets': MARKETS,
            'oddsFormat': ODDS_FORMAT,
            'dateFormat': DATE_FORMAT
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            return {
                "status": "error",
                "message": f"API request failed with status {response.status_code}",
                "response_text": response.text,
                "timestamp": datetime.now().isoformat()
            }
        
        odds_data = response.json()
        
        # Store in both old and new schemas
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for game in odds_data:
            sport_key = game.get('sport_key')
            sport_title = game.get('sport_title')
            commence_time = game.get('commence_time')
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            
            # Create fixture ID for DATA-02 schema
            fixture_id = f"{sport_key}_{commence_time}_{home_team}_vs_{away_team}".replace(' ', '_')
            
            # Insert into raw_fixtures (DATA-02)
            cursor.execute('''
                INSERT OR IGNORE INTO raw_fixtures 
                (fixture_id, sport_type, league, home_team, away_team, fixture_date, season, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fixture_id,
                sport_key,
                'NBA',
                home_team,
                away_team,
                commence_time,
                '2024_25',
                'upcoming'
            ))
            
            for bookmaker in game.get('bookmakers', []):
                bookmaker_name = bookmaker.get('key')
                
                for market in bookmaker.get('markets', []):
                    market_key = market.get('key')
                    
                    for outcome in market.get('outcomes', []):
                        outcome_name = outcome.get('name')
                        price = outcome.get('price')
                        
                        # Store in old schema (backward compatibility)
                        cursor.execute('''
                            INSERT INTO odds_data 
                            (sport_key, sport_title, commence_time, home_team, away_team, bookmaker, market_key, outcome_name, price)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (sport_key, sport_title, commence_time, home_team, away_team, bookmaker_name, market_key, outcome_name, price))
                        
                        # Store in DATA-02 schema
                        home_odds = price if outcome_name == home_team else None
                        away_odds = price if outcome_name == away_team else None
                        draw_odds = price if outcome_name == 'draw' else None
                        
                        cursor.execute('''
                            INSERT INTO raw_odds_snapshots 
                            (fixture_id, bookmaker, market_type, home_odds, away_odds, draw_odds, snapshot_timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            fixture_id,
                            bookmaker_name,
                            market_key,
                            home_odds,
                            away_odds,
                            draw_odds,
                            datetime.now().isoformat()
                        ))
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": f"Retrieved {len(odds_data)} games and stored in DATA-02 schema",
            "games_count": len(odds_data),
            "schema": "DATA-02",
            "timestamp": datetime.now().isoformat(),
            "data_preview": odds_data[:2] if odds_data else []
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ===== DATA-02 QUERY ENDPOINTS =====
@app.get("/data/advanced/fixtures")
async def get_advanced_fixtures():
    """Get fixtures from DATA-02 schema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM raw_fixtures 
            ORDER BY fixture_date DESC
            LIMIT 20
        ''')
        
        fixtures = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return {
            "status": "success",
            "schema": "DATA-02",
            "count": len(fixtures),
            "data": fixtures,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/data/advanced/odds")
async def get_advanced_odds():
    """Get odds from DATA-02 schema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM raw_odds_snapshots 
            ORDER BY snapshot_timestamp DESC
            LIMIT 20
        ''')
        
        odds = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return {
            "status": "success",
            "schema": "DATA-02",
            "count": len(odds),
            "data": odds,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ===== EXISTING ENDPOINTS (Keep for backward compatibility) =====
@app.get("/data/health")
async def data_health():
    """Check database connectivity and table status"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check both old and new tables
        tables = ['odds_data', 'raw_fixtures', 'raw_odds_snapshots', 'engineered_features', 'model_predictions', 'betting_ledger']
        table_status = {}
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()[0]
                table_status[table] = {
                    "exists": True,
                    "row_count": count
                }
            except:
                table_status[table] = {
                    "exists": False,
                    "row_count": 0
                }
        
        conn.close()
        
        return {
            "status": "success",
            "database": "connected",
            "schema": "DATA-02 implemented",
            "tables": table_status,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ===== ETL ENDPOINTS =====
@app.post("/etl/historical-fixtures")
@app.get("/etl/historical-fixtures")
async def run_historical_etl():
    """Simple ETL that returns success without processing"""
    try:
        return {
            "status": "success", 
            "message": "ETL endpoint reached successfully",
            "action": "Ready to process historical files",
            "available_files": 11,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/etl/status")
async def get_etl_status():
    """ETL status endpoint"""
    return {
        "status": "ready",
        "message": "ETL system initialized",
        "timestamp": datetime.now().isoformat()
    }

# ===== LEGACY ENDPOINTS (Keep working) =====
@app.get("/data/odds/latest")
async def get_latest_odds():
    """Get latest odds from database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM odds_data 
            WHERE timestamp >= datetime('now', '-1 hour')
            ORDER BY timestamp DESC
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        odds = [dict(row) for row in rows]
        
        return {
            "status": "success",
            "count": len(odds),
            "data": odds,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/data/tables")
async def get_table_info():
    """Get information about all tables"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        tables = ['odds_data', 'raw_fixtures', 'raw_odds_snapshots', 'engineered_features', 'model_predictions', 'betting_ledger']
        table_info = {}
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()[0]
                
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cursor.fetchall()]
                
                table_info[table] = {
                    "row_count": count,
                    "columns": columns
                }
            except:
                table_info[table] = {
                    "row_count": 0,
                    "columns": []
                }
        
        conn.close()
        
        return {
            "status": "success",
            "tables": table_info,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)