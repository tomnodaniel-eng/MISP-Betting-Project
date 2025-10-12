from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

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

# ===== DATABASE CONNECTIONS =====
def get_db_connection():
    """SQLite connection (existing)"""
    conn = sqlite3.connect('betting_data.db')
    conn.row_factory = sqlite3.Row
    return conn

def get_postgres_connection():
    """PostgreSQL connection - tries multiple environment variable names"""
    try:
        # Try these environment variable names in order
        env_vars = ['POSTGRES_URL', 'DATABASE_URL_NEW', 'BETTING_DB_URL', 'DATABASE_URL']
        database_url = None
        
        for env_var in env_vars:
            database_url = os.getenv(env_var)
            if database_url:
                print(f"Using database URL from {env_var}")
                break
        
        if not database_url:
            print("No PostgreSQL environment variable found")
            return None
            
        conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        print("PostgreSQL connection successful")
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None

# Initialize database tables
def init_db():
    conn = get_db_connection()
    
    # Existing tables (keep for backward compatibility)
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
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS historical_fixtures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            league TEXT,
            season TEXT,
            date TEXT,
            home_team TEXT,
            away_team TEXT,
            home_score INTEGER,
            away_score INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id TEXT,
            name TEXT,
            country TEXT,
            season TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id TEXT,
            name TEXT,
            country TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS fixtures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            league_id TEXT,
            season TEXT,
            date TEXT,
            home_team_id TEXT,
            away_team_id TEXT,
            home_score INTEGER,
            away_score INTEGER,
            status TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

# Initialize database on startup
init_db()

# ===== BASIC ENDPOINTS =====
@app.get("/")
async def root():
    return {
        "message": "MISP Betting API v2.0", 
        "status": "active",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat(),
        "databases": ["sqlite", "postgres"]
    }

@app.get("/health")
async def health_check():
    postgres_status = "connected" if get_postgres_connection() else "not_configured"
    
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "sqlite": "connected",
        "postgres": postgres_status
    }

# ===== POSTGRES DATABASE SETUP =====
@app.post("/data/init-postgres")
async def initialize_postgres_schema():
    """Initialize the core Postgres schema for DATA-02"""
    try:
        conn = get_postgres_connection()
        if not conn:
            return {
                "status": "error", 
                "message": "PostgreSQL not configured. Set POSTGRES_URL environment variable.",
                "help": "Add POSTGRES_URL to environment variables with your database connection string"
            }
        
        cur = conn.cursor()
        
        # Create the 5 core tables from DATA-02 design
        tables_sql = """
        -- 1. raw_fixtures - Core match/event data
        CREATE TABLE IF NOT EXISTS raw_fixtures (
            fixture_id VARCHAR(50) PRIMARY KEY,
            sport_type VARCHAR(20) NOT NULL,
            league VARCHAR(50) NOT NULL,
            home_team VARCHAR(100) NOT NULL,
            away_team VARCHAR(100) NOT NULL,
            fixture_date TIMESTAMP NOT NULL,
            season VARCHAR(10) NOT NULL,
            status VARCHAR(20) DEFAULT 'upcoming',
            home_score INTEGER,
            away_score INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 2. raw_odds_snapshots - Historical odds data
        CREATE TABLE IF NOT EXISTS raw_odds_snapshots (
            snapshot_id SERIAL PRIMARY KEY,
            fixture_id VARCHAR(50) REFERENCES raw_fixtures(fixture_id),
            bookmaker VARCHAR(50) NOT NULL,
            market_type VARCHAR(20) NOT NULL,
            home_odds DECIMAL(8,3),
            away_odds DECIMAL(8,3),
            draw_odds DECIMAL(8,3),
            snapshot_timestamp TIMESTAMP NOT NULL,
            last_update TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 3. engineered_features - ML-ready features
        CREATE TABLE IF NOT EXISTS engineered_features (
            feature_id SERIAL PRIMARY KEY,
            fixture_id VARCHAR(50) REFERENCES raw_fixtures(fixture_id),
            home_form_last_5 DECIMAL(5,3),
            away_form_last_5 DECIMAL(5,3),
            home_goals_avg DECIMAL(5,2),
            away_goals_avg DECIMAL(5,2),
            h2h_home_wins INTEGER,
            h2h_away_wins INTEGER,
            h2h_draws INTEGER,
            implied_prob_home DECIMAL(5,4),
            implied_prob_away DECIMAL(5,4),
            implied_prob_draw DECIMAL(5,4),
            value_indicator DECIMAL(6,4),
            feature_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 4. model_predictions - ML model outputs
        CREATE TABLE IF NOT EXISTS model_predictions (
            prediction_id SERIAL PRIMARY KEY,
            fixture_id VARCHAR(50) REFERENCES raw_fixtures(fixture_id),
            model_version VARCHAR(20) NOT NULL,
            predicted_outcome VARCHAR(10),
            confidence DECIMAL(5,4),
            predicted_prob_home DECIMAL(5,4),
            predicted_prob_away DECIMAL(5,4),
            predicted_prob_draw DECIMAL(5,4),
            recommended_stake DECIMAL(6,2),
            expected_value DECIMAL(6,4),
            prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 5. betting_ledger - Track all bets and results
        CREATE TABLE IF NOT EXISTS betting_ledger (
            bet_id SERIAL PRIMARY KEY,
            fixture_id VARCHAR(50) REFERENCES raw_fixtures(fixture_id),
            prediction_id INTEGER REFERENCES model_predictions(prediction_id),
            bet_type VARCHAR(20) NOT NULL,
            selection VARCHAR(50) NOT NULL,
            odds DECIMAL(8,3) NOT NULL,
            stake DECIMAL(8,2) NOT NULL,
            potential_return DECIMAL(8,2),
            bet_status VARCHAR(20) DEFAULT 'placed',
            actual_result VARCHAR(50),
            profit_loss DECIMAL(8,2),
            bet_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            settled_timestamp TIMESTAMP
        );
        """
        
        cur.execute(tables_sql)
        conn.commit()
        
        # Verify tables were created
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row['table_name'] for row in cur.fetchall()]
        
        conn.close()
        
        return {
            "status": "success", 
            "message": "Postgres schema initialized for DATA-02",
            "tables_created": [
                "raw_fixtures", "raw_odds_snapshots", "engineered_features", 
                "model_predictions", "betting_ledger"
            ],
            "existing_tables": tables,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/data/postgres-health")
async def postgres_health_check():
    """Check Postgres database connectivity and table status"""
    try:
        conn = get_postgres_connection()
        if not conn:
            return {
                "status": "error", 
                "message": "PostgreSQL not configured",
                "help": "Set POSTGRES_URL environment variable in Render dashboard"
            }
        
        cur = conn.cursor()
        
        # Check database version
        cur.execute("SELECT version();")
        db_version = cur.fetchone()['version']
        
        # Check table existence and row counts
        tables = ['raw_fixtures', 'raw_odds_snapshots', 'engineered_features', 'model_predictions', 'betting_ledger']
        table_status = {}
        
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cur.fetchone()['count']
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
            "database": "postgres_connected",
            "version": db_version.split(',')[0],  # Clean version string
            "tables": table_status,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ===== DATA MIGRATION ENDPOINTS =====
@app.post("/data/migrate-to-postgres")
async def migrate_odds_to_postgres():
    """Migrate existing odds data from SQLite to Postgres"""
    try:
        # Get existing odds from SQLite
        sqlite_conn = get_db_connection()
        sqlite_cursor = sqlite_conn.cursor()
        
        sqlite_cursor.execute('''
            SELECT sport_key, sport_title, commence_time, home_team, away_team, 
                   bookmaker, market_key, outcome_name, price, timestamp
            FROM odds_data 
            ORDER BY timestamp DESC
            LIMIT 100
        ''')
        
        sqlite_data = sqlite_cursor.fetchall()
        sqlite_conn.close()
        
        # Connect to Postgres
        pg_conn = get_postgres_connection()
        if not pg_conn:
            return {"status": "error", "message": "PostgreSQL not available"}
        
        pg_cursor = pg_conn.cursor()
        
        migrated_count = 0
        
        for row in sqlite_data:
            # Convert SQLite row to dict
            row_dict = dict(row)
            
            # Create fixture ID from game data
            fixture_id = f"{row_dict['sport_key']}_{row_dict['commence_time']}_{row_dict['home_team']}_vs_{row_dict['away_team']}".replace(' ', '_')
            
            # Insert into raw_fixtures if not exists
            pg_cursor.execute('''
                INSERT INTO raw_fixtures 
                (fixture_id, sport_type, league, home_team, away_team, fixture_date, season, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fixture_id) DO NOTHING
            ''', (
                fixture_id,
                row_dict['sport_key'],
                'NBA',  # Default league for basketball
                row_dict['home_team'],
                row_dict['away_team'],
                row_dict['commence_time'],
                '2024_25',  # Current season
                'upcoming'
            ))
            
            # Insert into raw_odds_snapshots
            # Determine odds based on outcome
            home_odds = row_dict['price'] if row_dict['outcome_name'] == row_dict['home_team'] else None
            away_odds = row_dict['price'] if row_dict['outcome_name'] == row_dict['away_team'] else None
            draw_odds = row_dict['price'] if row_dict['outcome_name'] == 'draw' else None
            
            pg_cursor.execute('''
                INSERT INTO raw_odds_snapshots 
                (fixture_id, bookmaker, market_type, home_odds, away_odds, draw_odds, snapshot_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
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
        
        pg_conn.commit()
        pg_conn.close()
        
        return {
            "status": "success",
            "message": f"Migrated {migrated_count} odds records to Postgres",
            "migrated_count": migrated_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ===== EXISTING ENDPOINTS (Keep all working functionality) =====
@app.get("/data/health")
async def data_health():
    """Check database connectivity and table status"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        tables = ['odds_data', 'historical_fixtures', 'leagues', 'teams', 'fixtures']
        table_status = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()[0]
            table_status[table] = {
                "exists": True,
                "row_count": count
            }
        
        conn.close()
        
        return {
            "status": "success",
            "database": "connected",
            "tables": table_status,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ===== ODDS API ENDPOINT =====
@app.get("/data/odds")
async def get_odds():
    """Fetch current odds from TheOddsAPI"""
    try:
        API_KEY = os.getenv("ODDS_API_KEY", "77d4e7a1f17fcbb5d4c1f7a553daca15")
        
        if not API_KEY or API_KEY == "your_api_key_here":
            return {
                "status": "error",
                "message": "API key not configured. Please set ODDS_API_KEY environment variable.",
                "timestamp": datetime.now().isoformat()
            }
        
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
        
        if response.status_code == 401:
            return {
                "status": "error",
                "message": "Invalid API key. Please check your ODDS_API_KEY environment variable.",
                "response_text": response.text,
                "timestamp": datetime.now().isoformat()
            }
        elif response.status_code != 200:
            return {
                "status": "error",
                "message": f"API request failed with status {response.status_code}",
                "response_text": response.text,
                "timestamp": datetime.now().isoformat()
            }
        
        odds_data = response.json()
        
        # Store in SQLite (existing functionality)
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for game in odds_data:
            sport_key = game.get('sport_key')
            sport_title = game.get('sport_title')
            commence_time = game.get('commence_time')
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            
            for bookmaker in game.get('bookmakers', []):
                bookmaker_name = bookmaker.get('key')
                
                for market in bookmaker.get('markets', []):
                    market_key = market.get('key')
                    
                    for outcome in market.get('outcomes', []):
                        outcome_name = outcome.get('name')
                        price = outcome.get('price')
                        
                        cursor.execute('''
                            INSERT INTO odds_data 
                            (sport_key, sport_title, commence_time, home_team, away_team, bookmaker, market_key, outcome_name, price)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (sport_key, sport_title, commence_time, home_team, away_team, bookmaker_name, market_key, outcome_name, price))
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": f"Retrieved {len(odds_data)} games",
            "games_count": len(odds_data),
            "timestamp": datetime.now().isoformat(),
            "data_preview": odds_data[:2] if odds_data else []
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ===== SIMPLE ETL ENDPOINTS =====
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

# ===== DATABASE QUERY ENDPOINTS =====
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
        
        tables = ['odds_data', 'historical_fixtures', 'leagues', 'teams', 'fixtures']
        table_info = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()[0]
            
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]
            
            table_info[table] = {
                "row_count": count,
                "columns": columns
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