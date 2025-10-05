from fastapi import FastAPI, HTTPException
import os
import pandas as pd
from data_collector import DataCollector
from datetime import datetime
import math
import json
import psycopg2
from psycopg2.extras import RealDictCursor

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
app.json_encoder = NaNSafeJSONEncoder  # Apply the NaN fix

# ===== DATABASE CONNECTION =====
def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(
            os.getenv('DATABASE_URL'),
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# ===== DATABASE INITIALIZATION =====
def init_database():
    """Initialize database tables"""
    conn = get_db_connection()
    if not conn:
        return {"status": "error", "message": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Create tables
        tables_sql = """
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
        return {"status": "success", "message": "Database tables created"}
        
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

# Ensure data directory exists on startup
os.makedirs('data/historical', exist_ok=True)

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database when app starts"""
    print("Initializing database...")
    result = init_database()
    print(f"Database init: {result}")

@app.get("/")
def read_root():
    return {"message": "MISP Betting API is running on Render!"}

@app.get("/health")
def health_check():
    db_url = os.getenv('DATABASE_URL')
    conn = get_db_connection()
    db_connected = conn is not None
    if conn:
        conn.close()
    
    return {
        "status": "healthy",
        "database_connected": db_connected,
        "environment": "production"
    }

# ===== DATABASE ENDPOINTS =====
@app.get("/data/db-health")
async def db_health_check():
    """Check database connectivity and version"""
    try:
        conn = get_db_connection()
        if not conn:
            return {"database": "error", "message": "Connection failed"}
        
        cur = conn.cursor()
        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        cur.execute("SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'public';")
        table_count = cur.fetchone()
        conn.close()
        
        return {
            "database": "connected", 
            "version": db_version['version'],
            "tables_count": table_count['table_count']
        }
    except Exception as e:
        return {"database": "error", "message": str(e)}

@app.post("/data/init-database")
async def initialize_database():
    """Manually initialize database tables"""
    result = init_database()
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result

@app.get("/data/tables")
async def list_tables():
    """List all tables in the database"""
    try:
        conn = get_db_connection()
        if not conn:
            return {"error": "Database connection failed"}
        
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        conn.close()
        
        return {"tables": [table['table_name'] for table in tables]}
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

# Add any other routes you have below...