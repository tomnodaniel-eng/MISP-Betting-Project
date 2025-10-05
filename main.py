from fastapi import FastAPI, HTTPException
import os
import pandas as pd
from data_collector import DataCollector
from datetime import datetime
import math
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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

# ===== DATABASE SETUP (SQLAlchemy) =====
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# Synchronous engine for table creation
sync_engine = create_engine(DATABASE_URL.replace("+asyncpg", ""))
Base = declarative_base()

# Define Tables
class RawFixture(Base):
    __tablename__ = 'raw_fixtures'
    fixture_id = Column(String(50), primary_key=True)
    sport_type = Column(String(20), nullable=False)
    league = Column(String(50), nullable=False)
    home_team = Column(String(100), nullable=False)
    away_team = Column(String(100), nullable=False)
    fixture_date = Column(DateTime, nullable=False)
    season = Column(String(10), nullable=False)
    status = Column(String(20), default='upcoming')
    home_score = Column(Integer)
    away_score = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class RawOddsSnapshot(Base):
    __tablename__ = 'raw_odds_snapshots'
    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(String(50))
    bookmaker = Column(String(50), nullable=False)
    market_type = Column(String(20), nullable=False)
    home_odds = Column(Float(8,3))
    away_odds = Column(Float(8,3))
    draw_odds = Column(Float(8,3))
    snapshot_timestamp = Column(DateTime, nullable=False)
    last_update = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

class EngineeredFeature(Base):
    __tablename__ = 'engineered_features'
    feature_id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(String(50))
    home_form_last_5 = Column(Float(5,3))
    away_form_last_5 = Column(Float(5,3))
    home_goals_avg = Column(Float(5,2))
    away_goals_avg = Column(Float(5,2))
    h2h_home_wins = Column(Integer)
    h2h_away_wins = Column(Integer)
    h2h_draws = Column(Integer)
    implied_prob_home = Column(Float(5,4))
    implied_prob_away = Column(Float(5,4))
    implied_prob_draw = Column(Float(5,4))
    value_indicator = Column(Float(6,4))
    feature_timestamp = Column(DateTime, default=datetime.utcnow)

class ModelPrediction(Base):
    __tablename__ = 'model_predictions'
    prediction_id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(String(50))
    model_version = Column(String(20), nullable=False)
    predicted_outcome = Column(String(10))
    confidence = Column(Float(5,4))
    predicted_prob_home = Column(Float(5,4))
    predicted_prob_away = Column(Float(5,4))
    predicted_prob_draw = Column(Float(5,4))
    recommended_stake = Column(Float(6,2))
    expected_value = Column(Float(6,4))
    prediction_timestamp = Column(DateTime, default=datetime.utcnow)

class BettingLedger(Base):
    __tablename__ = 'betting_ledger'
    bet_id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(String(50))
    prediction_id = Column(Integer)
    bet_type = Column(String(20), nullable=False)
    selection = Column(String(50), nullable=False)
    odds = Column(Float(8,3), nullable=False)
    stake = Column(Float(8,2), nullable=False)
    potential_return = Column(Float(8,2))
    bet_status = Column(String(20), default='placed')
    actual_result = Column(String(50))
    profit_loss = Column(Float(8,2))
    bet_timestamp = Column(DateTime, default=datetime.utcnow)
    settled_timestamp = Column(DateTime)

def init_database():
    """Initialize database tables"""
    try:
        Base.metadata.create_all(sync_engine)
        return {"status": "success", "message": "Database tables created"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Ensure data directory exists on startup
os.makedirs('data/historical', exist_ok=True)

# Initialize database on startup
@app.on_event("startup")
def startup_event():
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
    try:
        # Test database connection
        with sync_engine.connect() as conn:
            conn.execute("SELECT 1")
        db_connected = True
    except:
        db_connected = False
    
    return {
        "status": "healthy",
        "database_connected": db_connected,
        "environment": "production"
    }

# ===== DATABASE ENDPOINTS =====
@app.get("/data/db-health")
def db_health_check():
    """Check database connectivity and version"""
    try:
        with sync_engine.connect() as conn:
            result = conn.execute("SELECT version();")
            db_version = result.scalar()
            result = conn.execute("SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = result.scalar()
        
        return {
            "database": "connected", 
            "version": db_version,
            "tables_count": table_count
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
        with sync_engine.connect() as conn:
            result = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in result]
        
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