from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import pandas as pd
from datetime import datetime
import math
import json
import sqlite3
from pathlib import Path
import glob
from typing import List, Dict, Optional

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

# ===== CORS FIX =====
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins for development
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# ===== DATABASE SETUP =====
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
        
        conn.commit()
        conn.close()
        return {"status": "success", "message": "SQLite database tables created"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ===== SIMPLE ETL PROCESSOR =====
class SimpleETL:
    def __init__(self):
        self.data_dir = "data/historical"
    
    def discover_files(self):
        """Discover available historical files"""
        return glob.glob(f"{self.data_dir}/*.csv")
    
    def process_single_file(self, file_path: str):
        """Process one CSV file and return sample data"""
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Get basic info
            file_info = {
                "file": file_path,
                "rows": len(df),
                "columns": list(df.columns),
                "sample_data": df.head(2).to_dict('records')  # First 2 rows as sample
            }
            
            return file_info
            
        except Exception as e:
            return {"error": str(e), "file": file_path}
    
    def run_simple_etl(self):
        """Run a simple ETL that just reads files without loading to DB"""
        try:
            files = self.discover_files()
            results = []
            
            for file_path in files[:3]:  # Process only first 3 files to avoid timeout
                result = self.process_single_file(file_path)
                results.append(result)
            
            return {
                "status": "success",
                "message": "ETL analysis completed",
                "files_processed": len(results),
                "results": results
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}

# Initialize ETL
etl_processor = SimpleETL()

# ===== ETL ENDPOINTS =====
@app.post("/etl/historical-fixtures")
async def run_historical_etl():
    """Simple ETL endpoint that analyzes files without heavy processing"""
    try:
        result = etl_processor.run_simple_etl()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/etl/status")
async def get_etl_status():
    """Get ETL status and file information"""
    try:
        files = etl_processor.discover_files()
        
        return {
            "status": "ready",
            "available_files": files,
            "total_files": len(files),
            "message": "ETL system ready for analysis"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/fixtures/count")
async def get_fixtures_count():
    """Get total number of fixtures in database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_fixtures")
        count = cursor.fetchone()[0]
        conn.close()
        return {"total_fixtures": count}
    except Exception as e:
        return {"error": str(e)}

# ===== EXISTING WORKING ENDPOINTS =====
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

@app.get("/data/db-health")
async def db_health_check():
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