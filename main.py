from fastapi import FastAPI, HTTPException, BackgroundTasks
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
        
        # Create tables (same as before)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_fixtures (
                fixture_id TEXT PRIMARY KEY,
                sport_type TEXT NOT NULL,
                league TEXT NOT NULL,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                fixture_date TIMESTAMP NOT NULL,
                season TEXT NOT NULL,
                status TEXT DEFAULT 'completed',
                home_score INTEGER,
                away_score INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ... (other tables remain the same)
        
        conn.commit()
        conn.close()
        return {"status": "success", "message": "SQLite database tables created"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ===== ETL MODULE =====
class HistoricalETL:
    def __init__(self):
        self.data_dir = "data/historical"
        self.processed_files = set()
    
    def discover_historical_files(self) -> List[str]:
        """Discover all historical data files"""
        csv_files = glob.glob(f"{self.data_dir}/*.csv")
        json_files = glob.glob(f"{self.data_dir}/*.json")
        return csv_files + json_files
    
    def parse_season_from_filename(self, filename: str) -> str:
        """Extract season from filename"""
        # Example: "epl_2023_24.csv" -> "2023_24"
        base_name = Path(filename).stem
        if "epl" in base_name.lower():
            # Extract season pattern like 2023_24, 2022_23, etc.
            parts = base_name.split('_')
            for i, part in enumerate(parts):
                if part.isdigit() and len(part) == 4:
                    if i + 1 < len(parts) and parts[i + 1].isdigit():
                        return f"{part}_{parts[i + 1]}"
                    return part
        return "unknown_season"
    
    def load_csv_file(self, file_path: str) -> pd.DataFrame:
        """Load and standardize CSV data"""
        try:
            df = pd.read_csv(file_path)
            
            # Standardize column names
            column_mapping = {
                'Fixture ID': 'fixture_id',
                'HomeTeam': 'home_team', 
                'AwayTeam': 'away_team',
                'Date': 'fixture_date',
                'FTHG': 'home_score',  # Full Time Home Goals
                'FTAG': 'away_score',  # Full Time Away Goals
                'Season': 'season',
                'Div': 'league'
            }
            
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # Ensure required columns
            if 'fixture_id' not in df.columns:
                df['fixture_id'] = df.apply(
                    lambda row: f"{row['home_team']}_{row['away_team']}_{row.get('fixture_date', 'unknown')}", 
                    axis=1
                )
            
            if 'sport_type' not in df.columns:
                df['sport_type'] = 'soccer'
            
            if 'league' not in df.columns:
                df['league'] = 'EPL'
            
            if 'status' not in df.columns:
                df['status'] = 'completed'
            
            # Parse dates
            if 'fixture_date' in df.columns:
                df['fixture_date'] = pd.to_datetime(df['fixture_date'], errors='coerce')
            
            return df
            
        except Exception as e:
            print(f"Error loading CSV {file_path}: {e}")
            return pd.DataFrame()
    
    def transform_fixture_data(self, df: pd.DataFrame, season: str) -> List[Dict]:
        """Transform DataFrame to match raw_fixtures schema"""
        fixtures = []
        
        for _, row in df.iterrows():
            fixture = {
                'fixture_id': row.get('fixture_id', ''),
                'sport_type': row.get('sport_type', 'soccer'),
                'league': row.get('league', 'EPL'),
                'home_team': row.get('home_team', ''),
                'away_team': row.get('away_team', ''),
                'fixture_date': row.get('fixture_date'),
                'season': row.get('season', season),
                'status': row.get('status', 'completed'),
                'home_score': row.get('home_score'),
                'away_score': row.get('away_score')
            }
            
            # Validate required fields
            if (fixture['home_team'] and fixture['away_team'] and 
                fixture['fixture_date'] and pd.notna(fixture['fixture_date'])):
                fixtures.append(fixture)
        
        return fixtures
    
    def load_fixtures_to_db(self, fixtures: List[Dict]) -> Dict:
        """Load fixtures into database"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            inserted = 0
            updated = 0
            errors = 0
            
            for fixture in fixtures:
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO raw_fixtures 
                        (fixture_id, sport_type, league, home_team, away_team, fixture_date, season, status, home_score, away_score, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        fixture['fixture_id'], fixture['sport_type'], fixture['league'],
                        fixture['home_team'], fixture['away_team'], fixture['fixture_date'],
                        fixture['season'], fixture['status'], fixture['home_score'], 
                        fixture['away_score']
                    ))
                    
                    if cursor.rowcount == 1:
                        inserted += 1
                    else:
                        updated += 1
                        
                except Exception as e:
                    print(f"Error inserting fixture {fixture['fixture_id']}: {e}")
                    errors += 1
            
            conn.commit()
            conn.close()
            
            return {
                "status": "success",
                "inserted": inserted,
                "updated": updated,
                "errors": errors,
                "total_processed": len(fixtures)
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def run_etl_pipeline(self) -> Dict:
        """Run complete ETL pipeline"""
        try:
            files = self.discover_historical_files()
            total_fixtures = 0
            results = {}
            
            for file_path in files:
                season = self.parse_season_from_filename(file_path)
                print(f"Processing {file_path} for season {season}")
                
                if file_path.endswith('.csv'):
                    df = self.load_csv_file(file_path)
                else:
                    # Skip JSON for now, focus on CSV
                    continue
                
                if not df.empty:
                    fixtures = self.transform_fixture_data(df, season)
                    load_result = self.load_fixtures_to_db(fixtures)
                    
                    results[file_path] = {
                        "season": season,
                        "fixtures_found": len(fixtures),
                        "load_result": load_result
                    }
                    
                    total_fixtures += len(fixtures)
            
            return {
                "status": "success",
                "files_processed": len(files),
                "total_fixtures_processed": total_fixtures,
                "details": results
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}

# Initialize ETL processor
etl_processor = HistoricalETL()

# ===== ETL ENDPOINTS =====
@app.post("/etl/historical-fixtures")
async def run_historical_etl(background_tasks: BackgroundTasks):
    """Run historical data ETL pipeline"""
    try:
        result = etl_processor.run_etl_pipeline()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/etl/status")
async def get_etl_status():
    """Get ETL status and statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Count fixtures by season
        cursor.execute("""
            SELECT season, COUNT(*) as fixture_count, 
                   MIN(fixture_date) as earliest_date,
                   MAX(fixture_date) as latest_date
            FROM raw_fixtures 
            GROUP BY season 
            ORDER BY season DESC
        """)
        season_stats = cursor.fetchall()
        
        # Total fixtures
        cursor.execute("SELECT COUNT(*) as total_fixtures FROM raw_fixtures")
        total_fixtures = cursor.fetchone()[0]
        
        # Available files
        available_files = etl_processor.discover_historical_files()
        
        conn.close()
        
        return {
            "status": "ready",
            "total_fixtures_in_db": total_fixtures,
            "available_files": available_files,
            "season_statistics": [
                {
                    "season": row[0],
                    "fixture_count": row[1],
                    "date_range": f"{row[2]} to {row[3]}" if row[2] and row[3] else "N/A"
                }
                for row in season_stats
            ]
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

# ===== EXISTING ENDPOINTS (Keep all your working endpoints) =====
# [All your existing endpoints remain unchanged...]

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

# [Include all your other existing endpoints...]
# /data/db-health, /data/tables, /data/health, /data/seasons, /data/historical, /data/current-odds, etc.