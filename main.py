from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import requests
import os
import csv
import io
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

# ===== ENVIRONMENT VARIABLE STATUS =====
def get_env_status():
    """Check which environment variables are set"""
    env_vars = {
        'DATABASE_URL': os.getenv('DATABASE_URL'),
        'POSTGRES_URL': os.getenv('POSTGRES_URL'), 
        'ODDS_API_KEY': os.getenv('ODDS_API_KEY')
    }
    
    status = {}
    for key, value in env_vars.items():
        status[key] = {
            'set': value is not None,
            'length': len(value) if value else 0,
            'preview': value[:20] + '...' if value and len(value) > 20 else value
        }
    
    return status

# ===== DATA-02 CORE SCHEMA =====
def init_tables():
    """Initialize all DATA-02 tables in SQLite"""
    conn = get_db()
    
    # 1. raw_fixtures
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 2. raw_odds_snapshots
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
    
    # 3. engineered_features
    conn.execute('''
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
            feature_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 4. model_predictions
    conn.execute('''
        CREATE TABLE IF NOT EXISTS model_predictions (
            prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            model_version TEXT NOT NULL,
            predicted_outcome TEXT,
            confidence REAL,
            predicted_prob_home REAL,
            predicted_prob_away REAL,
            predicted_prob_draw REAL,
            prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 5. betting_ledger
    conn.execute('''
        CREATE TABLE IF NOT EXISTS betting_ledger (
            bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id TEXT,
            bet_type TEXT NOT NULL,
            selection TEXT NOT NULL,
            odds REAL NOT NULL,
            stake REAL NOT NULL,
            potential_return REAL,
            bet_status TEXT DEFAULT 'placed',
            actual_result TEXT,
            profit_loss REAL,
            bet_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Legacy table for compatibility
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

init_tables()

@app.get("/")
async def root():
    env_status = get_env_status()
    return {
        "message": "MISP Betting API - DATA-02 Complete",
        "database": "sqlite",
        "environment_variables": env_status,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health():
    env_status = get_env_status()
    return {
        "status": "healthy",
        "database": "sqlite",
        "environment_variables": env_status,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/data/init-schema")
async def init_schema():
    """Initialize DATA-02 schema - POST method only"""
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

@app.get("/config/environment")
async def get_environment():
    """Show current environment variable status"""
    env_status = get_env_status()
    return {
        "status": "success",
        "environment_variables": env_status,
        "database": "sqlite",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/etl/status")
async def etl_status():
    return {"status": "ready", "timestamp": datetime.now().isoformat()}

# =============================================================================
# FOOTBALL DATA UK ETL (No pandas required - using csv module)
# =============================================================================

class FootballDataUK:
    def __init__(self):
        self.base_url = "https://www.football-data.co.uk/mmz4281"
        self.leagues = {
            'EPL': 'E0',
            'Championship': 'E1', 
            'La_Liga': 'SP1',
            'Bundesliga': 'D1',
            'Serie_A': 'I1',
            'Ligue_1': 'F1'
        }
    
    def get_season_code(self, year):
        """Convert year to football-data.co.uk season code"""
        return f"{str(year)[2:4]}{str(year+1)[2:4]}"
    
    def download_season_data(self, league, season_year):
        """Download CSV data for a specific league and season"""
        season_code = self.get_season_code(season_year)
        league_code = self.leagues.get(league)
        
        if not league_code:
            raise ValueError(f"Unknown league: {league}. Available: {list(self.leagues.keys())}")
        
        url = f"{self.base_url}/{season_code}/{league_code}.csv"
        print(f"Downloading from: {url}")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data
            csv_data = response.text
            reader = csv.DictReader(io.StringIO(csv_data))
            rows = list(reader)
            
            if not rows:
                print(f"Warning: Empty data for {league} {season_year}")
                return None
                
            print(f"Successfully downloaded {len(rows)} rows for {league} {season_year}")
            return rows
            
        except requests.exceptions.RequestException as e:
            print(f"HTTP Error downloading {league} {season_year}: {e}")
            return None
        except Exception as e:
            print(f"Error processing {league} {season_year}: {e}")
            return None
    
    def get_available_leagues(self):
        """Return list of available leagues"""
        return list(self.leagues.keys())
    
    def test_connection(self):
        """Test connection by downloading a small sample"""
        try:
            test_data = self.download_season_data('EPL', 2023)
            if test_data is not None and len(test_data) > 0:
                return {
                    "status": "success",
                    "message": "Successfully connected to Football-Data.co.uk",
                    "sample_columns": list(test_data[0].keys())[:10],
                    "data_rows": len(test_data)
                }
            else:
                return {
                    "status": "error", 
                    "message": "Connected but no data returned"
                }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Connection failed: {str(e)}"
            }

@app.get("/etl/test-football-data")
async def test_football_data():
    """Test endpoint to verify Football-Data.co.uk connection"""
    try:
        fd = FootballDataUK()
        
        # Test connection
        connection_test = fd.test_connection()
        
        # Try to download a small sample
        sample_data = fd.download_season_data('EPL', 2023)
        
        if sample_data is not None:
            sample_info = {
                "row_count": len(sample_data),
                "columns": list(sample_data[0].keys()) if sample_data else [],
                "first_few_rows": sample_data[:3] if sample_data else []
            }
        else:
            sample_info = {"error": "No data downloaded"}
        
        return {
            "connection_test": connection_test,
            "sample_data": sample_info,
            "available_leagues": fd.get_available_leagues()
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.post("/etl/historical/download")
async def download_historical_data(league: str, season_year: int):
    """Download and load historical data for a specific league and season"""
    try:
        fd = FootballDataUK()
        
        # Download data
        raw_data = fd.download_season_data(league, season_year)
        if raw_data is None:
            return {"error": f"Failed to download data for {league} {season_year}"}
        
        # Transform and load data
        conn = get_db()
        inserted_count = 0
        
        for row in raw_data:
            try:
                # Extract and convert date
                date_str = row.get('Date', '')
                if not date_str:
                    continue
                    
                # Simple date conversion (you might need to adjust this based on the actual format)
                try:
                    # Try DD/MM/YY format
                    day, month, year = date_str.split('/')
                    if len(year) == 2:
                        year = '20' + year  # Convert YY to YYYY
                    fixture_date = f"{year}-{month}-{day}"
                except:
                    # If conversion fails, skip this row
                    continue
                
                # Create fixture_id for soccer data
                home_team = row.get('HomeTeam', '')
                away_team = row.get('AwayTeam', '')
                fixture_id = f"soccer_{league}_{fixture_date}_{home_team}_vs_{away_team}".replace(' ', '_')
                
                # Extract scores
                home_score = row.get('FTHG', '')  # Full Time Home Goals
                away_score = row.get('FTAG', '')  # Full Time Away Goals
                
                # Convert scores to integers if possible
                try:
                    home_score_int = int(home_score) if home_score and home_score.strip() else None
                    away_score_int = int(away_score) if away_score and away_score.strip() else None
                except:
                    home_score_int = None
                    away_score_int = None
                
                # Insert into raw_fixtures table
                conn.execute('''
                    INSERT OR IGNORE INTO raw_fixtures 
                    (fixture_id, sport_type, league, home_team, away_team, fixture_date, season, status, home_score, away_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    fixture_id,
                    'soccer',
                    league,
                    home_team,
                    away_team,
                    fixture_date,
                    str(season_year),
                    'FT',  # Full Time - historical games are completed
                    home_score_int,
                    away_score_int
                ))
                
                if conn.total_changes > 0:
                    inserted_count += 1
                    
            except Exception as e:
                print(f"Error inserting row: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        return {
            "message": f"Successfully loaded {inserted_count} fixtures",
            "league": league,
            "season": season_year,
            "fixtures_loaded": inserted_count,
            "total_rows_downloaded": len(raw_data)
        }
        
    except Exception as e:
        return {"error": str(e)}

@app.get("/etl/available-leagues")
async def get_available_leagues():
    """Get list of available leagues from Football-Data.co.uk"""
    try:
        fd = FootballDataUK()
        return {
            "available_leagues": fd.get_available_leagues(),
            "message": "These leagues can be downloaded using /etl/historical/download"
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)