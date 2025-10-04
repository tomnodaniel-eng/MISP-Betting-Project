from fastapi import FastAPI
import os
import pandas as pd
from data_collector import DataCollector
from datetime import datetime

app = FastAPI(title="MISP Betting API")

# Ensure data directory exists on startup
os.makedirs('data/historical', exist_ok=True)

@app.get("/")
def read_root():
    return {"message": "MISP Betting API is running on Render!"}

@app.get("/health")
def health_check():
    db_url = os.getenv('DATABASE_URL')
    return {
        "status": "healthy", 
        "database_connected": bool(db_url),
        "environment": "production"
    }

@app.get("/add/{a}/{b}")
def api_add(a: int, b: int):
    result = a + b
    return {"operation": "add", "result": result}

@app.get("/multiply/{a}/{b}")
def api_multiply(a: int, b: int):
    result = a * b
    return {"operation": "multiply", "result": result}

# DATA ENDPOINTS
@app.get("/data/test")
def data_test():
    """Test if data directory is accessible"""
    try:
        files = os.listdir('data/historical')
        return {
            "data_directory_exists": os.path.exists('data'),
            "historical_files": files,
            "file_count": len(files)
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/data/health")
def data_health():
    """Check data source connectivity"""
    collector = DataCollector()
    
    # Check historical data files
    try:
        historical_files = [f for f in os.listdir('data/historical') if f.endswith('.csv')]
        historical_count = len(historical_files)
    except:
        historical_count = 0
    
    # Test odds API
    odds_test = collector.get_current_odds()
    odds_working = not isinstance(odds_test, dict) or 'error' not in odds_test
    
    return {
        "historical_data_files": historical_count,
        "odds_api_connected": odds_working,
        "data_directory": "data/historical/",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/data/seasons")
def get_available_seasons():
    """List all available historical seasons"""
    try:
        files = [f for f in os.listdir('data/historical') if f.startswith('premier_league_')]
        seasons = [f.replace('premier_league_', '').replace('.csv', '') for f in files]
        return {"available_seasons": sorted(seasons)}
    except Exception as e:
        return {"error": str(e)}

@app.get("/data/historical")
def get_historical_data(season: str = "2023_24"):
    """Get historical match data for a specific season"""
    try:
        filename = f"data/historical/premier_league_{season}.csv"
        df = pd.read_csv(filename)
        return {
            "season": season,
            "match_count": len(df),
            "data": df.head(10).to_dict('records')  # First 10 matches
        }
    except Exception as e:
        return {"error": f"Could not load data for season {season}: {str(e)}"}

@app.get("/data/current-odds")
def get_current_odds():
    """Get current betting odds"""
    collector = DataCollector()
    odds = collector.get_current_odds()
    return odds

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)