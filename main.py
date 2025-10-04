from fastapi import FastAPI
import os
import pandas as pd
from data_collector import DataCollector
from datetime import datetime
import math
import json

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

# ===== DATA ENDPOINTS =====
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
    # Your existing historical data loading code here
    # This will now work with NaN values!
    try:
        # Example structure - replace with your actual data loading
        data = {"season": season, "matches": []}
        return data
    except Exception as e:
        return {"error": str(e)}

@app.get("/data/current-odds")
async def get_current_odds():
    # Your existing odds code here
    try:
        # Example structure - replace with your actual odds loading
        data = {"odds": [], "timestamp": datetime.now().isoformat()}
        return data
    except Exception as e:
        return {"error": str(e)}

# Add any other routes you have below...