import os
import requests
import pandas as pd
import json
from datetime import datetime

class DataCollector:
    def __init__(self):
        self.odds_api_key = os.getenv('THE_ODDS_API_KEY')
        self.football_data_key = os.getenv('FOOTBALL_DATA_API_KEY')
    
    def download_historical_data(self):
        """Download historical match data from football-data.co.uk"""
        seasons = {
            '2023-24': '2324/E0.csv',
            '2022-23': '2223/E0.csv', 
            '2021-22': '2122/E0.csv',
            '2020-21': '2021/E0.csv',
            '2019-20': '1920/E0.csv',
            '2018-19': '1819/E0.csv'
        }
        
        base_url = "https://www.football-data.co.uk/mmz4281/"
        
        for season, path in seasons.items():
            url = f"{base_url}{path}"
            filename = f"data/historical/premier_league_{season.replace('-', '_')}.csv"
            
            try:
                df = pd.read_csv(url)
                df.to_csv(filename, index=False)
                print(f"✅ Downloaded {season} data: {len(df)} matches")
            except Exception as e:
                print(f"❌ Failed to download {season}: {e}")
    
    def get_current_odds(self, sport='soccer_epl'):
        """Get current odds from TheOddsAPI"""
        if not self.odds_api_key:
            return {"error": "No API key configured"}
            
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds"
        params = {
            'apiKey': self.odds_api_key,
            'regions': 'uk',
            'markets': 'h2h',
            'oddsFormat': 'decimal'
        }
        
        try:
            response = requests.get(url, params=params)
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def test_apis(self):
        """Test all configured data sources"""
        print("🧪 Testing data sources...")
        
        # Test historical data
        self.download_historical_data()
        
        # Test odds API
        if self.odds_api_key:
            odds = self.get_current_odds()
            print(f"📊 Odds API response: {len(odds) if isinstance(odds, list) else 'Error'}")
        
        print("✅ Data source testing complete")

if __name__ == "__main__":
    collector = DataCollector()
    collector.test_apis()