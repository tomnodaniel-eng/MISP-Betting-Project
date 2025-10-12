# data_sources/football_data_uk.py
import pandas as pd
import requests
from datetime import datetime
import io
import time

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
        """Convert year to football-data.co.uk season code
        Example: 2022 -> 2223 (for 2022-2023 season)
        """
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
            # Add a small delay to be respectful to the server
            time.sleep(1)
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Read CSV into pandas
            df = pd.read_csv(io.StringIO(response.text))
            
            # Basic validation
            if df.empty:
                print(f"Warning: Empty DataFrame for {league} {season_year}")
                return None
                
            print(f"Successfully downloaded {len(df)} rows for {league} {season_year}")
            return df
            
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
            # Try to download current season EPL data
            test_df = self.download_season_data('EPL', 2023)
            if test_df is not None and not test_df.empty:
                return {
                    "status": "success",
                    "message": "Successfully connected to Football-Data.co.uk",
                    "sample_columns": list(test_df.columns)[:10],  # First 10 columns
                    "data_shape": test_df.shape
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

# Example usage
if __name__ == "__main__":
    # Test the class
    fd = FootballDataUK()
    
    print("Available leagues:", fd.get_available_leagues())
    
    # Test connection
    test_result = fd.test_connection()
    print("Connection test:", test_result)
    
    # Download sample data
    sample_data = fd.download_season_data('EPL', 2023)
    if sample_data is not None:
        print(f"Sample data shape: {sample_data.shape}")
        print("Columns:", list(sample_data.columns))
        print("\nFirst 3 rows:")
        print(sample_data[['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']].head(3))