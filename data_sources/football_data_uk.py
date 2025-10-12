# data_sources/football_data_uk.py
import requests
import csv
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
            time.sleep(1)  # Be respectful to the server
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data without pandas
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