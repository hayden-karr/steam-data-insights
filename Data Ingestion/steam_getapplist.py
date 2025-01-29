from dotenv import load_dotenv
import os
import sys
import requests
import json

# Load environment variables for api key
load_dotenv()

api_key = os.getenv('steam_api_key')

# function to get steam data
def fetch_steam_game_data():
    try:
        url = f' https://api.steampowered.com/ISteamApps/GetAppList/v2/?key={api_key}&format=json'

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        app_list = data.get("applist", {}).get("apps", [])
        print(f'collected {len(app_list)} steam apps')
        return app_list

    except Exception as e:
        print(f'The following error occured when collecting the App List: {e}')
        return None

def estimate_json_size(data):
    """Estimates the size of JSON data in memory (before saving)."""
    json_str = json.dumps(data)  # Convert to JSON string
    size_bytes = sys.getsizeof(json_str)  # Get size in bytes
    size_kb = size_bytes / 1024  # Convert to KB
    size_mb = size_kb / 1024  # Convert to MB
    return size_bytes, size_kb, size_mb


if __name__ == "__main__":
    app_list = fetch_steam_game_data()

    

    # if app_list:
    #     size_bytes, size_kb, size_mb = estimate_json_size(app_list)
    #     print(f'{size_mb: .4f} MB')
