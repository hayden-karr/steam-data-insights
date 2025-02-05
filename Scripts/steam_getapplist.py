from dotenv import load_dotenv
import os
import sys
import requests
import json
from database_handler import upload_to_s3, download_existing_s3_data

# Load environment variables for api key
load_dotenv(dotenv_path=os.path.join('config', '.env'))

# Get Steam API key
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
    
# Function to update Steam App List in S3
def update_steam_app_list():
    # Get new data from Steam API
    new_data = fetch_steam_game_data()
    
    if not new_data:
        print("No new data fetched. Exiting.")
        return
    
    # Download existing data from S3
    existing_data = download_existing_s3_data()
    
    # Convert existing data to a dictionary for fast lookup
    existing_app_dict = {app["appid"]: app for app in existing_data}
    
    # Merge new data (avoiding duplicates)
    for app in new_data:
        existing_app_dict[app["appid"]] = app  # Update or add new entry
    
    # Convert back to a list
    updated_data = list(existing_app_dict.values())

    # Upload updated data to S3
    upload_to_s3(updated_data)


def estimate_json_size(data):
    """Estimates the size of JSON data in memory (before saving)."""
    json_str = json.dumps(data)  # Convert to JSON string
    size_bytes = sys.getsizeof(json_str)  # Get size in bytes
    size_kb = size_bytes / 1024  # Convert to KB
    size_mb = size_kb / 1024  # Convert to MB
    return size_bytes, size_kb, size_mb


if __name__ == "__main__":
    app_list = fetch_steam_game_data()

    #upload_to_s3(app_list,file_name = "steam_app_list.json")


    with open("output_data/steam_games.json", "w") as json_file:
        json.dump(app_list, json_file, indent=4) 
    
    # if app_list:
    #     size_bytes, size_kb, size_mb = estimate_json_size(app_list)
    #     print(f'{size_mb: .4f} MB')
