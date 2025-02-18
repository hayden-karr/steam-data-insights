import os
import json
import time
import requests
import polars as pl
from dotenv import load_dotenv
from parquet_handler import save_or_update_parquet
from api_track_cache import load_cache, load_api_usage, check_api_limit, save_api_usage, save_cache, reset_api_usage_if_new_day, reset_processed_appids_if_complete

# Load environment variables
load_dotenv(dotenv_path=os.path.join('config','.env'))

# Get Steam API Key
api_key = os.getenv('steam_api_key')

# File paths
app_list_file = os.path.join('output_data', 'steam_games.json')
parquet_file_name = 'steam_game_data.parquet'
processed_appids_file = os.path.join('APIandCache', 'processed_appids.json')

# Constraints
batch_size = 100
max_file_size_bytes = 1000  # Example limit, modify as needed
daily_api_limit = 100000

def load_processed_appids():
    """Load the list of processed App IDs from a JSON file."""
    if os.path.exists(processed_appids_file):
        with open(processed_appids_file, 'r', encoding='utf-8') as file:
            return set(json.load(file))
    return set()

def save_processed_appids(processed_appids):
    """Save the processed App IDs to a JSON file."""
    with open(processed_appids_file, 'w', encoding='utf-8') as file:
        json.dump(list(processed_appids), file)

def fetch_game_details(app_ids, retries=5):
    """Fetch game details from the Steam API."""
    api_usage = load_api_usage()
    cache = load_cache()
    game_data = {}

    for app_id in app_ids:
        if str(app_id) in cache:
            print(f"Cache hit: {app_id}")
            game_data[app_id] = cache[str(app_id)]
            continue

        if not check_api_limit(api_usage):
            return game_data

        url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"

        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if data and str(app_id) in data and data[str(app_id)]["success"]:
                    game_data[app_id] = data[str(app_id)]["data"]
                    cache[str(app_id)] = data[str(app_id)]["data"]
                    api_usage["calls"] += 1
                    break
                else:
                    print(f"App ID {app_id} returned no data.")
                    break
            except requests.exceptions.RequestException as e:
                wait_time = 2 ** attempt + 1
                print(f"Error fetching App ID {app_id}: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        time.sleep(1)
    
    save_cache(cache)
    save_api_usage(api_usage)
    return game_data

def collect_and_store_game_data():
    """Collect and store game data while ensuring previous data is not overwritten."""

    reset_api_usage_if_new_day()

    with open(app_list_file, 'r', encoding='utf-8') as file:
        app_list = json.load(file)


    all_data = []
    processed_appids = set()
    new_appids = [game['appid'] for game in app_list]

    for i in range(0, len(new_appids), batch_size):
        batch = new_appids[i:i + batch_size]
        print(f'Processing batch {i // batch_size + 1}/{len(new_appids) // batch_size + 1}')
        
        game_data = fetch_game_details(batch)

        if game_data:
            for app_id, details in game_data.items():
                if details:
                    all_data.append({
                        "appid": app_id,
                        "name": details.get("name"),
                        "release_date": details.get("release_date", {}).get("date"),
                        "price": details.get("price_overview", {}).get("final_formatted"),
                        "genres": [g["description"] for g in details.get("genres", [])],
                        "developers": details.get("developers", []),
                        "publishers": details.get("publishers", [])
                    })
                    processed_appids.add(app_id)

        # Prevent exceeding storage limits
        if len(json.dumps(all_data).encode('utf-8')) >= max_file_size_bytes:
            print("Reached storage limitation, stopping data collection")
            break

        time.sleep(5)  # Avoid hitting API rate limits

    if all_data:
        new_df = pl.DataFrame(all_data)
        save_or_update_parquet(new_df, parquet_file_name)  # Merges and saves the new data

    reset_processed_appids_if_complete("output_data/steam_data.json", "APIandCache/processed_appids.json")
    save_cache(load_cache())  # Ensure cache is updated


if __name__ == "__main__":
    collect_and_store_game_data()


