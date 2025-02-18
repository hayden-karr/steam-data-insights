import json
import os
from datetime import datetime, timezone

API_LIMIT_PER_DAY = 100000  # Steam's daily API limit
CACHE_FILE = os.path.join("APIandCache","steam_api_cache.json")  # Local JSON cache
API_USAGE_FILE = os.path.join("APIandCache","api_usage.json")  # Track API calls

# Load or initialize API usage tracking
def load_api_usage():
    """Loads API usage tracking or initializes it."""
    if os.path.exists(API_USAGE_FILE):
        with open(API_USAGE_FILE, "r") as file:
            api_usage = json.load(file)
    else:
        api_usage = {"date": str(datetime.today().date()), "calls": 0}
    
    # Reset count if it's a new day
    if api_usage["date"] != str(datetime.today().date()):
        api_usage = {"date": str(datetime.today().date()), "calls": 0}
        clear_cache()

    return api_usage

# Save API usage data
def save_api_usage(api_usage):
    """Saves API usage tracking to a file."""
    with open(API_USAGE_FILE, "w") as file:
        json.dump(api_usage, file)

# Check if API limit is exceeded
def check_api_limit(api_usage):
    """Checks if the API call limit has been reached."""
    if api_usage["calls"] >= API_LIMIT_PER_DAY:
        print("Daily API limit reached. Try again tomorrow.")
        return False
    return True

# Load cache from JSON file
def load_cache():
    """Loads the API cache from a local JSON file."""

    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as file:
            return json.load(file)
    return {}

# Save cache to JSON file
def save_cache(cache):
    """Saves the API cache to a local JSON file."""
    with open(CACHE_FILE, "w") as file:
        json.dump(cache, file)

def clear_cache():
    """Deletes the cached file"""
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
        print("Cache cleared successfully.")
    else:
        print("No cache file found to clear.")

def reset_api_usage_if_new_day():
    """Ensures API usage is reset at the start of a new day."""
    api_usage = load_api_usage()
    today_str = str(datetime.today().date())

    if api_usage["date"] != today_str:
        api_usage = {"date": today_str, "calls": 0}
        save_api_usage(api_usage)
        clear_cache()
    
def load_all_appids(app_list_file):
    """Load all Steam game App IDs from a JSON file."""
    if os.path.exists(app_list_file):
        with open(app_list_file, 'r', encoding='utf-8') as file:
            app_list = json.load(file)
        return {game['appid'] for game in app_list}
    return set()

def load_processed_appids(processed_appids_file):
    """Load the list of processed App IDs, keeping progress until all are processed."""
    if os.path.exists(processed_appids_file):
        with open(processed_appids_file, 'r', encoding='utf-8') as file:
            return set(json.load(file))
    return set()

def save_processed_appids(processed_appids, processed_appids_file):
    """Save processed App IDs to a JSON file."""
    with open(processed_appids_file, 'w', encoding='utf-8') as file:
        json.dump(list(processed_appids), file)

def reset_processed_appids_if_complete(app_list_file, processed_appids_file):
    """Reset processed App IDs if all have been collected."""
    all_appids = load_all_appids(app_list_file)
    processed_appids = load_processed_appids(processed_appids_file)

    if all_appids and processed_appids >= all_appids:  # Ensure non-empty app list
        print("All Steam games have been processed. Resetting processed_appids.")
        save_processed_appids(set(), processed_appids_file)  # Reset tracking file