import os
import json
import time
import requests
import polars as pl
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv
from database_handler import download_existing_s3_data, upload_file_to_s3
from parquet_handler import save_to_parquet, read_parquet, update_parquet_with_new_data

# Load env variables
load_dotenv(dotenv_path=os.path.join('config','.env'))

# Get Steam API Key
api_key = os.getenv('steam_api_key')

# s3 file paths
app_list_file = 'steam_games.json' # File with game ids for api calls
parquet_file_name='steam_game_data.parquet' # Final file to upload

# Constraints
batch_size = 6 # Fetch 100 games at a time
total_max_file_size = 4 # stop at 4 gb
total_max_file_size_bytes = 2000 # Stopping at 1 Mb for the sake of saving storage 
#= total_max_file_size * (1024**3) # convert to bytes
total_uploaded_size = 0 # Track total uploaded size

# Function to fetch game details
def fetch_game_details(app_ids):

    # It appears to be a strange problem that the steam api doesnt allow passing through multiple app_ids in the form of a comma seperated list
    # so if that does ever get fixed then the following code should potentially work

    # url = f"https://store.steampowered.com/api/appdetails?appids={','.join(map(str, app_ids))}"

    # try:
    #         response = requests.get(url)
    #         response.raise_for_status()
    #         return response.json()
    
    # except Exception as e:
    #     print(f'Error fetching data: {e}')
    #     raise

    game_data = {}

    for app_id in app_ids:
        url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if data and str(app_id) in data and data[str(app_id)]["success"]:
                game_data[app_id] = data[str(app_id)]["data"]
                print('Data collected')
                
            else:
                print(f"App ID {app_id} returned no data or was unsuccessful.")

            time.sleep(1)  # Add delay to avoid rate limits (adjust as needed)
            
        except Exception as e:
            print(f"Error fetching data for App ID {app_id}: {e}")
            raise  # Stop execution if an error occurs

    return game_data


# Function to collect data in batches and store in Parquet
def collect_and_store_game_data():
    global total_uploaded_size

    #app_list = download_existing_s3_data(app_list_file)

    with open("steam_games.json", 'r', encoding='utf-8') as file:
        app_list = json.load(file)

    
    all_data = []

    # Check if the Parquet file exists and load existing data (if any)
    if os.path.exists(parquet_file_name):
        existing_df = read_parquet(parquet_file_name)  # Read existing Parquet data
    else:
        existing_df = pl.DataFrame()  # If file does not exist, start with an empty DataFrame

    for i in range(0, len(app_list), batch_size): #investigate
        batch = app_list[i:i+batch_size]
        app_ids = [str(game['appid']) for game in batch]

        print(f'Processing batch {i/batch_size + 1}: {len(app_ids)} games') 
        #print(f"App IDs being sent: {app_ids}") # debug comment


        game_data = fetch_game_details(app_ids)
        #print("Fetched game data") # debug comment

        if game_data:
            for app_id, details in game_data.items():

                #print("Checking App ID details") # debug comment

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
                else:
                    print(details)
        
        #print(f"Batch {i}: all_data contains {len(all_data)} entries") # debug comment

        current_size = len(json.dumps(all_data).encode('utf-8'))
        print(current_size)
        if current_size >= total_max_file_size_bytes:
             print("Reached storage limitation, stopping data collection")
             break
        
        time.sleep(5)
    
    # convert to polars data frame
    new_df = pl.DataFrame(all_data)


    #print(new_df) # Debug comment

    # Perform duplication checking and update Parquet file
    updated_df = update_parquet_with_new_data(new_df, existing_df, parquet_file_name)

    # For debug purposes
    #if updated_df is None or updated_df.is_empty():
    #    print("Warning: DataFrame is empty, skipping Parquet write.")
    #    return
    

    #print(updated_df) # Debug comment


    save_to_parquet(updated_df, parquet_file_name)

    # convert to Pyarrow table to write to Parquet
    #table = df.to_arrow()
    #pq.write_table(table, parquet_file_name)
    
    print(f"Saved data as {parquet_file_name} ({current_size:.2f} MB)")

    # Upload to s3
    #upload_file_to_s3(parquet_file_name)

if __name__ == "__main__":
   collect_and_store_game_data()