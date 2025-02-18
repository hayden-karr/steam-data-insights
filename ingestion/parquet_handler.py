import polars as pl
import os

# Function to read existing Parquet file
def read_parquet(parquet_file_name):
    file_path = os.path.join("output_data", parquet_file_name)
    
    # Ensure output directory exists
    os.makedirs("output_data", exist_ok=True)

    # Read existing data if the file exists, otherwise return an empty DataFrame
    if os.path.exists(file_path):
        return pl.read_parquet(file_path)
    else:
        return pl.DataFrame([])

# Function to merge and save data efficiently
def save_or_update_parquet(new_data, parquet_file_name):
    file_path = os.path.join("output_data", parquet_file_name)

    # Read existing data
    existing_data = read_parquet(parquet_file_name)

    # Merge and deduplicate based on 'appid'
    if not existing_data.is_empty():
        merged_data = pl.concat([existing_data, new_data]).unique(subset="appid", keep="last")
    else:
        merged_data = new_data

    # Save merged data
    merged_data.write_parquet(file_path)

    return merged_data
