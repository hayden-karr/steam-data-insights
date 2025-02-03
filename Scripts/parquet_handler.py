import polars as pl
import pyarrow.parquet as pq
import os

# Function to read existing parquet file
def read_parquet(parquet_file_name):
    if os.path.exists(parquet_file_name):
        return pl.read_parquet(parquet_file_name)
    else:
        return pl.DataFrame([])
    

# Function to save parquet file in chunks
def save_to_parquet(df, parquet_file_name, chunk_size=100):
    """Save the DataFrame to Parquet in chunks to avoid memory overload."""

    kwargs = dict(
    use_pyarrow=True,
    pyarrow_options=dict(partition_cols=['appid'])
    )
    
    total_rows = len(df)
    num_chunks = (total_rows // chunk_size) + 1  # Calculate the number of chunks
    
    # Loop through the DataFrame in chunks
    for i in range(num_chunks):
        # Get the chunk of rows
        start_row = i * chunk_size
        end_row = min((i + 1) * chunk_size, total_rows)
        
        chunk_df = df[start_row:end_row]  # Get the current chunk
        
        if i== 0:
            chunk_df.write_parquet(parquet_file_name)
        else:
            chunk_df.write_parquet(parquet_file_name, **kwargs)

# Function to check for duplicates and update if necessary
def update_parquet_with_new_data(new_data, existing_data, parquet_file_name, chunk_size=100):
    # Convert inputs to Polars DataFrame
    existing_df = existing_data
    new_df = pl.DataFrame(new_data)

    # Create an empty list to store updated chunks
    updated_chunks = []

    # Process data in chunks
    for existing_chunk in existing_df.iter_slices(n_rows=chunk_size):
        # Join chunk with new data
        merged_df = existing_chunk.join(new_df, on="appid", how="outer", suffix="_new")

        # Use `coalesce` to replace only changed values
        updated_chunk = merged_df.with_columns([
            pl.coalesce("name_new", "name").alias("name"),
            pl.coalesce("release_date_new", "release_date").alias("release_date"),
            pl.coalesce("price_new", "price").alias("price"),
            pl.coalesce("genres_new", "genres").alias("genres"),
            pl.coalesce("developers_new", "developers").alias("developers"),
            pl.coalesce("publishers_new", "publishers").alias("publishers"),
        ]).select(["appid", "name", "release_date", "price", "genres", "developers", "publishers"])

        # Store the processed chunk
        updated_chunks.append(updated_chunk)

    # Combine all processed chunks
    final_df = pl.concat(updated_chunks)

    # Save the updated data
    # save_to_parquet(final_df, parquet_file_name, chunk_size)

    return final_df