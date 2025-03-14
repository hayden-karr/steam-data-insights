import polars as pl

# Load cleaned Parquet data
input_path = "output_data/cleaned_steam_game_data.parquet"
df = pl.read_parquet(input_path)

# Extract year from release_date (assuming "release_date" is a string in YYYY-MM-DD format)
df = df.with_columns(pl.col("release_date").str.slice(0, 4).alias("year"))

# Compute average price per genre per year
df_genre_price_trends = (
    df.groupby(["year", "genres"])
    .agg(pl.col("price").mean().alias("avg_price"))
)

# Save the result as a Parquet file
df_genre_price_trends.write_parquet("output_data/avg_price_year_genre.parquet", compression="zstd")
