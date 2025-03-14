import polars as pl

# Load cleaned Parquet data
input_path = "output_data/cleaned_steam_game_data.parquet"
df = pl.read_parquet(input_path)

# Extract year from release_date (assuming "release_date" is in YYYY-MM-DD format)
df = df.with_columns(pl.col("release_date").str.slice(0, 4).alias("year"))

# Count games per genre per year
df_genre_count = (
    df.groupby(["year", "genres"])
    .agg(pl.col("appid").count().alias("num_games_by_genre_per_year"))
)

# Rank genres per year
df_genre_ranked = (
    df_genre_count.with_columns(
        pl.col("num_games_by_genre_per_year")
        .rank(descending=True, method="ordinal")
        .over("year")
        .alias("rank")
    )
)

# Count games published per developer per year
df_dev_count = (
    df.groupby(["year", "developers"])
    .agg(pl.col("appid").count().alias("num_games_published"))
)

# Rank developers per year
df_top_devs = (
    df_dev_count.with_columns(
        pl.col("num_games_published")
        .rank(descending=True, method="ordinal")
        .alias("rank")
    )
)

# Save results as Parquet files with ZSTD compression for efficiency
df_genre_ranked.write_parquet("output_data/genre_rankings.parquet", compression="zstd")
df_top_devs.write_parquet("output_data/top_developers.parquet", compression="zstd")
