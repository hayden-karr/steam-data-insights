import polars as pl

# Load cleaned Parquet data
input_path = "output_data/cleaned_steam_game_data.parquet"
df = pl.read_parquet(input_path)

# Compute Q1, Q3, and IQR
Q1 = df["priceUSD"].quantile(0.25)
Q3 = df["priceUSD"].quantile(0.75)
IQR = Q3 - Q1

# Define outlier bounds
lower_bound, upper_bound = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR

# Flag outliers
df = df.with_columns(
    pl.when((pl.col("priceUSD") < lower_bound) | (pl.col("priceUSD") > upper_bound))
    .then(1)
    .otherwise(0)
    .alias("price_outlier")
)

# Show detected outliers
outliers = df.filter(pl.col("price_outlier") == 1)
print(outliers)

# Save the updated dataset with outlier flags, partitioned by release year
df.write_parquet("output_data/outliers.parquet", compression="zstd", statistics=True)
