import polars as pl
import logging
from currency_convert_polars import parse_and_convert_price

# Set up logging for better monitoring
logging.basicConfig(level=logging.INFO)

def clean_steam_data(input_path="output_data/steam_game_data.parquet", output_path="output_data/cleaned_steam_game_data.parquet"):
    try:
        # Load raw data from Parquet
        logging.info("Loading data from input path: %s", input_path)
        df = pl.read_parquet(input_path)

        # Print the schema to check input types (for debugging purposes)
        logging.info("Schema of loaded data:")
        print(df.schema)

        # Clean column names (remove spaces, convert to lowercase)
        logging.info("Renaming columns for consistency")
        df = df.rename({"release_date": "releaseDate"})

        # Handle NULL for STRING columns, e.g., 'name'
        logging.info("Filling NULL values in 'name' with 'Unknown'")
        df = df.with_columns(
            pl.col("name").fill_none("Unknown")
        )

        # Ensure 'priceUSD' is cast to Float64 (Polars' equivalent of DoubleType), only for valid price formats
        logging.info("Validating and casting 'price' to Float64, keeping NULLs for invalid prices")
        df = df.with_columns(
            pl.when(pl.col("price").str.contains(r"^\d+(\.\d{1,2})?$"))
            .then(pl.col("price").cast(pl.Float64))
            .otherwise(pl.lit(None))
            .alias("priceUSD")
        )

        # Keep NULL values as NULL (only convert valid price strings to Float64)
        df = df.with_columns(
            pl.when(pl.col("price").is_null()).then(pl.lit(None)).otherwise(pl.col("price")).alias("price")
        )

        # Ensure both 'price' and 'priceUSD' are available in the conversion function
        logging.info("Applying currency conversion to 'priceUSD'")
        df = parse_and_convert_price(df)

        # Handle missing genres, developers, and publishers (make them arrays if NULL)
        logging.info("Handling missing 'genres', 'developers', and 'publishers' values")
        df = df.with_columns(
            pl.when(pl.col("genres").is_null()).then(pl.list([pl.lit("Unknown")])).otherwise(pl.col("genres")).alias("genres"),
            pl.when(pl.col("developers").is_null()).then(pl.list([pl.lit("Unknown")])).otherwise(pl.col("developers")).alias("developers"),
            pl.when(pl.col("publishers").is_null()).then(pl.list([pl.lit("Unknown")])).otherwise(pl.col("publishers")).alias("publishers")
        )

        # Trim spaces inside LIST columns (e.g., genres, developers)
        logging.info("Trimming spaces inside 'genres', 'developers', and 'publishers' arrays")
        df = df.with_columns(
            pl.col("genres").apply(lambda x: [s.strip() for s in x], return_dtype=pl.List(pl.Utf8())).alias("genres"),
            pl.col("developers").apply(lambda x: [s.strip() for s in x], return_dtype=pl.List(pl.Utf8())).alias("developers"),
            pl.col("publishers").apply(lambda x: [s.strip() for s in x], return_dtype=pl.List(pl.Utf8())).alias("publishers")
        )

        # Try to extract the year from releaseDate
        logging.info("Extracting year from releaseDate")
        df = df.with_columns(
            pl.when(pl.col("releaseDate").str.contains(r"^\d{1,2}/\d{1,2}/\d{4}$"))
            .then(pl.col("releaseDate").str.strptime(pl.Date, fmt="%m/%d/%Y").dt.year())
            .when(pl.col("releaseDate").str.contains(r"^\w{3} \d{1,2}, \d{4}$"))
            .then(pl.col("releaseDate").str.strptime(pl.Date, fmt="%b %d, %Y").dt.year())
            .when(pl.col("releaseDate").str.contains(r"^\d{1,2} \w{3}, \d{4}$"))
            .then(pl.col("releaseDate").str.strptime(pl.Date, fmt="%d %b, %Y").dt.year())
            .when(pl.col("releaseDate").str.contains(r"Q[1-4] \d{4}"))
            .then(pl.col("releaseDate").str.extract(r"(\d{4})").cast(pl.Int64()))
            .otherwise(pl.lit(None)).alias("release_year")
        )

        # Standardize price formatting (Free vs. Paid)
        logging.info("Creating 'priceCategory' column to categorize free and paid games")
        df = df.with_columns(
            pl.when(pl.col("priceUSD") == 0).then(pl.lit("Free")).otherwise(pl.lit("Paid")).alias("priceCategory")
        )

        # Remove duplicate game entries based on 'appid'
        logging.info("Removing duplicate entries based on 'appid'")
        df = df.unique(subset=["appid"])

        # Check for rows where 'appid' is NULL, which shouldn't be the case
        null_appid_count = df.filter(pl.col("appid").is_null()).height
        if null_appid_count > 0:
            logging.warning("There are %d rows with NULL 'appid'. These will be removed.", null_appid_count)
            df = df.filter(pl.col("appid").is_not_null())

        # Ensure correct column type for 'appid' (casting to IntegerType explicitly)
        df = df.with_columns(
            pl.col("appid").cast(pl.Int32)
        )

        # Write the cleaned data to Parquet, partitioned by 'release_year'
        logging.info("Writing cleaned data to output path: %s", output_path)
        df.write_parquet(output_path, row_group_size=1000000)  # Optional: specify row group size for optimization

        logging.info("Data cleaning and writing completed successfully!")

    except Exception as e:
        logging.error("An error occurred during the cleaning process: %s", str(e))

if __name__ == "__main__":
    clean_steam_data()

