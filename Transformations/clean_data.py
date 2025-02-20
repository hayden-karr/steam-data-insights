from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DoubleType
import logging
import currency_convert


# Set up logging for better monitoring
logging.basicConfig(level=logging.INFO)

def clean_steam_data(input_path="output_data/steam_game_data.parquet", output_path="output_data/cleaned_steam_game_data.parquet"):
    # Initialize Spark Session
    spark = SparkSession.builder.appName("SteamDataCleaning") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

    try:
        # Load raw data from Parquet
        logging.info("Loading data from input path: %s", input_path)
        df = spark.read.parquet(input_path)

        # Print the schema to check input types (for debugging purposes)
        df.printSchema()

        # Clean column names (remove spaces, convert to lowercase)
        logging.info("Renaming columns for consistency")
        df = df.withColumnRenamed("release_date", "releaseDate")

        # Handle NULL for STRING columns, e.g., 'name'
        logging.info("Filling NULL values in 'name' with 'Unknown'")
        df = df.fillna({"name": "Unknown"})

        # Ensure 'priceUSD' is cast to DoubleType, only for valid price formats
        logging.info("Validating and casting 'price' to DoubleType, keeping NULLs for invalid prices")

        # Keep 'price' as a string (original) and create 'priceUSD' as a separate column for numeric values
        df = df.withColumn("priceUSD", 
            F.when(F.col("price").rlike("^\d+(\.\d{1,2})?$"), F.col("price").cast(DoubleType()))
            .otherwise(None)  # Keeps invalid values as NULL
        )

        # Keep NULL values as NULL (only convert valid price strings to DoubleType)
        df = df.withColumn("price", F.when(F.col("price").isNull(), F.lit(None)).otherwise(F.col("price")))

        # Ensure both 'price' and 'priceUSD' are available in the conversion function
        logging.info("Applying currency conversion to 'priceUSD'")
        df = currency_convert.parse_and_convert_price(df)
        
        # Handle missing genres, developers, and publishers (make them arrays if NULL)
        logging.info("Handling missing 'genres', 'developers', and 'publishers' values")
        df = df.withColumn("genres", F.when(F.col("genres").isNull(), F.array(F.lit("Unknown"))).otherwise(F.col("genres"))) \
               .withColumn("developers", F.when(F.col("developers").isNull(), F.array(F.lit("Unknown"))).otherwise(F.col("developers"))) \
               .withColumn("publishers", F.when(F.col("publishers").isNull(), F.array(F.lit("Unknown"))).otherwise(F.col("publishers")))

        # Trim spaces inside ARRAY columns (e.g., genres, developers)
        logging.info("Trimming spaces inside 'genres', 'developers', and 'publishers' arrays")
        df = df.withColumn("genres", F.expr("transform(genres, x -> trim(x))")) \
               .withColumn("developers", F.expr("transform(developers, x -> trim(x))")) \
               .withColumn("publishers", F.expr("transform(publishers, x -> trim(x))"))

         # Try to extract the year from releaseDate
        df = df.withColumn("release_year",
                   F.when(F.col("releaseDate").rlike("^\d{1,2}/\d{1,2}/\d{4}$"), 
                          F.coalesce(
                              F.year(F.to_date("releaseDate", "M/d/yyyy")),
                              F.year(F.to_date("releaseDate", "d/M/yyyy"))
                          ))  # Matches: "12/1/2023" or "1/12/2023"
                    .when(F.col("releaseDate").rlike("^\w{3} \d{1,2}, \d{4}$"), F.year(F.to_date("releaseDate", "MMM d, yyyy")))  # Matches: "Dec 1, 2023"
                    .when(F.col("releaseDate").rlike("^\d{1,2} \w{3}, \d{4}$"), F.year(F.to_date("releaseDate", "d MMM, yyyy")))  # Matches: 12 Jan, 2023
                    .when(F.col("releaseDate").rlike("Q[1-4] \d{4}"), F.regexp_extract(F.col("releaseDate"), r"(\d{4})", 1))  # Extract year from "Q1 2025"
                    .when(F.col("releaseDate").rlike("^\d{4}$"), F.col("releaseDate")) # Matches: 2025
                    .when(F.col("releaseDate").rlike("Coming Soon|Not Announced"), F.lit("Unknown"))  # Matches "Coming Soon" or "Not Announced"
                    .otherwise(F.lit("Unknown")))  # Default to "Unknown" if not recognized

        # Standardize price formatting (Free vs. Paid)
        logging.info("Creating 'priceCategory' column to categorize free and paid games")
        df = df.withColumn("priceCategory", F.when(F.col("priceUSD") == 0, "Free").otherwise("Paid"))

        # Remove duplicate game entries based on 'appid'
        logging.info("Removing duplicate entries based on 'appid'")
        df = df.dropDuplicates(["appid"])

        # Check for rows where 'appid' is NULL, which shouldn't be the case
        null_appid_count = df.filter(F.col("appid").isNull()).count()
        if null_appid_count > 0:
            logging.warning("There are %d rows with NULL 'appid'. These will be removed.", null_appid_count)
            df = df.filter(F.col("appid").isNotNull())

        # Ensure correct column type for 'appid' (casting to IntegerType explicitly)
        df = df.withColumn("appid", F.col("appid").cast(IntegerType()))

        # Write the cleaned data to Parquet, partitioned by 'release_year'
        logging.info("Writing cleaned data to output path: %s", output_path)
        df.write.mode("overwrite").partitionBy("release_year").parquet(output_path)

        logging.info("Data cleaning and writing completed successfully!")

    except Exception as e:
        logging.error("An error occurred during the cleaning process: %s", str(e))

    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    clean_steam_data()
