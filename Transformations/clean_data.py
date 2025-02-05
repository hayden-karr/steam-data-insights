from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, trim


# Initialize Spark Session
spark = SparkSession.builder.appName("SteamDataCleaning").getOrCreate()

# Load raw data from Parquet
input_path = "output_data/steam_game_data.parquet"
df = spark.read.parquet(input_path)


# Clean column names (remove spaces, convert to lowercase)
df = df.withColumnRenamed("release_date", "releaseDate") \
    .withColumnRenamed("price", "priceUSD")

# Handle null or missing values
df = df.fillna({
    "name": "Unknown",
    "genres": "Unknown",
    "developers": "Unknown"
})

# Convert price to float (ensure consistency)
df = df.withColumn("priceUSD", col("priceUSD").cast("float"))

# Remove any leading or trailing spaces in string columns
df = df.withColumn("name", trim(col("name"))) \
       .withColumn("genres", trim(col("genres"))) \
       .withColumn("developers", trim(col("developers"))) \
       .withColumn("publishers", trim(col("publishers")))

# Split multi-value fields into arrays
df = df.withColumn("genres", split(col("genres"), ", "))
df = df.withColumn("developers", split(col("developers"), ", "))
df = df.withColumn("publishers", split(col("publishers"), ", "))

# Partition data by release year
df = df.withColumn("release_year", col("release_date").substr(1, 4))

# Standardize price formatting (convert to free if price is 0)
df = df.withColumn("priceCategory", when(col("priceUSD") == 0, "Free").otherwise("Paid"))

# Remove duplicate game entries based on `appid`
df = df.dropDuplicates(["appid"])

# Write the cleaned data to a new Parquet file, partitioned by release year
df.write.mode("overwrite").partitionBy("release_year").parquet("output_data/cleaned_steam_game_data.parquet")

# Stop The Spark Session
spark.stop()
