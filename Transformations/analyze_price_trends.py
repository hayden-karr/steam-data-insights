from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col


# Initialize Spark Session
spark = SparkSession.builder.appName("SteamAvgPrices").getOrCreate()

# Load clean data from Parquet
input_path = "output_data/cleaned_steam_game_data.parquet"
df = spark.read.parquet(input_path)

# Extract year from release_date
df = df.withColumn("year", col("release_date").substr(1, 4))

# Compute average price per genre per year
df_genre_price_trends = df.groupBy("year", "genres").agg(avg("price").alias("avg_price"))

df.write.mode("overwrite").partitionBy("release_year").parquet("output_data/avg_price_year_genre.parquet")

# Stop The Spark Session
spark.stop()