from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window


# Initialize Spark Session
spark = SparkSession.builder.appName("SteamRankGenreDevs").getOrCreate()

# Load clean data from Parquet
input_path = "output_data/cleaned_steam_game_data.parquet"
df = spark.read.parquet(input_path)


# Count games per genre per year
df_genre_count = df.groupBy("year", "genres").agg(count("appid").alias("num_games_by_genre_per_year"))

# Rank genres per year
window_spec = Window.partitionBy("year").orderBy(col("num_games_by_genre_per_year").desc())
df_genre_ranked = df_genre_count.withColumn("rank", row_number().over(window_spec))

# Count devs per genre per year
df_top_count = df.groupBy("year", "developers").agg(count("appid").alias("num_games_published"))

# Rank devs per year
window_dev = Window.orderBy(col("num_games_published").desc())
df_top_devs = df_top_count.withColumn("rank", row_number().over(window_dev))

# Save top developers and genre rankings as separate Parquet files
df_genre_ranked.write.mode("overwrite").parquet("output_data/genre_rankings.parquet")
df_top_devs.write.mode("overwrite").parquet("output_data/top_developers.parquet")

# Stop The Spark Session
spark.stop()