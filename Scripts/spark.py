from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SteamDataPipeline").getOrCreate()

# Load Parquet file
df = spark.read.parquet("steam_game_data.parquet")

# Show schema
df.printSchema()
df.show(5)