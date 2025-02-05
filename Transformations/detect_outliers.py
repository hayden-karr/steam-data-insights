from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Initialize Spark Session
spark = SparkSession.builder.appName("SteamDetectOutliers").getOrCreate()

# Load clean data from Parquet
input_path = "output_data/cleaned_steam_game_data.parquet"
df = spark.read.parquet(input_path)

# Compute Q1, Q3, and IQR
price_summary = df.selectExpr(
    "percentile(price, 0.25) as Q1",
    "percentile(price, 0.75) as Q3"
).collect()[0]

Q1, Q3 = price_summary["Q1"], price_summary["Q3"]
IQR = Q3 - Q1

# Define outlier bounds
lower_bound, upper_bound = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR

# Flag outliers
df = df.withColumn("price_outlier", 
                   when((col("price") < lower_bound) | (col("price") > upper_bound), 1).otherwise(0))

df.filter("price_outlier == 1").show()

# Save the updated dataset with outlier flags, partitioned by release year
df.write.mode("overwrite").partitionBy("release_year").parquet("output_data/outliers.parquet")

# Stop The Spark Session
spark.stop()