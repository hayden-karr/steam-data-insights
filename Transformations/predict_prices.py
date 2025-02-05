from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("SteamPricePrediction").getOrCreate()

# Load clean data from Parquet
input_path = "output_data/cleaned_steam_game_data.parquet"
df = spark.read.parquet(input_path)

# Ensure release_year is numeric
df = df.withColumn("release_year", col("release_year").cast("int"))

# Prepare features
assembler = VectorAssembler(inputCols=["release_year"], outputCol="features")
df_ml = assembler.transform(df).select("features", "price")

# Split into train and test sets (80% train, 20% test)
train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)

# Train model
lr = LinearRegression(featuresCol="features", labelCol="price")
model = lr.fit(train_df)

# Predict prices
df_predicted = model.transform(test_df)

df.write.mode("overwrite").parquet("output_data/predicted_price.parquet")

# Stop The Spark Session
spark.stop()