import os
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession

# Initialize Spark (Only needed if running independently)
spark = SparkSession.builder.appName("OptimizedCurrencyConversion").getOrCreate()

# Ensure Spark finds the currency conversion file if running in a distributed environment
currency_conversion_path = os.path.join("Transformations", "currency_convert.py")
sc = spark.sparkContext
sc.addPyFile(currency_conversion_path)

# Currency symbol to ISO code mapping
currency_symbol_map = {
    'R$': 'BRL',  # Brazilian Real
    '$': 'USD',   # US Dollar
    '€': 'EUR',   # Euro
    '£': 'GBP',   # British Pound
    '₹': 'INR',   # Indian Rupee
    '₣': 'CHF',   # Swiss Franc
    'A$': 'AUD',  # Australian Dollar
}

# Convert mapping to DataFrame & Broadcast it
currency_df = spark.createDataFrame(list(currency_symbol_map.items()), ["symbol", "currency_code"])
currency_df = F.broadcast(currency_df)  # Broadcast for efficiency

# Define Pandas UDF for currency conversion
@pandas_udf(DoubleType())
def convert_to_usd_pandas(price_series: pd.Series, currency_series: pd.Series) -> pd.Series:
    """Efficiently converts a batch of prices to USD using Pandas UDF."""
    
    # Import inside the UDF to prevent serialization errors
    from currency_converter import CurrencyConverter
    c = CurrencyConverter()

    converted_prices = []
    for price, currency in zip(price_series, currency_series):
        try:
            if pd.notnull(price) and pd.notnull(currency):
                converted_prices.append(c.convert(price, currency, "USD"))
            else:
                converted_prices.append(None)  # Preserve null values
        except Exception as e:
            print(f"Error converting {currency} to USD: {e}")
            converted_prices.append(None)
    
    return pd.Series(converted_prices)

def parse_and_convert_price(df):
    """Optimized function to parse price, extract currency, and convert to USD efficiently."""

    # Clean price column: replace commas with dots
    df = df.withColumn("price_cleaned", F.regexp_replace(F.col("price"), ",", "."))

    # Extract currency symbol (handles multi-character symbols like "R$")
    df = df.withColumn("currency_symbol", F.regexp_extract(F.col("price"), "^([^\\d\\s]+)", 1))

    # Extract numeric price
    df = df.withColumn("numeric_price", F.regexp_extract(F.col("price_cleaned"), "[\\d\\.]+", 0).cast(DoubleType()))

    # Join with broadcasted currency mapping for efficient lookup
    df = df.join(currency_df, df.currency_symbol == currency_df.symbol, "left")

    # Apply Pandas UDF for efficient batch conversion
    df = df.withColumn("priceUSD", F.round(convert_to_usd_pandas(F.col("numeric_price"), F.col("currency_code")), 2))

    # Drop intermediate columns
    df = df.drop("price_cleaned", "currency_symbol", "currency_code", "numeric_price")

    return df

