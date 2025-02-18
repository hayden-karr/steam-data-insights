import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from currency_converter import CurrencyConverter
from pyspark.sql.functions import udf

# Initialize the currency converter
c = CurrencyConverter()

# Define the mapping of currency symbols to currency codes for the converter
currency_symbol_map = {
    'R$': 'BRL',  # Brazilian Real
    '$': 'USD',   # US Dollar
    '€': 'EUR',   # Euro
    '£': 'GBP',   # British Pound
    '₹': 'INR',   # Indian Rupee
    '₣': 'CHF',   # Swiss Franc
    'A$': 'AUD',  # Australian Dollar
}

# UDF for currency conversion
@udf(DoubleType())
def convert_to_usd(price, currency_symbol):
    """Convert the price from a given currency symbol to USD."""
    try:
        # Check if symbol exists in the map
        if currency_symbol in currency_symbol_map:
            # Use the symbol's mapped currency code
            currency_code = currency_symbol_map[currency_symbol]
            if price is not None:
                return c.convert(price, currency_code, "USD")
    except Exception as e:
        print(f"Error converting {currency_symbol} to USD: {e}")
    
    # Return None if no valid conversion was possible
    return None

def parse_and_convert_price(df):
    """Parse price column, extract currency symbol, and apply conversion to USD."""
    
    # Clean the price column: replace commas with dots for proper numeric format
    df = df.withColumn("price_cleaned", F.regexp_replace(F.col("price"), ",", "."))

    # Extract the currency symbol using regex (first non-numeric character(s) in the price)
    df = df.withColumn("currency_symbol", F.regexp_extract(F.col("price"), "^([^\\d\\s]+)", 1))

    # Extract the numeric part of the price
    df = df.withColumn("numeric_price", F.regexp_extract(F.col("price_cleaned"), "[\\d\\.]+", 0).cast(DoubleType()))

    # Apply the conversion to USD
    df = df.withColumn("priceUSD", F.round(convert_to_usd(F.col("numeric_price"), F.col("currency_symbol")), 2))

    # Drop intermediate columns for cleanup
    df = df.drop("price_cleaned", "currency_symbol", "numeric_price")

    return df

