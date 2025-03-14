import polars as pl
from currency_converter import CurrencyConverter

# Initialize CurrencyConverter
c = CurrencyConverter()

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

def convert_prices_to_usd(prices, currencies):
    """Batch converts a list of prices and currency codes to USD."""
    converted_prices = []
    for price, currency in zip(prices, currencies):
        try:
            if price is not None and currency is not None:
                converted_prices.append(round(c.convert(price, currency, "USD"), 2))
            else:
                converted_prices.append(None)
        except Exception:
            converted_prices.append(None)
    return converted_prices

def parse_and_convert_price(df: pl.DataFrame) -> pl.DataFrame:
    """Parses the price column, extracts currency symbols, and converts to USD."""

    # Replace commas with dots for decimal compatibility
    df = df.with_columns(pl.col("price").str.replace(",", "."))

    # Extract currency symbols and numeric prices
    df = df.with_columns([
        pl.col("price").str.extract(r"^([^0-9\s]+)").alias("currency_symbol"),
        pl.col("price").str.extract(r"([\d\.]+)").cast(pl.Float64).alias("numeric_price")
    ])

    # Map symbols to currency codes
    df = df.with_columns(
        pl.col("currency_symbol").replace(currency_symbol_map).alias("currency_code")
    )

    # Perform batch conversion (avoiding row-wise UDFs)
    converted_prices = convert_prices_to_usd(
        df["numeric_price"].to_list(),
        df["currency_code"].to_list()
    )

    # Add converted prices to dataframe
    df = df.with_columns(pl.Series("priceUSD", converted_prices))

    # Drop intermediate columns
    df = df.drop(["currency_symbol", "currency_code", "numeric_price"])

    return df
