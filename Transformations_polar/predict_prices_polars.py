import polars as pl
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pickle

# Load cleaned Parquet data
input_path = "output_data/cleaned_steam_game_data.parquet"
df = pl.read_parquet(input_path)

# Ensure release_year is numeric
df = df.with_columns(pl.col("release_year").cast(pl.Int32))

# Extract features (X) and target (y)
X = df["release_year"].to_numpy().reshape(-1, 1)
y = df["price"].to_numpy()

# Split into train and test sets (80% train, 20% test)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Predict prices
y_pred = model.predict(X_test)

# Convert predictions back to Polars DataFrame
df_predicted = pl.DataFrame({
    "release_year": X_test.flatten(),
    "actual_price": y_test,
    "predicted_price": y_pred
})

# Save predictions as Parquet file
df_predicted.write_parquet("output_data/predicted_price.parquet", compression="zstd")

# Optional: Save the trained model for later use
with open("output_data/linear_regression_model.pkl", "wb") as f:
    pickle.dump(model, f)
