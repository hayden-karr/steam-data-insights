from parquet_handler import save_to_parquet, update_parquet_with_new_data
import polars as pl




if __name__ == "__main__":

    existing_df = pl.DataFrame([
        {"appid": 1001, "name": "GAME1", "release_date": "2022-01-01", "price": 6.99, "genres": "Action", "developers": "DevA", "publishers": "PubA"},
        {"appid": 1002, "name": "GAME2", "release_date": "2021-05-15", "price": 10.99, "genres": "Adventure", "developers": "DevB", "publishers": "PubB"}
        ])

    existing_df.write_parquet("output_data/games.parquet")

    new_df = pl.DataFrame([
        {"appid": 1001, "name": "GAME1", "release_date": "2022-01-01", "price": 9.99, "genres": "Action", "developers": "DevA", "publishers": "PubA"},
        {"appid": 1002, "name": "GAME2", "release_date": "2021-05-15", "price": 12.99, "genres": "RPG", "developers": "DevB", "publishers": "PubB"},
        {"appid": 1003, "name": "GAME3", "release_date": "2021-12-15", "price": 16.99, "genres": "Hello", "developers": "DevC", "publishers": "PubC"},
        {"appid": 1004, "name": "GAME4", "release_date": "2021-12-17", "price": .99, "genres": "Puzzle", "developers": "DevD", "publishers": "PubD"}
        ])

    updated_df  = update_parquet_with_new_data(new_df, existing_df, "games.parquet")
    
    save_to_parquet(updated_df, "games.parquet")