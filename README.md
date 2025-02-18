# steam-data-insights
 A data pipeline to analyze video game trends using the Steam API and historical sales data.

# Work In Progress
Working towards the goal of learning about and implementing the usage of Polars, PySpark, AWS, Parquest, Tableau and Apache Airflow. 

# Features
Features
1. API Rate Limiting Compliance

    The Steam API allows 100,000 calls per day.
    The script tracks daily API usage and stops fetching data when the limit is reached.
    Redis caching prevents unnecessary API calls by storing previously retrieved game data. - Investigating

2. Batch Processing for Efficiency

    The script processes game details in batches (default: 100 games at a time).
    This minimizes redundant requests and optimizes data retrieval.

3. Caching with Redis - Investigating - local file used instead

    Game details are cached for 24 hours to prevent repeated requests.
    Before making an API call, the script checks if the data is already cached.
    Cached data significantly reduces API consumption and improves performance.

4. Error Handling and Retry Logic

    Implements exponential backoff for handling temporary API failures.
    Retries failed API calls up to 5 times before skipping a game.

5. Incremental Data Storage

    Previously processed game IDs are stored in processed_appids.csv to avoid duplicate processing.
    Fetches only new game data that hasn't been collected before.
    Stores game data in a Parquet file, which is optimized for efficient querying and storage.

6. Storage Limit Enforcement

    The script stops collecting data when the file size exceeds the defined limit (default: 2000 bytes for testing).
    This prevents excessive storage usage.

7. Automatic S3 Upload (Optional)

    Supports downloading and uploading Parquet files from AWS S3.
    Easily integrates into cloud-based workflows.

# Want to add
- Multi-threading 
- Data cleaning using Polars (Practice)
- Currency conversion using UDF via pandas/polars AND looking it to possibly broadcasting conversions
- Docker?
- Industry standardized practices 