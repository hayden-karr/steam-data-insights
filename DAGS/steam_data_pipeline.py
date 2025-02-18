from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "steam_data_pipeline",
    default_args=default_args,
    description="Automates the Steam data pipeline",
    schedule_interval="@daily",
)

# Ingestion tasks
fetch_game_list = BashOperator(
    task_id="fetch_game_list",
    bash_command="python /opt/airflow/dags/steam_data_insights/ingestion/steam_getapplist.py",
    dag=dag,
)

fetch_game_details = BashOperator(
    task_id="fetch_game_details",
    bash_command="python /opt/airflow/dags/steam_data_insights/ingestion/fetch_steam_game_details.py",
    dag=dag,
)

# Transformation task
clean_data = BashOperator(
    task_id="clean_data",
    bash_command="python /opt/airflow/dags/steam_data_insights/Transformations/clean_data.py",
    dag=dag,
)

# Analysis tasks (run in parallel)
analyze_price_trends = BashOperator(
    task_id="analyze_price_trends",
    bash_command="python /opt/airflow/dags/steam_data_insights/Transformations/analyze_price_trends.py",
    dag=dag,
)

detect_outliers = BashOperator(
    task_id="detect_outliers",
    bash_command="python /opt/airflow/dags/steam_data_insights/Transformations/detect_outliers.py",
    dag=dag,
)

predict_prices = BashOperator(
    task_id="predict_prices",
    bash_command="python /opt/airflow/dags/steam_data_insights/Transformations/predict_prices.py",
    dag=dag,
)

rank_genre_popularity = BashOperator(
    task_id="rank_genre_popularity",
    bash_command="python /opt/airflow/dags/steam_data_insights/Transformations/rank_genre_popularity_dev_publishes.py",
    dag=dag,
)

# Define task dependencies
fetch_game_list >> fetch_game_details >> clean_data
clean_data >> [analyze_price_trends, detect_outliers, predict_prices, rank_genre_popularity]
