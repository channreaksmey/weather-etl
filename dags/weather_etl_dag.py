"""
Weather Data ETL Pipeline
Automated workflow: Extract → Transform → Load
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add plugins to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../plugins'))

from helpers.extract import extract_weather_data
from helpers.transform import transform_weather_data
from helpers.load import load_weather_data

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='Collect weather data from API, process and store in PostgreSQL',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['weather', 'etl', 'api', 'hourly'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather_data,
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather_data,
    )

    load_task = PythonOperator(
        task_id='load_weather',
        python_callable=load_weather_data,
    )

    # Task dependencies: Extract → Transform → Load
    extract_task >> transform_task >> load_task