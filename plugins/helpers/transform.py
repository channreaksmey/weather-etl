"""
Transform: Clean and process weather data
"""

import pandas as pd
import numpy as np
from airflow.exceptions import AirflowFailException
import logging

logger = logging.getLogger(__name__)

def transform_weather_data(**context):
    """
    Clean and enrich weather data
    """
    logger.info("=" * 60)
    logger.info("STEP 2: TRANSFORM - Processing Weather Data")
    logger.info("=" * 60)
    
    # Pull from XCom
    ti = context['ti']
    raw_json = ti.xcom_pull(task_ids='extract_weather', key='raw_weather_data')
    
    if not raw_json:
        raise AirflowFailException("No data from extract task")
    
    df = pd.read_json(raw_json)
    initial_count = len(df)
    logger.info(f"Received {initial_count} records")
    
    # Data Cleaning & Transformation
    logger.info("Cleaning data...")
    
    # 1. Remove duplicates (if any)
    df = df.drop_duplicates(subset=['city', 'timestamp'])
    
    # 2. Handle missing values
    df['visibility'] = df['visibility'].fillna(10000)  # Default visibility
    
    # 3. Data type conversions
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
    df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce')
    
    # 4. Feature Engineering
    logger.info("Creating features...")
    
    # Temperature categories
    def categorize_temp(temp):
        if temp < 0:
            return 'Freezing'
        elif temp < 10:
            return 'Cold'
        elif temp < 20:
            return 'Mild'
        elif temp < 30:
            return 'Warm'
        else:
            return 'Hot'
    
    df['temp_category'] = df['temperature'].apply(categorize_temp)
    
    # Comfort index (simple calculation)
    df['comfort_score'] = 100 - abs(df['temperature'] - 22) * 2 - df['humidity'] * 0.1
    
    # Wind category
    def categorize_wind(speed):
        if speed < 1:
            return 'Calm'
        elif speed < 5:
            return 'Light'
        elif speed < 10:
            return 'Moderate'
        elif speed < 20:
            return 'Strong'
        else:
            return 'Storm'
    
    df['wind_category'] = df['wind_speed'].apply(categorize_wind)
    
    # 5. Add metadata
    df['processed_at'] = pd.Timestamp.now()
    
    final_count = len(df)
    logger.info(f"Transformed {final_count} records")
    logger.info(f"Features added: temp_category, comfort_score, wind_category")
    
    # Push to XCom
    ti.xcom_push(key='clean_weather_data', value=df.to_json())
    ti.xcom_push(key='records_transformed', value=final_count)
    
    return f"Transformed {final_count} weather records"