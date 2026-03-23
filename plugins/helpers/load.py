"""
Load: Store processed weather data in PostgreSQL
"""

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import logging

logger = logging.getLogger(__name__)

def load_weather_data(**context):
    """
    Load weather data to PostgreSQL
    """
    logger.info("=" * 60)
    logger.info("STEP 3: LOAD - Storing to PostgreSQL")
    logger.info("=" * 60)
    
    # Pull from XCom
    ti = context['ti']
    clean_json = ti.xcom_pull(task_ids='transform_weather', key='clean_weather_data')
    
    if not clean_json:
        raise AirflowFailException("No data from transform task")
    
    df = pd.read_json(clean_json)
    logger.info(f"Loading {len(df)} records to database")
    
    try:
        # Connect using PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_weather')
        
        # Truncate and load pattern for daily snapshots
        # Or use append for historical data
        
        # Convert timestamp for PostgreSQL
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['processed_at'] = pd.to_datetime(df['processed_at'])
        
        # Insert data
        rows = df.to_sql(
            'weather_data',
            con=pg_hook.get_sqlalchemy_engine(),
            if_exists='append',  # Options: 'fail', 'replace', 'append'
            index=False,
            chunksize=1000
        )
        
        logger.info(f"Successfully loaded {len(df)} rows to PostgreSQL")
        
        # Also create/update summary statistics
        update_summary_stats(pg_hook, df)
        
        return f"Loaded {len(df)} weather records to PostgreSQL"
        
    except Exception as e:
        logger.error(f"Database load failed: {e}")
        raise AirflowFailException(f"PostgreSQL load failed: {e}")

def update_summary_stats(pg_hook, df):
    """
    Update summary statistics table for dashboard
    """
    try:
        # Calculate city averages
        summary = df.groupby('city').agg({
            'temperature': 'mean',
            'humidity': 'mean',
            'comfort_score': 'mean'
        }).reset_index()
        
        summary['updated_at'] = pd.Timestamp.now()
        
        summary.to_sql(
            'weather_summary',
            con=pg_hook.get_sqlalchemy_engine(),
            if_exists='replace',
            index=False
        )
        
        logger.info(f"Updated summary stats for {len(summary)} cities")
        
    except Exception as e:
        logger.warning(f"Summary stats update failed: {e}")