"""
Weather Data Dashboard
Streamlit app for visualizing weather data from PostgreSQL
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# Page configuration
st.set_page_config(
    page_title="Weather Data Dashboard",
    page_icon="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200&icon_names=partly_cloudy_day",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_db_engine():
    """Create database connection"""
    # Get from environment or use default
    db_url = os.getenv(
        'DATABASE_URL',
        'postgresql://airflow:airflow@localhost:5432/weather_db'
    )
    return create_engine(db_url)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load weather data from PostgreSQL"""
    engine = get_db_engine()
    
    # Latest weather data
    query = """
    SELECT * FROM weather_data 
    ORDER BY timestamp DESC 
    LIMIT 100
    """
    
    df = pd.read_sql(query, engine)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df

@st.cache_data(ttl=300)
def load_summary():
    """Load summary statistics"""
    engine = get_db_engine()
    
    query = "SELECT * FROM weather_summary"
    return pd.read_sql(query, engine)

def main():
    st.title("Weather Data Pipeline Dashboard")
    st.markdown("Real-time weather data from WeatherAPI")
    
    # Load data
    try:
        df = load_data()
        summary = load_summary()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Make sure the ETL pipeline has run at least once")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    cities = st.sidebar.multiselect(
        "Select Cities",
        options=df['city'].unique(),
        default=df['city'].unique()[:3]
    )
    
    # Filter data
    if cities:
        filtered_df = df[df['city'].isin(cities)]
    else:
        filtered_df = df
    
    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Avg Temperature", f"{filtered_df['temperature'].mean():.1f}°C")
    with col2:
        st.metric("Avg Humidity", f"{filtered_df['humidity'].mean():.1f}%")
    with col3:
        st.metric("Cities Tracked", len(filtered_df['city'].unique()))
    with col4:
        st.metric("Last Update", filtered_df['timestamp'].max().strftime('%H:%M'))
    
    # Charts
    st.header("Current Weather Conditions")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Temperature by City
        fig_temp = px.bar(
            filtered_df,
            x='city',
            y='temperature',
            color='temp_category',
            title='Temperature by City',
            labels={'temperature': 'Temperature (°C)', 'city': 'City'}
        )
        st.plotly_chart(fig_temp, use_container_width=True)
    
    with col2:
        # Humidity vs Temperature scatter
        fig_scatter = px.scatter(
            filtered_df,
            x='temperature',
            y='humidity',
            size='wind_speed',
            color='city',
            title='Temperature vs Humidity',
            hover_data=['feels_like', 'weather_description']
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Weather conditions distribution
    st.header("Weather Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Weather conditions pie chart
        weather_counts = filtered_df['weather_main'].value_counts()
        fig_pie = px.pie(
            values=weather_counts.values,
            names=weather_counts.index,
            title='Weather Conditions'
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Wind speed by city
        fig_wind = px.box(
            filtered_df,
            x='city',
            y='wind_speed',
            title='Wind Speed Distribution'
        )
        st.plotly_chart(fig_wind, use_container_width=True)
    
    # Raw data table
    st.header("Latest Data")
    st.dataframe(
        filtered_df[[
            'city', 'temperature', 'feels_like', 'humidity',
            'weather_description', 'wind_speed', 'timestamp'
        ]].sort_values('timestamp', ascending=False),
        use_container_width=True
    )
    
    # Footer
    st.markdown("---")
    st.markdown("Data updates every hour via Apache Airflow ETL pipeline")

if __name__ == "__main__":
    main()