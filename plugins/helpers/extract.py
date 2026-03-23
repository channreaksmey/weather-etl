"""
Extract: Pull weather data from WeatherAPI
"""

import requests
import pandas as pd
from airflow.exceptions import AirflowFailException
import logging
import os

logger = logging.getLogger(__name__)

# API Configuration
API_KEY = os.getenv('WEATHERAPI_KEY', 'your_api_key_here')
CITIES = ['London', 'New York', 'Tokyo', 'Singapore', 'Sydney']
BASE_URL = 'https://api.weatherapi.com/v1/current.json'

def extract_weather_data(**context):
    """
    Fetch weather data from API for multiple cities
    """
    logger.info("=" * 60)
    logger.info("STEP 1: EXTRACT - Fetching Weather Data")
    logger.info("=" * 60)
    
    execution_date = context['ds']
    all_weather_data = []
    
    for city in CITIES:
        try:
            logger.info(f"Fetching data for: {city}")
            
            params = {
                'q': city,
                'key': API_KEY,
                'aqi': 'no' 
            }
            
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract relevant fields
            weather_record = {
                "city": city,
                "country": data["location"]["country"],
                "temperature": data["current"]["temp_c"],
                "feels_like": data["current"]["feelslike_c"],
                "humidity": data["current"]["humidity"],
                "pressure": data["current"]["pressure_mb"],
                "weather_main": data["current"]["condition"]["text"].split()[0],
                "weather_description": data["current"]["condition"]["text"],
                "wind_speed": round(data["current"]["wind_kph"] / 3.6, 2),
                "clouds": data["current"].get("cloud", 0),
                "visibility": int(data["current"].get("vis_km", 0) * 1000),
                "timestamp": pd.Timestamp.now(),
                "execution_date": execution_date,
                }
            
            all_weather_data.append(weather_record)
            logger.info(f"{city}: {data['current']['temp_c']}°C, {data['current']['condition']['text']}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API error for {city}: {e}")
            continue
        except KeyError as e:
            logger.error(f"Data parsing error for {city}: {e}")
            continue
    
    if not all_weather_data:
        raise AirflowFailException("No weather data retrieved from API")
    
    # Convert to DataFrame and push to XCom
    df = pd.DataFrame(all_weather_data)
    
    context['ti'].xcom_push(key='raw_weather_data', value=df.to_json())
    context['ti'].xcom_push(key='cities_count', value=len(all_weather_data))
    
    logger.info(f"Extracted data for {len(all_weather_data)} cities")
    return f"Extracted weather data for {len(all_weather_data)} cities"