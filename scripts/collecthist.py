import os
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client
def setup_openmeteo_client():
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def fetch_weather_data(start_date, end_date):
    client = setup_openmeteo_client()
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 34.7470,
        "longitude": 10.7601,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "apparent_temperature",
            "precipitation", "surface_pressure", "wind_speed_10m",
            "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm"
        ]
    }
    return client.weather_api(url, params=params)

# Process the API response into a DataFrame
def process_weather_data(response):
    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "apparent_temperature": hourly.Variables(2).ValuesAsNumpy(),
        "precipitation": hourly.Variables(3).ValuesAsNumpy(),
        "surface_pressure": hourly.Variables(4).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(5).ValuesAsNumpy(),
        "soil_temperature_100_to_255cm": hourly.Variables(6).ValuesAsNumpy(),
        "soil_moisture_0_to_7cm": hourly.Variables(7).ValuesAsNumpy()
    }
    return pd.DataFrame(data=hourly_data)

# Save the DataFrame to a CSV file
def save_data(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if not os.path.isfile(file_path):
        data.to_csv(file_path, index=False)
    else:
        print(f"File already exists: {file_path}")


