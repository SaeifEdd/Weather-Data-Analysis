import os
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime, timedelta

# Setup the Open-Meteo API client
def setup_openmeteo_client():
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def fetch_data(start_date):
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")

    # Calculate the end of the month
    next_month = start_date_obj.replace(day=28) + timedelta(days=4)
    end_date_obj = next_month.replace(day=1) - timedelta(days=1)

    # Format the end_date as a string
    end_date = end_date_obj.strftime("%Y-%m-%d")
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
def process_data(responses):
    response = responses[0]
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


def collect(start_date, file_path):
    try:
        # Step 1: Fetch data
        response = fetch_data(start_date)

        # Step 2: Process data
        weather_data = process_data(response)

        # Step 3: Save data to file
        save_data(weather_data, file_path)
        print(f"Data saved to {file_path}")
    except Exception as e:
        print(f"Error in collecting or saving weather data: {e}")
