import pandas as pd
import os

def clean_weather_data(input_file, output_file):

    # Read the raw data
    df = pd.read_csv(input_file)

    # Round numeric columns to 2 decimal places
    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    # Reformat date column
    df['date'] = pd.to_datetime(df['date'])
    df['hour'] = df['date'].dt.hour
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month

    # Remove time from the 'date' column
    df['date'] = df['date'].dt.date

    # Drop soil information columns
    df.drop(['soil_temperature_100_to_255cm', 'soil_moisture_0_to_7cm'], axis=1, inplace=True)

    # Save cleaned data, replacing the file if it exists
    #os.makedirs(os.path.dirname(output_file), exist_ok=True)  # Ensure the directory exists
    df.to_csv(output_file, index=False)