import pandas as pd

df = pd.read_csv('data/weather_data.csv')

#round numeric columns to 2 decimal places
numeric_cols = df.select_dtypes(include=['float', 'int']).columns
df[numeric_cols] = df[numeric_cols].round(2)
#reformat date column
df['date'] = pd.to_datetime(df['date'])
df['hour'] = df['date'].dt.hour
df['day'] = df['date'].dt.day
df['month'] = df['date'].dt.month
#remove time from 'date' column
df['date'] = df['date'].dt.date
#drop soil info columns
df.drop(['soil_temperature_100_to_255cm', 'soil_moisture_0_to_7cm'], axis=1, inplace=True)

#save cleaned data to a new file
cleaned_csv_file = 'data/clean_weather_data.csv'
df.to_csv(cleaned_csv_file, index=False)
