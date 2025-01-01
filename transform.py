from db import db_engine
import pandas as pd

# get engine
engine = db_engine()

#daily_agg CTE: compute daily averages of temperature, wind speed ...
#temp_diff CTE: compute difference between today and yesterday temperature
#select new features with old ones with join
query = ("WITH daily_agg AS ("
         "SELECT "
            "day,"
            "SUM(precipitation) as daily_precip,"
            "AVG(temperature_2m) as daily_avg_temp,"
            "AVG(relative_humidity_2m) as daily_avg_humid,"
            "AVG(wind_speed_10m) as daily_avg_wspeed "
         "FROM  "
            "weather_data "
         "GROUP BY "
            "day), "
         "temp_diff AS ("
         "SELECT *,"
            "daily_avg_temp - LAG(daily_avg_temp) OVER (ORDER BY day) as temp_diff_prev_day"
         " FROM "
            "daily_agg) "
         "SELECT "
            "wd.*, "
            "d.daily_avg_temp, "
            "d.daily_avg_humid, "
            "d.daily_avg_wspeed, "
            "d.daily_precip, "
            "d.temp_diff_prev_day "
        "FROM " 
            "weather_data wd "
        "JOIN " 
            "temp_diff d "
        "ON "
            "wd.day = d.day;")

df = pd.read_sql(query, engine)
#create table with new features
df.to_sql("new_weather", engine, if_exists="replace", index=False)
#print(df.head())
