from configparser import ConfigParser
from sqlalchemy import create_engine
import psycopg
import pandas as pd

# get configurations
def config(filename="db.ini", section="postgresql"):
    parser = ConfigParser()
    #read file
    parser.read(filename)
    db =  {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception("section {0} is not found in "
                        "file {1}".format(section, filename))
    return db

def db_engine():
    db_params = config()
    user = db_params["user"]
    password = db_params["password"]
    host = db_params["host"]
    port = db_params["port"]
    database = db_params["dbname"]
    return create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}")

# Load cleaned data
df = pd.read_csv("data/clean_weather_data.csv")

# create table weather_data
engine = db_engine()
df.to_sql(
    name="weather_data",  # Table name
    con=engine,           # Database engine
    if_exists="replace",  # Replace table if it exists
    index=False           # Do not include DataFrame index as a column
)
print("Data saved to database!")