from configparser import ConfigParser
from sqlalchemy import create_engine
import psycopg
import pandas as pd
import os

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


# Function to load and save cleaned data into the PostgreSQL database
def save_data_to_db(csv_file, table_name="weather_data"):

    if not os.path.isfile(csv_file):
        print(f"File {csv_file} not found!")
        return

    # Load cleaned data
    df = pd.read_csv(csv_file)

    # Create a database engine
    engine = db_engine()

    # Save the data into the database table
    df.to_sql(
        name=table_name,  # Table name
        con=engine,  # Database engine
        if_exists="replace",  # Replace table if it exists
        index=False  # Do not include DataFrame index as a column
    )

    print(f"Data saved to table '{table_name}' in the database!")


# Example Usage
#csv_file = "data/clean_weather_data.csv"
#save_data_to_db(csv_file)