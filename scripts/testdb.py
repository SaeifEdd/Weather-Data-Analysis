# Test script
import pandas as pd
from sqlalchemy import create_engine
from scripts.db import db_engine
# Adjust this to your connection string
engine = db_engine()
# Sample data
data = {"col1": [1, 2], "col2": ["A", "B"]}
df = pd.DataFrame(data)

df.to_sql(name="test_table", con=engine, if_exists="replace", index=False)
# Test connection
# with engine.begin() as connection:
#     df.to_sql(name="test_table", con=connection, if_exists="replace", index=False)
print("Test completed.")