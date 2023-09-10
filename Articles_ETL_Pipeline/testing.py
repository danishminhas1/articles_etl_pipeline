import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine

env_variables = dotenv_values()
DB_PASSWORD = env_variables.get('DB_PASSWORD')
warehouse_engine = create_engine(f"mysql+mysqlconnector://root:{DB_PASSWORD}@localhost:3306/nyt_warehouse")

sql = "SELECT * FROM geo_dim;"
geo_df = pd.read_sql(sql, warehouse_engine)
print(geo_df)