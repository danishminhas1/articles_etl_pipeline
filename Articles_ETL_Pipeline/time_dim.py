import pandas as pd
import datetime
from sqlalchemy import create_engine
from dotenv import dotenv_values

# Get the list of dates
dates = []
today = datetime.datetime(2023, 1, 1)
end_date = today.replace(year=today.year + 1)
count = 0
for day in range((end_date - today).days + 1):
  count = count + 1
  dates.append([count, today + datetime.timedelta(days=day)])
for d in dates[1]:
  pd.to_datetime(d)

final_dates = list()
for d in dates:
  final_dates.append({'time_key': d[0], 
  'full_date': d[1], 
  'date': d[1].day, 
  'month': d[1].month, 
  'year': d[1].year})
df = pd.DataFrame(final_dates)
print(df)

env_variables = dotenv_values()
DB_PASSWORD = env_variables.get('DB_PASSWORD')
engine = create_engine(f"mysql+mysqlconnector://root:{DB_PASSWORD}@localhost:3306/nyt_warehouse")
df.to_sql(name="time_dim", con=engine, if_exists='replace', index=False)

