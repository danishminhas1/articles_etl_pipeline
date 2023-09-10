import pandas as pd
import requests
from dotenv import dotenv_values
from sqlalchemy import create_engine
import mysql.connector

env_variables = dotenv_values()
API_KEY = env_variables.get('API_KEY')
DB_PASSWORD = env_variables.get('DB_PASSWORD')
engine = create_engine(f"mysql+mysqlconnector://root:{DB_PASSWORD}@localhost:3306/nyt")
connection = mysql.connector.connect(
        host='localhost',  
        database='nyt',  
        user='root', 
        password=DB_PASSWORD  
    )
connection.cursor().execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

#Get data
x = requests.get(f"https://api.nytimes.com/svc/mostpopular/v2/viewed/1.json?api-key={API_KEY}")
data = x.json()
num_results = data['num_results']
count = 0

for i in data['results']:
    data_list = []
    #For article table
    data_dict = {
        'url': i['url'],
        'source': i['source'],
        'published_date': i['published_date'],
        'updated': i['updated'],
        'section': i['section'],
        'subsection': i['subsection'],
        'title': i['title'],
        'abstract': i['abstract'],
        'byline': i['byline'],
        'type': i['type']
    }
    data_list.append(data_dict)
    df = pd.DataFrame(data_list)
    table_name = "article"
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    #print("Success!")

    #Select the ID of the latest inserted record keywords table
    select_query = 'SELECT id FROM article ORDER BY id desc LIMIT 1'
    cursor = connection.cursor()
    cursor.execute(select_query)
    last_row = cursor.fetchone()
    keywords_id = last_row[0]

    #For keywords table
    keywords = str(i['adx_keywords']).split(";")
    keywords_dict = {
    'id': [keywords_id] * len(keywords),
    'keyword': keywords
    }
    keywords_df = pd.DataFrame(keywords_dict, index=range(len(keywords)))
    keywords_df.to_sql(name="keywords", con=engine, if_exists='append', index=False)

    #For des_facet table
    descriptions = (i['des_facet'])
    descriptions_dict = {
    'id': [keywords_id] * len(descriptions),
    'description': descriptions
    }
    descriptions_df = pd.DataFrame(descriptions_dict, index=range(len(descriptions)))
    descriptions_df.to_sql(name="des_facet", con=engine, if_exists='append', index=False)

    #For geo_facet table
    locations = (i['geo_facet'])
    locations_dict = {
    'id': [keywords_id] * len(locations),
    'location': locations
    }
    locations_df = pd.DataFrame(locations_dict, index=range(len(locations)))
    locations_df.to_sql(name="geo_facet", con=engine, if_exists='append', index=False)
    count += 1
    if count == 10: break
connection.close()
print("Success!")
