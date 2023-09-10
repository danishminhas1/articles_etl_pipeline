import pandas as pd
import requests
from dotenv import dotenv_values
from sqlalchemy import create_engine
import mysql.connector

env_variables = dotenv_values()
DB_PASSWORD = env_variables.get('DB_PASSWORD')
engine = create_engine(f"mysql+mysqlconnector://root:{DB_PASSWORD}@localhost:3306/nyt")
warehouse_engine = create_engine(f"mysql+mysqlconnector://root:{DB_PASSWORD}@localhost:3306/nyt_warehouse")

sql = "SELECT * FROM geo_facet;"
geo_facet_df = pd.read_sql(sql, engine)
geo_facet_df.to_sql(name="geo_dim", con=warehouse_engine, if_exists='append', index=False)

sql = "SELECT * FROM des_facet;"
des_df = pd.read_sql(sql, engine)
des_df.to_sql(name="des_dim", con=warehouse_engine, if_exists='append', index=False)

sql = "SELECT * FROM keywords;"
keywords_df = pd.read_sql(sql, engine)
keywords_df.to_sql(name="keywords_dim", con=warehouse_engine, if_exists='append', index=False)

sql = "select * from article"
oltp_article = pd.read_sql(sql, engine)

olap_articles = []
for row in oltp_article.itertuples():
    curr_olap_row = {}
    curr_olap_row['id'] = row[1]
    curr_olap_row['url'] = row[2]
    curr_olap_row['source'] = row[3]

    published_date = row[4]
    curr_olap_row['published_fk'] = int(pd.read_sql(f"select time_key from time_dim where full_date='{published_date}'", warehouse_engine)['time_key'])

    updated_date = row[5]
    ts = pd.Timestamp(updated_date)
    dt = ts.to_pydatetime().date()
    curr_olap_row['updated_fk'] = int(pd.read_sql(f"select time_key from time_dim where full_date='{dt}'", warehouse_engine)['time_key'])

    section = row[6]
    sql = "SELECT * FROM section_dim;"
    section_df = pd.read_sql(sql, warehouse_engine)
    if section in list(section_df['section_name']):
        curr_olap_row['section_fk'] = int(pd.read_sql(f"select section_key from section_dim where section_name='{section}'", warehouse_engine)['section_key'])
    else:
        max_section_key = section_df['section_key'].max()
        new_row = {'section_key': max_section_key + 1, 'section_name': row[6]}
        curr_olap_row['section_fk'] = max_section_key + 1
        section_df = section_df.append(new_row, ignore_index=True)
        section_df.to_sql(name="section_dim", con=warehouse_engine, if_exists='replace', index=False)
    
    curr_olap_row['subsection'] = row[7]
    curr_olap_row['title'] = row[8]
    curr_olap_row['abstract'] = row[9]
    curr_olap_row['byline'] = row[10]

    this_type = row[11]
    sql = "SELECT * FROM type_dim;"
    type_df = pd.read_sql(sql, warehouse_engine)
    if this_type in list(type_df['content_type']):
        curr_olap_row['type_fk'] = int(pd.read_sql(f"select type_key from type_dim where content_type='{this_type}'", warehouse_engine)['type_key'])
    else:
        max_type_key = type_df['type_key'].max()
        new_row = {'type_key': max_type_key + 1, 'content_type': row[11]}
        curr_olap_row['type_fk'] = max_type_key + 1
        type_df = type_df.append(new_row, ignore_index=True)
        type_df.to_sql(name="type_dim", con=warehouse_engine, if_exists='replace', index=False)

    extract_date = row[12]
    ts = pd.Timestamp(extract_date)
    dt = ts.to_pydatetime().date()
    curr_olap_row['time_fk'] = int(pd.read_sql(f"select time_key from time_dim where full_date='{dt}'", warehouse_engine)['time_key'])
    olap_articles.append(curr_olap_row)
    
olap_df = pd.DataFrame(olap_articles)
olap_df.to_sql(name="article_fact", con=warehouse_engine, if_exists='append', index=True)






    

    

    


