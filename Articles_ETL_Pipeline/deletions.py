from dotenv import dotenv_values
import mysql.connector

env_variables = dotenv_values()
DB_PASSWORD = env_variables.get('DB_PASSWORD')

connection = mysql.connector.connect(
        host='localhost', 
        database='nyt', 
        user='root', 
        password=DB_PASSWORD 
    )

connection.cursor().execute("delete from des_facet")
connection.cursor().execute("delete from geo_facet")
connection.cursor().execute("delete from keywords")
connection.cursor().execute("delete from article")
connection.commit()
connection.close()
print("Deletions from OLTP completed!")