import pandas as pd
# import mysql.connector
import json
import pymysql

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']


conn = pymysql.connect(
    host=mysql_config['host'],
    user=mysql_config['user'],
    password=mysql_config['password'],
    database=mysql_config['database']
)

query = "SELECT * FROM clients where region is not null"
df = pd.read_sql(query, conn)
conn.close()
print(df)
