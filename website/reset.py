# import mysql.connector
from werkzeug.security import generate_password_hash
import json
import pymysql

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']

mydb = pymysql.connect(
    host=mysql_config['host'],
    user=mysql_config['user'],
    password=mysql_config['password'],
    database=mysql_config['database']
)

mycursor = mydb.cursor()


def reset_password(username, new_password):
    cursor = mydb.cursor()
    sql = "SELECT * FROM users WHERE username = %s"
    val = (username,)
    cursor.execute(sql, val)
    user = cursor.fetchone()

    if user:
        hashed_password = generate_password_hash(new_password)
        sql_update_pass = "UPDATE users SET password = %s WHERE username = %s"
        val_update_pass = (hashed_password, username)
        cursor.execute(sql_update_pass, val_update_pass)
        mydb.commit()
        print(f"Parola pentru {username} a fost resetată cu succes!")
    else:
        print("Utilizatorul nu există în baza de date!")

reset_password('test3@abcd', 'test12')