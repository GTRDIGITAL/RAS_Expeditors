# import mysql.connector
import datetime
import os
from flask_login import login_required, current_user
from flask import session, send_from_directory
import json
import zipfile
import shutil
import time
import pymysql
import requests
import xml.etree.ElementTree as ET
import mysql.connector


def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']

def get_all_users():
    try:
        db = pymysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
        cursor = db.cursor()  # Dicționar pentru a obține rezultate ca JSON

        query = "SELECT id, username, role FROM users"
        cursor.execute(query)
        users = cursor.fetchall()  # Lista de utilizatori
        users_list = [{'id': user[0], 'username': user[1], 'role': user[2]} for user in users]
        print('aici sunt userii', users)

        cursor.close()
        db.close()

        return users_list # Returnează lista de utilizatori
    except KeyError as e:
        print(f"Eroare la preluarea utilizatorilor: {e}")
        return []
    
import mysql.connector

def get_user_from_db(user_id):
    connection = mysql.connector.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)  # Rezultatul va fi un dicționar

    query = "SELECT id, username, role FROM users WHERE id = %s"
    cursor.execute(query, (user_id,))
    user = cursor.fetchone()

    cursor.close()
    connection.close()

    return user

# def get_gl_from_db(startMonth, startYear, endMonth, endYear):
#     connection = mysql.connector.connect(
#         host=mysql_config['host'],
#         user=mysql_config['user'],
#         password=mysql_config['password'],
#         database=mysql_config['database']
#     )
#     cursor = connection.cursor(dictionary=True)  # Rezultatul va fi un dicționar

#     query = """
#     SELECT Month, Year, GL, Statutory_GL, Br,  Amount
#     FROM general_ledger
#     WHERE DATE_FORMAT(date, '%%Y-%%m') BETWEEN %s AND %s
# """
#     periodStart = f"{startYear:04d}-{startMonth:02d}"
#     periodEnd = f"{endYear:04d}-{endMonth:02d}"
#     cursor.execute(query, (periodStart, periodEnd))

#     cursor.close()
#     connection.close()

#     return gl     
def update_user_in_db(user_id, username, role):
    connection = mysql.connector.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    query = "UPDATE users SET username = %s, role = %s WHERE id = %s"
    cursor.execute(query, (username, role, user_id))
    connection.commit()

    cursor.close()
    connection.close()
