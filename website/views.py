from flask import Blueprint, render_template, redirect, request, session, flash, url_for, jsonify
from flask_login import login_user, login_required, logout_user, current_user
from . import db
from datetime import datetime
import zipfile
from datetime import date
# from .utils import extract_name_from_email
from .otp import generate_new_code
from .mail import trimitereMail, trimitereOTPMail
import utils
from .database import get_all_users, get_user_from_db, update_user_in_db
# from .stocareBD import *
# import mysql
from .decorators import admin_required
from .models import Users
import pandas as pd
import os
import uuid
import decimal
from .insert_GL import import_into_db
from celery import Celery  # Import Celery
from celery.result import AsyncResult
import logging
import redis  # Import the redis library
import json
from collections import defaultdict
from .trimitereCodOTP import trimitereFilesMail
import mysql.connector as mysql
from .tasks import *
from .config import *
from .insert_GL import *
from .procedurasql import *
from calendar import monthrange
abc = ''
def extract_name_from_email(email):
    local_part = email.split('@')[0]
    name_parts = local_part.replace('.', ' ').replace('-', ' ').split(' ')
    formatted_name = ' '.join([part.capitalize() for part in name_parts])
    return formatted_name


# filepath: c:\Dezvoltare\RAS\RAS Expeditors\website\views.py
from dotenv import load_dotenv

views = Blueprint('views', __name__)

UPLOAD_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'uploads'))  # Define the upload folder
print(f"UPLOAD_FOLDER: {UPLOAD_FOLDER}")
print(f"Current working directory: {os.getcwd()}")
print(f"Files in upload folder: {os.listdir(UPLOAD_FOLDER)}")

TEMP_FOLDER= 'D:\\Projects\\35. GIT RAS\\RAS_Expeditors\\temp'  # Define the temporary folder
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']
def get_previous_month(year, month):
    if month == 1:
        return year - 1, 12
    else:
        return year, month - 1


# @views.route('/mapare')
# def porneste_maparea():
#     mapareSQL_task.delay()  # ✅ pornește task-ul în fundal
#     flash("🔄 Mapare pornită în fundal!")
#     return redirect(url_for('views.home'))

def insert_nc_into_general_ledger(df):
    

    # Înlocuiește NaN cu None
    df = df.where(pd.notnull(df), None)

    # Generează UUID pe fiecare Journal
    # journal_uuid_map = {journal: str(uuid.uuid4()) for journal in df['Journal'].unique()}
    # df['Acct_Tran_Id'] = df['Journal'].map(journal_uuid_map)

    try:
        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = connection.cursor()

        sql = """
            INSERT INTO general_ledger (
                JT, GL, BR, Statutory_GL, Prod, GL_Type, GL_Group, GL_Subtype, GL_Cat,
                Journal, GCI, GCI_Br, Company, Open_Item, File_Ref, Date, Month, Year,
                TC, Amount, Foreign_Amount, Foreign_Currency, External_Ref, MBL, IC,
                House, BC, Billing_Description, Customer_GCI, Customer_Name,
                data_Description, GL_Description, GL_Local_Description, Post_Date,
                Last_Modifier, Approver, Acct_Tran_Id, RowNumber, Commissionable, data_Source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            values = (
                'EXT',               # JT
                None,               # GL
                row.get('BR'),
                row.get('Statutory_GL'),
                None, None, None, None, None,        # Prod, GL_Type, ...
                row.get('Journal'),
                None, None, None,                    # GCI, GCI_Br, Company
                row.get('Open_Item'),
                row.get('File_Ref'),
                row.get('Date'),
                row.get('Month'),
                row.get('Year'),
                row.get('TC'),
                row.get('Amount'),
                row.get('Foreign Amount'),
                row.get('Foreign Currency'),
                None, None, None, None, None,        # External_Ref, MBL, IC, House, BC
                None, None,                          # Billing_Description, Customer_GCI
                row.get('Customer Name'),
                row.get('data_Description'),
                None, None,                          # GL_Description, GL_Local_Description
                row.get('Post_Date'),
                row.get('User_email'),               # ← Last_Modifier
                None,                                # Approver
                row.get('Acct_Tran_Id'),             # UUID per Journal
                row.get('rowNumber'),
                None, None                           # Commissionable, data_Source
            )
            cursor.execute(sql, values)

        connection.commit()
        return True, f"{len(df)} rânduri inserate în general_ledger"
    
    except Exception as e:
        connection.rollback()
        return False, f"Eroare la inserare in GL: {str(e)}"
    
    finally:
        cursor.close()
        connection.close()


def insert_into_sold_clienti(clienti_df):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    
    column_mapping = {
        'GCI': 'ï»¿GCI',
        'Company': 'Name',
        'BR': 'Branch',
        'Journal': 'Invoice',
        'File_Ref': 'File',
        'Amount': 'Balance',
        'Foreign Amount': 'Balance Foreign',
        'For currency': 'CUR',
        'Date': 'Date',
        'Month': 'Month',
        'Year': 'Year',
        'DSO': 'DSO',
        'Overdue': 'Overdue',
        'Statutory_GL': 'GL',
    }

    sold_clienti_columns = [
        'ï»¿GCI', 'Name', 'Billing Profile', 'Branch', 'Invoice', 'File', 'Total', 'Total Foreign', 'Tot CUR',
        'Balance', 'Balance Foreign', 'CUR', 'Tax Invoice', 'Date', 'Month', 'Year', 'Issue Date',
        'Tax Invoice Date', 'DSO', 'Due Date', 'Overdue', 'Close Date', 'EDI Status', 'Service Order',
        'Consol', 'curs bnr', 'reevaluare', 'diferenta de curs', 'Status', 'GL','Period_paid'
    ]

    numeric_columns = ['Total', 'Total Foreign', 'Tot CUR', 'Balance', 'Balance Foreign',
                       'DSO', 'Overdue', 'curs bnr', 'reevaluare', 'diferenta de curs']

    # Inițializare cu valori implicite
    insert_df = pd.DataFrame(index=clienti_df.index, columns=sold_clienti_columns)

    # Populăm cu valorile din dataframe-ul sursă
    for client_col, sold_col in column_mapping.items():
        if client_col in clienti_df.columns and sold_col in insert_df.columns:
            insert_df[sold_col] = clienti_df[client_col]

    # Valori implicite
    insert_df['curs bnr'] = 1
    insert_df['Status'] = 'Unpaid'
    insert_df['Consol'] = None

    # Issue Date = Date dacă există
    if 'Date' in clienti_df.columns and 'Issue Date' in insert_df.columns:
        insert_df['Issue Date'] = clienti_df['Date']

    # Completăm coloanele numerice lipsă cu 0
    for col in numeric_columns:
        if col in insert_df.columns:
            insert_df[col] = insert_df[col].apply(lambda x: 0 if pd.isna(x) or x == '' else x)

    # Completăm restul coloanelor lipsă cu ''
    insert_df.fillna('', inplace=True)

    # Înlocuim stringurile goale cu None pentru MySQL unde e nevoie
    insert_df = insert_df.replace('', None)

    # Pregătim query-ul
    placeholders = ', '.join(['%s'] * len(sold_clienti_columns))
    columns_sql = ', '.join(f"`{col}`" for col in sold_clienti_columns)
    sql = f"INSERT INTO sold_clienti ({columns_sql}) VALUES ({placeholders})"
    
    # Transformăm dataframe-ul într-o listă de tuple pentru executemany
    rows_to_insert = insert_df.to_records(index=False).tolist()

    # Executăm în bloc
    cursor = connection.cursor()
    cursor.executemany(sql, rows_to_insert)
    connection.commit()
    cursor.close()

def insert_into_sold_furnizori(clienti_df):
    try:
        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )

        column_mapping = {
            'GCI': 'ï»¿GCI',
            'Company': 'Name',
            'BR': 'Branch',
            'Journal': 'Payable',
            'File_Ref': 'File',
            'Amount': 'Balance',
            'Foreign Amount': 'Balance Foreign',
            'For currency': 'CUR',
            'Date': 'Date',
            'Month': 'Month',
            'Year': 'Year',
        
            
            'Statutory_GL': 'GL',
        }

        sold_clienti_columns = [
        'ï»¿GCI', 'Name', 'A/P Profile', 'BR', 'Product', 'Pymt Status', 'Payable', 'File',
        'Total', 'Total Foreign', 'Tot CUR', 'Balance', 'Balance Foreign', 'CUR', 'Tax Invoice',
        'Date', 'Month', 'Year', 'Issue Date', 'Due Date', 'curs bnr', 'reevaluare',
        'diferenta de curs', 'Status', 'GL', 'Period_paid'
    ]


        numeric_columns = ['Total', 'Total Foreign', 'Tot CUR', 'Balance', 'Balance Foreign',
                            'curs bnr', 'reevaluare', 'diferenta de curs']

        # Inițializare cu valori implicite
        insert_df = pd.DataFrame(index=clienti_df.index, columns=sold_clienti_columns)

        # Populăm cu valorile din dataframe-ul sursă
        for client_col, sold_col in column_mapping.items():
            if client_col in clienti_df.columns and sold_col in insert_df.columns:
                insert_df[sold_col] = clienti_df[client_col]

        # Valori implicite
        insert_df['curs bnr'] = 1
        insert_df['Status'] = 'Unpaid'
        # insert_df['Consol'] = None

        # Issue Date = Date dacă există
        if 'Date' in clienti_df.columns and 'Issue Date' in insert_df.columns:
            insert_df['Issue Date'] = clienti_df['Date']

        # Completăm coloanele numerice lipsă cu 0
        for col in numeric_columns:
            if col in insert_df.columns:
                insert_df[col] = insert_df[col].apply(lambda x: 0 if pd.isna(x) or x == '' else x)

        # Completăm restul coloanelor lipsă cu ''
        insert_df.fillna('', inplace=True)

        # Înlocuim stringurile goale cu None pentru MySQL unde e nevoie
        insert_df = insert_df.replace('', None)

        # Pregătim query-ul
        placeholders = ', '.join(['%s'] * len(sold_clienti_columns))
        columns_sql = ', '.join(f"`{col}`" for col in sold_clienti_columns)
        sql = f"INSERT INTO sold_furnizori ({columns_sql}) VALUES ({placeholders})"
        
        # Transformăm dataframe-ul într-o listă de tuple pentru executemany
        rows_to_insert = insert_df.to_records(index=False).tolist()
        cursor = connection.cursor()
        cursor.executemany(sql, rows_to_insert)
        connection.commit()
        cursor.close()
    except Exception as e:

    # Executăm în bloc
        # cursor = connection.cursor()
        # cursor.executemany(sql, rows_to_insert)
        # connection.commit()
        cursor.close()



def update_map_row(id, data):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)

    sql = """
        UPDATE mapping
        SET GL = %s,
            Br = %s,
            Statutory_GL = %s,
            Statutory_Type = %s,
            Transaction_Type = %s,
            Headers = %s
        WHERE ID = %s
    """
    values = (
        data.get('GL'),
        data.get('Br'),
        data.get('Statutory_GL'),
        data.get('Statutory_Type'),
        data.get('Transaction_Type'),
        data.get('Headers'),
        id
    )

    cursor.execute(sql, values)
    connection.commit()
    cursor.close()
    connection.close()
@views.route('/delete_map', methods=['POST'])
def delete_map():
    data = request.get_json()

    try:
        # Extrage câmpurile cheie
        gl = data.get("GL")
        br = data.get("Br")
        stat_gl = data.get("Statutory_GL")
        stat_type = data.get("Statutory_Type")
        trans_type = data.get("Transaction_Type")

        # Colectează condițiile doar pentru câmpurile prezente
        conditions = []
        params = []

        if gl is not None:
            conditions.append("GL = %s")
            params.append(gl)

        if br is not None:
            conditions.append("Br = %s")
            params.append(br)

        if stat_gl is not None:
            conditions.append("Statutory_GL = %s")
            params.append(stat_gl)

        if stat_type is not None:
            conditions.append("Statutory_Type = %s")
            params.append(stat_type)

        if trans_type is not None:
            conditions.append("Transaction_Type = %s")
            params.append(trans_type)

        # Dacă nu ai niciun câmp, nu face nimic
        if not conditions:
            return jsonify({'error': 'Trebuie cel puțin un câmp pentru a șterge'}), 400

        # Construiește WHERE dinamic
        where_clause = " AND ".join(conditions)

        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )

        with connection.cursor() as cursor:
            query = f"DELETE FROM mapping WHERE {where_clause}"
            cursor.execute(query, params)
            connection.commit()

        return jsonify({'message': 'Șters cu succes'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

def insert_map_row(data):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)
    # cursor = conn.cursor()

    sql = """
        INSERT INTO mapping (GL, Br, Statutory_GL, Statutory_Type, Transaction_Type, Headers)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (
        data.get('GL'),
        data.get('Br'),
        data.get('Statutory_GL'),
        data.get('Statutory_Type'),
        data.get('Transaction_Type'),
        data.get('Headers')
    )

    cursor.execute(sql, values)
    connection.commit()
    cursor.close()
    connection.close()
def insert_istoric_nc_rows(df):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    sql = """
        INSERT INTO istoric_nc (
            BR, Statutory_GL, Journal, Open_Item, File_Ref, Date,
            Month, Year, Amount, data_Description, Post_Date, TC,
            `Foreign Amount`, `Foreign Currency`, GCI, `Customer Name`,
            JT, rowNumber, timestamp,User_id, User_name, User_email, Acct_Tran_Id

        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
    """

    try:
        for _, row in df.iterrows():
            values = (
                row.get('BR'),
                row.get('Statutory_GL'),
                row.get('Journal'),
                row.get('Open_Item'),
                row.get('File_Ref'),
                row.get('Date'),
                row.get('Month'),
                row.get('Year'),
                row.get('Amount'),
                row.get('data_Description'),
                row.get('Post_Date'),
                row.get('TC'),
                row.get('Foreign Amount'),
                row.get('Foreign Currency'),
                row.get('GCI'),
                row.get('Customer Name'),
                row.get('JT'),
                row.get('rowNumber'),
                row.get('timestamp'),
                row.get('User_id'),
    row.get('User_name'),
    row.get('User_email'),
    row.get('Acct_Tran_Id')
            )
            cursor.execute(sql, values)

        connection.commit()
        return True, f"{len(df)} rânduri inserate cu succes în `istoric_nc`"
    except Exception as e:
        connection.rollback()
        return False, f"Eroare la inserare in istoric note contabile: {str(e)}"
    finally:
        cursor.close()
        connection.close()

@views.errorhandler(403)
def forbidden_error(error):
    code = session.get('verified_code')
    cod = session.get('cod')

    if code == cod:
        email = session.get('email')
        return render_template("403.html", email=email), 403
    else:
        return render_template('auth.html')

@views.route('/main', methods=['GET', 'POST'])
@login_required
def main():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    user = get_user_from_db(email)
    is_admin = user and user.role == 'admin'

    code = session.get('verified_code')
    cod = session.get('cod')

    if code == cod:
        return render_template('main.html', email=user.username, user_name=first_name, is_admin=is_admin)
    else:
        return render_template('auth.html')

def get_gl_from_db(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)  # Rezultatul va fi un dicționar

    query = """
        SELECT Month, Year, GL, Statutory_GL, Br, Amount
        FROM general_ledger
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
        ORDER BY Year, Month
    """
    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth

    cursor.execute(query, (start_key, end_key))
    gl = cursor.fetchall()

    cursor.close()
    connection.close()
    

    return gl 
# 






@views.route('/add_map', methods=['POST'])
def add_map():
    data = request.get_json()
    try:
        insert_map_row(data)  # inserare nouă în DB
        return jsonify({'status': 'added'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
def exists_exact_data_in_db(clienti_df, start_date, end_date):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT * FROM sold_clienti
        WHERE (`Year` * 100 + `Month`) BETWEEN %s AND %s
    """
    start_key = int(start_date.split('-')[0]) * 100 + int(start_date.split('-')[1])
    end_key = int(end_date.split('-')[0]) * 100 + int(end_date.split('-')[1])
    cursor.execute(query, (start_key, end_key))
    existing = cursor.fetchall()
    cursor.close()
    connection.close()

    if not existing:
        return False

    existing_df = pd.DataFrame(existing)
    existing_df = existing_df[clienti_df.columns.intersection(existing_df.columns)]
    clienti_comp = clienti_df[existing_df.columns]

    # Sort + Reset Index for reliable comparison
    existing_df = existing_df.sort_values(by=existing_df.columns.tolist()).reset_index(drop=True)
    clienti_comp = clienti_comp.sort_values(by=clienti_comp.columns.tolist()).reset_index(drop=True)
    

    return existing_df.equals(clienti_comp)

def generare_sold_clienti(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)  # Rezultatul va fi un dicționar

    query = """
        SELECT *
        FROM general_ledger
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
  AND JT IN ('INV', 'XINV', 'ICR')
  
        ORDER BY Year, Month
    """
    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth

    cursor.execute(query, (start_key, end_key))
    gl = cursor.fetchall()

    cursor.close()
    connection.close()
    

    return gl 
def generare_sold_furnizori(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)  # Rezultatul va fi un dicționar

    query = """
        SELECT *
        FROM general_ledger
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
  AND JT IN ('RIN', 'XRIN', 'RCR')
  
        ORDER BY Year, Month
    """
    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth

    cursor.execute(query, (start_key, end_key))
    gl = cursor.fetchall()

    cursor.close()
    connection.close()
    

    return gl 
def get_cont_tb_from_db(cont):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT DISTINCT GL
        FROM balanta_conturi
        WHERE GL LIKE %s
        ORDER BY GL
    """

    # Extrage prima cifră din `cont` și formează patternul pentru LIKE
    prima_cifra = str(cont)[0]
    like_pattern = f"{prima_cifra}%"

    cursor.execute(query, (like_pattern,))
    fisa_values = cursor.fetchall()

    cursor.close()
    connection.close()

    return fisa_values
def get_map_from_db():
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT *
        FROM mapping
        
        ORDER BY GL asc
    """

    # Extrage prima cifră din `cont` și formează patternul pentru LIKE
    

    cursor.execute(query)
    map = cursor.fetchall()

    cursor.close()
    connection.close()

    return map
def get_balanta_months():
    from collections import defaultdict
    from datetime import datetime
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    query = "SELECT DISTINCT Month, Year FROM balanta_conturi ORDER BY Year DESC, Month DESC"
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Rows from DB:", rows)  # debug

    cursor.close()
    connection.close()

    luni_disponibile = defaultdict(list)
    for month, year in rows:
        try:
            dt = datetime(year, month, 1)
            luna_nume = dt.strftime('%B')  # ex: 'June'
            an = str(year)
            luni_disponibile[an].append(luna_nume)
        except Exception as e:
            print(f"Error processing {year}-{month}: {e}")  # debug

    print("Grouped luni_disponibile:", dict(luni_disponibile))  # debug
    return dict(luni_disponibile)
def get_GL_months():
    from collections import defaultdict
    from datetime import datetime
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    query = "SELECT DISTINCT Month, Year FROM GENERAL_LEDGER ORDER BY Year DESC, Month DESC"
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Rows from DB:", rows)  # debug

    cursor.close()
    connection.close()

    luni_disponibile = defaultdict(list)
    for month, year in rows:
        try:
            dt = datetime(year, month, 1)
            luna_nume = dt.strftime('%B')  # ex: 'June'
            an = str(year)
            luni_disponibile[an].append(luna_nume)
        except Exception as e:
            print(f"Error processing {year}-{month}: {e}")  # debug

    print("Grouped luni_disponibile:", dict(luni_disponibile))  # debug
    return dict(luni_disponibile)

def get_clienti_months():
    from collections import defaultdict
    from datetime import datetime
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    query = "SELECT DISTINCT Month, Year FROM sold_clienti ORDER BY Year DESC, Month DESC"
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Rows from DB:", rows)  # debug

    cursor.close()
    connection.close()

    luni_disponibile = defaultdict(list)
    for month, year in rows:
        try:
            dt = datetime(int(year), int(month), 1)
            luna_nume = dt.strftime('%B')  # ex: 'June'
            an = str(year)
            luni_disponibile[an].append(luna_nume)
        except Exception as e:
            print(f"Error processing {year}-{month}: {e}")  # debug

    print("Grouped luni_disponibile:", dict(luni_disponibile))  # debug
    return dict(luni_disponibile)

def get_furnizori_months():
    from collections import defaultdict
    from datetime import datetime
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    query = "SELECT DISTINCT Month, Year FROM sold_furnizori ORDER BY Year DESC, Month DESC"
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Rows from DB:", rows)  # debug

    cursor.close()
    connection.close()

    luni_disponibile = defaultdict(list)
    for month, year in rows:
        try:
            dt = datetime(int(year), int(month), 1)
            luna_nume = dt.strftime('%B')  # ex: 'June'
            an = str(year)
            luni_disponibile[an].append(luna_nume)
        except Exception as e:
            print(f"Error processing {year}-{month}: {e}")  # debug

    print("Grouped luni_disponibile:", dict(luni_disponibile))  # debug
    return dict(luni_disponibile)
def months_in_interval(startMonth, startYear, endMonth, endYear):
    # Generează lista tuplelor (Year, Month) între start și end (inclusiv)
    months = []
    year, month = startYear, startMonth
    while (year < endYear) or (year == endYear and month <= endMonth):
        months.append((year, month))
        # Incrementăm luna
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    return months  
def get_tb_from_db(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)

    # 1. Verificăm lunile existente în baza de date
    query_check = """
        SELECT DISTINCT Year, Month
        FROM balanta_conturi
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
    """
    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth

    cursor.execute(query_check, (start_key, end_key))
    months_in_db = cursor.fetchall()

    # Transformăm în set pentru verificare rapidă
    months_in_db_set = set((row['Year'], row['Month']) for row in months_in_db)

    # 2. Generăm lista tuturor lunilor din interval
    months_expected = months_in_interval(startMonth, startYear, endMonth, endYear)

    # 3. Verificăm dacă toate lunile există în DB
    missing_months = [f"{m[1]:02d}/{m[0]}" for m in months_expected if m not in months_in_db_set]

    if missing_months:
        cursor.close()
        connection.close()
        mesaj = f"Lipsește balanța pentru lunile: {', '.join(missing_months)}. Va rugam sa generati intai balanta lunara pentru fiecare dintre aceste luni."
        return None, mesaj
        # raise ValueError(f"Lipsește balanța pentru lunile: {', '.join(missing_months)}")

    # 4. Dacă toate lunile sunt prezente, facem select-ul complet
    query = """
        SELECT *
        FROM balanta_conturi
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
        ORDER BY Year, Month
    """

    cursor.execute(query, (start_key, end_key))
    gl = cursor.fetchall()
    mesaj = "Balanța a fost generată cu succes."

    cursor.close()
    connection.close()

    return gl, mesaj 
def get_prev_tb_from_db(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT GL, Ending_balance, End_DC, Description
        FROM balanta_conturi
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
        ORDER BY Year, Month
    """
    # Corectez start_key pentru a nu scădea 1 direct (ex: ianuarie -> 202501-1=202500 care nu există)
    # Aici ar fi mai bine să faci logică corectă, dar păstrez ideea ta:
    prev_year, prev_month = get_previous_month(startYear, startMonth)
    prev_year_end, prev_month_end = get_previous_month(endYear, endMonth)
    start_key = prev_year * 100 + prev_month
    end_key = prev_year_end * 100 + prev_month_end

    cursor.execute(query, (start_key, end_key))
    tb_prev = cursor.fetchall()

    cursor.close()
    connection.close()

    if not tb_prev:
        mesaj = "Nu există date în balanta_conturi pentru intervalul selectat."
        return None, mesaj

    mesaj = "Datele precedente au fost încărcate cu succes."
    return tb_prev, mesaj

def get_gl_period_from_db(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)  # Rezultatul va fi un dicționar
    err=0
    query = """
        SELECT * 
        FROM general_ledger
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
        ORDER BY Year, Month
    """
    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth
    try:
        cursor.execute(query, (start_key, end_key))
        gl = cursor.fetchall()
    except Exception as e:
        print("Eroare la inserare:", e)
        connection.rollback()
        err=1
    finally:
        cursor.close()
        connection.close()

  

    return gl, err 
def get_fisa_cont_period_from_db(startMonth, startYear, endMonth, endYear, cont):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)
    err = 0

    query = """
        SELECT *
        FROM general_ledger
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
          AND Acct_Tran_Id IN (
              SELECT DISTINCT Acct_Tran_Id
              FROM general_ledger
              WHERE (Year * 100 + Month) BETWEEN %s AND %s
                AND Statutory_GL = %s
          )
    """

    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth

    try:
        cursor.execute(query, (start_key, end_key, start_key, end_key, cont))
        gl = cursor.fetchall()
    except Exception as e:
        print("Eroare la interogare:", e)
        connection.rollback()
        err = 1
        gl = []
    finally:
        cursor.close()
        connection.close()

    return gl, err

def get_sold_clienti_period_from_db(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)
    err = 0

    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth

    # Convertim endKey la format 'YYYY-MM' pentru comparare cu period_paid
    selected_period = f"{endYear}-{endMonth:02d}"

    last_day = monthrange(startYear, startMonth)[1]
    limit_date = date(startYear, startMonth, last_day)  # 2025-03-31

# Executăm query-ul
    query = """
        SELECT * 
        FROM sold_clienti
        WHERE STR_TO_DATE(CONCAT(Year, '-', LPAD(Month, 2, '0'), '-01'), '%Y-%m-%d') <= %s
        AND (Period_paid IS NULL OR Period_paid > %s)
        ORDER BY Year, Month
    """


    try:
        
        cursor.execute(query, (limit_date, limit_date))
        sold_clienti = cursor.fetchall()
        print("Sold clienti:", sold_clienti)  # Debugging line
    except Exception as e:
        print("Eroare la interogare:", e)
        connection.rollback()
        err = 1
        sold_clienti = []
    finally:
        cursor.close()
        connection.close()

    return sold_clienti, err

def get_sold_furnizori_period_from_db(startMonth, startYear, endMonth, endYear):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)  # Rezultatul va fi un dicționar
    err=0

    query = """
        SELECT * 
        FROM sold_furnizori
        WHERE (Year * 100 + Month) BETWEEN %s AND %s
        ORDER BY Year, Month
    """
    start_key = startYear * 100 + startMonth
    end_key = endYear * 100 + endMonth
    print(start_key, end_key)
    try:
        cursor.execute(query, (start_key, end_key))
        sold_furnizori = cursor.fetchall()
    except Exception as e:
        print("Eroare la interogare:", e)
        connection.rollback()
        err=1
        sold_furnizori = []
    finally:
        cursor.close()
        connection.close()
    print(err)
    print(sold_furnizori)
    return sold_furnizori, err

import mysql.connector as mysql

def actualizeaza_status_facturi_clienti(period):
    """
    Actualizează statusul facturilor din sold_clienti din 'unpaid' în 'paid',
    dacă factura apare în GL.Open_Item cu contul începând cu 411,
    și nu apare în GL.Journal pe aceeași linie.
    """
    try:
        year, month = map(int, period.split("-"))

        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = connection.cursor()

        sql = """
        UPDATE sold_clienti sc
        JOIN (
            SELECT 
                gl.Open_Item,
                MIN(gl.date) AS min_date
            FROM general_ledger gl
            WHERE gl.Statutory_GL LIKE '411%%'
              AND gl.Open_Item != gl.Journal
              AND gl.Year = %s
              AND gl.Month = %s
            GROUP BY gl.Open_Item
        ) AS sub ON sub.Open_Item = sc.Invoice
        SET 
            sc.status = 'Paid',
            sc.Period_paid = sub.min_date
        WHERE sc.status = 'Unpaid'
          AND sc.Period_paid IS NULL;
        """

        cursor.execute(sql, (year, month))
        connection.commit()
        print(f"Status actualizat pentru perioada {period}.")
        return 0

    except Exception as e:
        print("Eroare la actualizare:", e)
        connection.rollback()
        return 1

    finally:
        cursor.close()
        connection.close()



def insert_tb_df_to_db(tb_df):
    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    # Curățăm NaN
    numeric_cols = tb_df.select_dtypes(include=['number']).columns
    tb_df[numeric_cols] = tb_df[numeric_cols].fillna(0)
    tb_df = tb_df.fillna({col: '' for col in tb_df.select_dtypes(include=['object']).columns})
    err=0
    cols = tb_df.columns.tolist()
    cols_str = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))

    sql = f"INSERT INTO balanta_conturi ({cols_str}) VALUES ({placeholders})"

    values = [tuple(x) for x in tb_df.to_numpy()]

    try:
        cursor.executemany(sql, values)
        connection.commit()
    except Exception as e:
        print("Eroare la inserare:", e)
        connection.rollback()
        err=1
    finally:
        cursor.close()
        connection.close()
    return err

@views.route('/verify', methods=['GET', 'POST'])
@login_required
def verify():
    email = session.get('email')
    code = None
    if request.method == 'POST':
        user_code = request.form['code']
        cod = session.get('cod')
        if user_code == cod:
            user = get_user_from_db(email)
            print(f"User found: {user}")  # Debugging line
            login_user(user)
            code = user_code
            session['verified_code'] = code
            return redirect(url_for('views.main', email=email))
        else:
            flash('Cod incorect. Încearcă din nou.')
    return render_template('verify.html')


@views.route('/fail', methods=['GET', 'POST'])
@login_required
def fail():
    email = session.get('email')
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        return render_template('fail.html')
    else:
        return render_template('auth.html')


@views.route('/view-users')
@login_required
@admin_required
def users():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()

    if code == cod:
        return render_template('view_users.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id)
    else:
        return render_template('auth.html')


@views.route('/edit_user/<int:user_id>')
@login_required
@admin_required
def edit_user(user_id):
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        user = get_user_from_db(user_id)
        return render_template('edit_user.html', user=user)
    else:
        return render_template('auth.html')


@views.route("/update_user/<int:user_id>", methods=["POST"])
@login_required
@admin_required
def update_user(user_id):
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        username = request.form.get("username")
        role = request.form.get("role")
        if username and role:
            update_user_in_db(user_id, username, role)
        return redirect(url_for('views.users'))
    else:
        return render_template('auth.html')


@views.route('/delete-user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    user = Users.query.get(user_id)
    if not user:
        return jsonify({"error": "Userul nu a fost gasit."}), 404

    try:
        db.session.delete(user)
        db.session.commit()
        return jsonify({"success": True, "message": "Userul a fost sters cu succes."}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Eroare stergere user: {str(e)}"}), 500

# UPLOAD_FOLDER = r'C:\Dezvoltare\RAS/RAS Expeditors/uploads'  # Define the upload folder
# UPLOAD_FOLDER = 'C:/Dezvoltare/RAS/RAS Expeditors/uploads'  # Define the upload folder
# if not os.path.exists(UPLOAD_FOLDER):
#     os.makedirs(UPLOAD_FOLDER)

@views.route('/load_transform', methods=['GET', 'POST'])
# @login_required
# @admin_required
def load_transform():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')
    luni_disponibile_gl = get_GL_months()
    luni_disponibile=get_balanta_months()
    luni_disponibile_clienti = get_clienti_months()
    luni_disponibile_furnizori = get_furnizori_months()
    user = get_user_from_db(email)
    users_list = get_all_users()
    map=get_map_from_db()
    map_df=pd.DataFrame(map)
    map_data = map_df.to_dict(orient='records')  # => list of dicts
    # print(map_data)


    if request.method == 'POST':
        if 'file' not in request.files:
            print("No file part in the request")  # Debugging
            return jsonify({'message': 'No file part'}), 400

        file = request.files['file']
        print(file, 'fileee---')

        if file.filename == '':
            print("No file selected")  # Debugging
            return jsonify({'message': 'No selected file'}), 400

        if file:
            try:
                filename = file.filename
                print(filename)
                filepath = os.path.join(UPLOAD_FOLDER, filename)  # Use os.path.join
                file.save(filepath)

                print(f"File saved to: {filepath}")  # Debugging
                return jsonify({'message': 'File upload successful.'}), 200 # No task started here

            except Exception as e:
                print(f"Error processing file: {e}")  # Detailed error logging
                return jsonify({'message': str(e)}), 500

    if code == cod:
        return render_template('load_transform.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id, map_df=map_data, luni_disponibile=luni_disponibile, luni_disponibile_gl=luni_disponibile_gl, luni_disponibile_clienti=luni_disponibile_clienti, luni_disponibile_furnizori=luni_disponibile_furnizori)
    else:
        return render_template('auth.html')
@views.route('/get-journal-seq')
def get_journal_seq():
    from flask import request, jsonify

    date_str = request.args.get('date')  # format: YYYY-MM-DD
    if not date_str:
        return jsonify({'seq': 1})

    year, month, day = date_str.split('-')
    prefix = f"EE{year[-2:]}{month}{day}"  # ex: EE250806

    connection = mysql.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor()

    # Caută doar Journal care încep cu prefixul din acea zi
    cursor.execute(
        "SELECT Journal FROM general_ledger WHERE Journal LIKE %s ORDER BY Journal DESC LIMIT 1",
        (f"{prefix}%",)
    )
    row = cursor.fetchone()

    cursor.close()
    connection.close()

    if row and row[0]:
        try:
            last_seq = int(row[0][-3:])  # ultimele 3 caractere
            return jsonify({'seq': last_seq + 1})
        except ValueError:
            pass  # în caz că nu e număr
    return jsonify({'seq': 1})

@views.route('/genereaza_fisa', methods=['POST'])
def genereaza_fisa():
    conturi6 = request.form.getlist('conturi6[]')
    conturi7 = request.form.getlist('conturi7[]')

    start_date = request.form.get('start-date')   # din input name="start-date"
    end_date = request.form.get('end-date')       # din input name="end-date"
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))
    email = session.get('email')
    first_name = extract_name_from_email(email)

    print("Conturi 6 selectate:", conturi6)
    print("Conturi 7 selectate:", conturi7)
    print("Perioada selectată:", start_date, "→", end_date)
    rezultate = []
    atasamente = []

    # Dacă lista conturi6 nu e goală
    if conturi6 and not ('select_all' in conturi6):
        for cont in conturi6:
            rezultat,err = get_fisa_cont_period_from_db(start_month, start_year, end_month, end_year, cont)
            print(rezultat)
            if rezultat == ([], 0)or rezultat==[]:   
                flash(f"Nu există date pentru contul {cont} în perioada selectată.", "warning")
                return jsonify({'status': 'no_data'})
                
            else:
                rezultat_df= pd.DataFrame(rezultat)
                fisier_path = os.path.join(TEMP_FOLDER, f"Fisa_cont_{cont}_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx")
                rezultat_df.to_excel(fisier_path, index=False)
                atasamente.append(fisier_path)
                flash(f"Fișa pentru contul {cont} a fost generată cu succes! Veti primi rezulatul pe e-mail!", "success")
                # return jsonify({'status': 'success'})
                

    # La fel pentru conturi7
    if conturi7 and not ('select_all' in conturi7):
        for cont in conturi7:
            rezultat,err = get_fisa_cont_period_from_db(start_month, start_year, end_month, end_year, cont)
            print(rezultat,"----------")
            if rezultat == ([], 0) or rezultat==[]:   
                flash(f"Nu există date pentru contul {cont} în perioada selectată.", "warning")
                return jsonify({'status': 'no_data'})
                
            else:
                fisier_path = os.path.join(TEMP_FOLDER, f"Fisa_cont_{cont}_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx")
                rezultat_df = pd.DataFrame(rezultat)
                rezultat_df.to_excel(fisier_path, index=False)
                atasamente.append(fisier_path)
                
                flash(f"Fișa pentru contul {cont} a fost generată cu succes! Veti primi rezulatul pe e-mail!", "success")
                # return jsonify({'status': 'success'})
    # print("Rezultate extrase din baza:", rezultate)
    if atasamente:
        
        zip_path = os.path.join(TEMP_FOLDER, "fise_conturi_selectate.zip")
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for file_path in atasamente:
                zipf.write(file_path, arcname=os.path.basename(file_path))
            
        trimitereFilesMail(first_name,email, zip_path)
    return jsonify({'status': 'success'})
    # return redirect(url_for('views.generate_reports_processing'))  # Redirect to the report generation page
    # Aici poți procesa mai departe, returna pagină, genera PDF, etc.
    return "Fișa generată! Verifică consola serverului pentru output."

@views.route('/genereaza_sold_clienti', methods=['POST'])
def genereaza_sold_clienti():
    start_date = request.form.get('start-dateCl')
    end_date = request.form.get('end-dateCl')
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))

    email = session.get('email')
    first_name = extract_name_from_email(email)

    atasamente = []

    clienti = generare_sold_clienti(start_month, start_year, end_month, end_year)
    if not clienti:
        flash("Nu s-au găsit date pentru perioada selectată. Va rugam sa importati GL pentru aceasta perioada", "warning")
        return jsonify({'status': 'no_data'})

    clienti_df = pd.DataFrame(clienti)
    gl_map = (
        clienti_df[clienti_df['GL_Type'] == 'ASSET']
        # doar dacă vrei să sari peste cele goale
        .groupby('GCI')['Statutory_GL']
        .first()
    )

    # 2. Suprascriem toate valorile din Statutory_GL, dacă GCI-ul are un GL asociat în map
    clienti_df['Statutory_GL'] = clienti_df.apply(
        lambda row: gl_map[row['GCI']] if row['GCI'] in gl_map else row['Statutory_GL'],
        axis=1
    )

    # 3. (Opțional) Ștergem linia cu GL_Type == 'ASSET'
    clienti_df = clienti_df[clienti_df['GL_Type'] != 'ASSET']
    # clienti_df= clienti_df.loc[~clienti_df['GL_Type'].astype(str).str.contains("ASSET")]
    clienti_df["Amount"]=clienti_df["Amount"]*-1
    clienti_df["Foreign_Amount"] = clienti_df["Foreign_Amount"].apply(lambda x: x * -1 if pd.notnull(x) else x)
    
    clienti_df.to_excel(os.path.join(TEMP_FOLDER, f"Clienti_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
    required_cols = ['Statutory_GL', 'Customer_GCI', 'Customer_Name', 'Amount', 'Foreign_Amount', 'Foreign_Currency']
    if exists_exact_data_in_db(clienti_df, start_date, end_date):
        return jsonify({'status': 'already_exists', 'message': 'Datele pentru această perioadă au fost deja importate și sunt identice.'})

    insert_into_sold_clienti(clienti_df)
    actualizeaza_status_facturi_clienti(start_date)
    
    if not all(col in clienti_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in clienti_df.columns]
        raise ValueError(f"Lipsesc coloane necesare: {missing}")
    # clienti=genereaza_raport_sold_clienti(start_month, start_year, end_month, end_year)
    # clienti_df=pd.DataFrame(clienti)
    # unique_gls = clienti_df['Statutory_GL'].dropna().unique()

    # for gl in unique_gls:
    #     gl_df = clienti_df[clienti_df['Statutory_GL'] == gl].copy()

    #     # Pivot extins pe GCI + Customer_Name
    #     pivot = gl_df.pivot_table(
    #         index=['Customer_GCI', 'Customer_Name'],
    #         values=['Amount', 'Foreign_Amount'],
    #         aggfunc='sum'
    #     ).reset_index()

    #     pivot['Statutory_GL'] = gl

    #     # Detectăm valută unică sau MULTI
    #             # Pivot extins pe Customer_Name + Customer_GCI
    #     pivot = gl_df.pivot_table(
    #         index=['Customer_GCI', 'Customer_Name'],
    #         values=['Amount', 'Foreign_Amount'],
    #         aggfunc='sum'
    #     ).reset_index()

    #     # Asigurăm coloanele necesare
    #     for col in ['Amount', 'Foreign_Amount']:
    #         if col not in pivot.columns:
    #             pivot[col] = 0.0

    #     pivot['Statutory_GL'] = gl

    #     # Mapare valute per client
    #     currency_map = (
    #         gl_df.groupby('Customer_Name')['Foreign_Currency']
    #         .apply(lambda x: ', '.join(sorted(x.dropna().unique())))
    #         .fillna('RON')
    #         .to_dict()
    #     )
    #     pivot['Foreign_Currency'] = pivot['Customer_Name'].map(currency_map).fillna('RON')
    #     pivot['Foreign_Currency']=pivot["Foreign_Currency"].apply(lambda x: x if x != '' else 'RON')
    #     # Reordonare finală
    #     pivot = pivot[['Statutory_GL', 'Customer_GCI', 'Customer_Name', 'Amount', 'Foreign_Amount', 'Foreign_Currency']]


    #     # Salvare fișier Excel
    #     file_name = os.path.join(TEMP_FOLDER, f"Sold_clienti_{gl}_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx")

    #     with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
    #         gl_df.to_excel(writer, index=False, sheet_name='Date')
    #         pivot.to_excel(writer, index=False, sheet_name='Pivot_Clienti')

    #         workbook = writer.book
    #         worksheet = writer.sheets['Pivot_Clienti']

    #         # Formatare
    #         header_format = workbook.add_format({
    #             'bold': True, 'bg_color': '#BDD7EE',
    #             'border': 1, 'align': 'center'
    #         })
    #         currency_format = workbook.add_format({'num_format': '#,##0.00', 'align': 'right'})
    #         text_format = workbook.add_format({'align': 'left'})

    #         for col_num, value in enumerate(pivot.columns):
    #             worksheet.write(0, col_num, value, header_format)
    #             max_width = max(pivot[value].astype(str).map(len).max(), len(value)) + 2
    #             if value in ['Amount', 'Foreign_Amount']:
    #                 worksheet.set_column(col_num, col_num, max_width, currency_format)
    #             else:
    #                 worksheet.set_column(col_num, col_num, max_width, text_format)

    #     atasamente.append(file_name)
    # actualizeaza_status_facturi_clienti()

    # # ZIP și e-mail
    # if atasamente:
    #     zip_path = os.path.join(TEMP_FOLDER, f"Sold_clienti_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.zip")
    #     with zipfile.ZipFile(zip_path, 'w') as zipf:
    #         for path in atasamente:
    #             zipf.write(path, arcname=os.path.basename(path))

    #     try:
    #         trimitereFilesMail(first_name, email, zip_path)
    #         flash("Fișierele au fost trimise pe e-mail.", "info")
    #     except Exception as e:
    #         print("Eroare trimitere e-mail:", e)
    #         flash("A apărut o eroare la trimiterea e-mailului.", "danger")

    return jsonify({'status': 'success'})

@views.route('/genereaza_raport_sold_clienti', methods=['POST'])
def genereaza_raport_sold_clienti():
    start_date = request.form.get('start-dateCl')
    end_date = request.form.get('end-dateCl')
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))

    email = session.get('email')
    first_name = extract_name_from_email(email)

    atasamente = []

    clienti,err = get_sold_clienti_period_from_db(start_month, start_year, end_month, end_year)
    if not clienti:
        flash("Nu s-au găsit date pentru perioada selectată. Va rugam sa importati GL pentru aceasta perioada", "warning")
        return jsonify({'status': 'no_data'})
    if err:
        flash("Ceva nu a mers bine!", "warning")
        return jsonify({'status': 'no_data'})

    clienti_df = pd.DataFrame(clienti)
    # print(clienti_df)
    clienti_df["Balance Foreign"] = clienti_df["Balance Foreign"].fillna(0)
    # gl_map = (
    #     clienti_df[clienti_df['GL_Type'] == 'ASSET']
    #     # doar dacă vrei să sari peste cele goale
    #     .groupby('GCI')['Statutory_GL']
    #     .first()
    # )

    # # 2. Suprascriem toate valorile din Statutory_GL, dacă GCI-ul are un GL asociat în map
    # clienti_df['Statutory_GL'] = clienti_df.apply(
    #     lambda row: gl_map[row['GCI']] if row['GCI'] in gl_map else row['Statutory_GL'],
    #     axis=1
    # )

    # # 3. (Opțional) Ștergem linia cu GL_Type == 'ASSET'
    # clienti_df = clienti_df[clienti_df['GL_Type'] != 'ASSET']
    # clienti_df= clienti_df.loc[~clienti_df['GL_Type'].astype(str).str.contains("ASSET")]
    # clienti_df.to_excel(os.path.join(TEMP_FOLDER, f"Clienti_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
    # required_cols = ['Statutory_GL', 'Customer_GCI', 'Customer_Name', 'Amount', 'Foreign_Amount', 'Foreign_Currency']
    # if exists_exact_data_in_db(clienti_df, start_date, end_date):
    #     return jsonify({'status': 'already_exists', 'message': 'Datele pentru această perioadă au fost deja importate și sunt identice.'})

    # insert_into_sold_clienti(clienti_df)
    # if not all(col in clienti_df.columns for col in required_cols):
    #     missing = [col for col in required_cols if col not in clienti_df.columns]
    #     raise ValueError(f"Lipsesc coloane necesare: {missing}")

    unique_gls = clienti_df['GL'].dropna().unique()
    clienti_df['Balance Foreign'] = pd.to_numeric(clienti_df['Balance Foreign'], errors='coerce').fillna(0)

    for gl in unique_gls:
        gl_df = clienti_df[clienti_df['GL'] == gl].copy()

        # Pivot extins pe GCI + Customer_Name
        pivot = gl_df.pivot_table(
            index=['ï»¿GCI', 'Name'],
            values=['reevaluare', 'Balance Foreign'],
            aggfunc='sum'
        ).reset_index()

        pivot['GL'] = gl

        # Detectăm valută unică sau MULTI
                # Pivot extins pe Name + Customer_GCI
        pivot = gl_df.pivot_table(
            index=['ï»¿GCI', 'Name'],
            values=['reevaluare'],
            aggfunc='sum'
        ).reset_index()

        # Asigurăm coloanele necesare
        for col in ['reevaluare']:
            if col not in pivot.columns:
                pivot[col] = 0.0

        pivot['GL'] = gl

        # Mapare valute per client
        currency_map = (
            gl_df.groupby('Name')['CUR']
            .apply(lambda x: ', '.join(sorted(x.dropna().unique())))
            .fillna('RON')
            .to_dict()
        )
        pivot['CUR'] = pivot['Name'].map(currency_map).fillna('RON')
        pivot['CUR']=pivot["CUR"].apply(lambda x: x if x != '' else 'RON')
        # Reordonare finală
        pivot = pivot[['GL', 'ï»¿GCI', 'Name', 'reevaluare', 'CUR']]


        # Salvare fișier Excel
        file_name = os.path.join(TEMP_FOLDER, f"Sold_clienti_{gl}_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx")

        with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
            gl_df.to_excel(writer, index=False, sheet_name='Date')
            pivot.to_excel(writer, index=False, sheet_name='Pivot_Clienti')

            workbook = writer.book
            worksheet = writer.sheets['Pivot_Clienti']

            # Formatare
            header_format = workbook.add_format({
                'bold': True, 'bg_color': '#BDD7EE',
                'border': 1, 'align': 'center'
            })
            currency_format = workbook.add_format({'num_format': '#,##0.00', 'align': 'right'})
            text_format = workbook.add_format({'align': 'left'})

            for col_num, value in enumerate(pivot.columns):
                worksheet.write(0, col_num, value, header_format)
                max_width = max(pivot[value].astype(str).map(len).max(), len(value)) + 2
                if value in ['reevaluare', 'Balance Foreign']:
                    worksheet.set_column(col_num, col_num, max_width, currency_format)
                else:
                    worksheet.set_column(col_num, col_num, max_width, text_format)

        atasamente.append(file_name)

    # ZIP și e-mail
    if atasamente:
        zip_path = os.path.join(TEMP_FOLDER, f"Sold_clienti_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.zip")
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for path in atasamente:
                zipf.write(path, arcname=os.path.basename(path))

        try:
            trimitereFilesMail(first_name, email, zip_path)
            flash("Fișierele au fost trimise pe e-mail.", "info")
        except Exception as e:
            print("Eroare trimitere e-mail:", e)
            flash("A apărut o eroare la trimiterea e-mailului.", "danger")

    return jsonify({'status': 'success'})
@views.route('/genereaza_sold_furnizori', methods=['POST'])
def genereaza_sold_furnizori():
    start_date = request.form.get('start-dateFz')
    end_date = request.form.get('end-dateFz')
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))

    email = session.get('email')
    first_name = extract_name_from_email(email)

    atasamente = []

    clienti = generare_sold_furnizori(start_month, start_year, end_month, end_year)
    if not clienti:
        flash("Nu s-au găsit date pentru perioada selectată. Va rugam sa importati GL pentru aceasta perioada", "warning")
        return jsonify({'status': 'no_data'})

    clienti_df = pd.DataFrame(clienti)
    gl_map = (
        clienti_df[clienti_df['GL_Type'] == 'ASSET']
        # doar dacă vrei să sari peste cele goale
        .groupby('GCI')['Statutory_GL']
        .first()
    )

    # 2. Suprascriem toate valorile din Statutory_GL, dacă GCI-ul are un GL asociat în map
    clienti_df['Statutory_GL'] = clienti_df.apply(
        lambda row: gl_map[row['GCI']] if row['GCI'] in gl_map else row['Statutory_GL'],
        axis=1
    )

    # 3. (Opțional) Ștergem linia cu GL_Type == 'ASSET'
    clienti_df = clienti_df[clienti_df['GL_Type'] != 'ASSET']
    clienti_df= clienti_df.loc[~clienti_df['GL_Type'].astype(str).str.contains("ASSET")]
    clienti_df.to_excel(os.path.join(TEMP_FOLDER, f"Furnizori_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
    required_cols = ['Statutory_GL', 'Customer_GCI', 'Customer_Name', 'Amount', 'Foreign_Amount', 'Foreign_Currency']
    if exists_exact_data_in_db(clienti_df, start_date, end_date):
        return jsonify({'status': 'already_exists', 'message': 'Datele pentru această perioadă au fost deja importate și sunt identice.'})

    insert_into_sold_furnizori(clienti_df)
    
    if not all(col in clienti_df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in clienti_df.columns]
        raise ValueError(f"Lipsesc coloane necesare: {missing}")
    # clienti=genereaza_raport_sold_clienti(start_month, start_year, end_month, end_year)
    # clienti_df=pd.DataFrame(clienti)
    # unique_gls = clienti_df['Statutory_GL'].dropna().unique()

    # for gl in unique_gls:
    #     gl_df = clienti_df[clienti_df['Statutory_GL'] == gl].copy()

    #     # Pivot extins pe GCI + Customer_Name
    #     pivot = gl_df.pivot_table(
    #         index=['Customer_GCI', 'Customer_Name'],
    #         values=['Amount', 'Foreign_Amount'],
    #         aggfunc='sum'
    #     ).reset_index()

    #     pivot['Statutory_GL'] = gl

    #     # Detectăm valută unică sau MULTI
    #             # Pivot extins pe Customer_Name + Customer_GCI
    #     pivot = gl_df.pivot_table(
    #         index=['Customer_GCI', 'Customer_Name'],
    #         values=['Amount', 'Foreign_Amount'],
    #         aggfunc='sum'
    #     ).reset_index()

    #     # Asigurăm coloanele necesare
    #     for col in ['Amount', 'Foreign_Amount']:
    #         if col not in pivot.columns:
    #             pivot[col] = 0.0

    #     pivot['Statutory_GL'] = gl

    #     # Mapare valute per client
    #     currency_map = (
    #         gl_df.groupby('Customer_Name')['Foreign_Currency']
    #         .apply(lambda x: ', '.join(sorted(x.dropna().unique())))
    #         .fillna('RON')
    #         .to_dict()
    #     )
    #     pivot['Foreign_Currency'] = pivot['Customer_Name'].map(currency_map).fillna('RON')
    #     pivot['Foreign_Currency']=pivot["Foreign_Currency"].apply(lambda x: x if x != '' else 'RON')
    #     # Reordonare finală
    #     pivot = pivot[['Statutory_GL', 'Customer_GCI', 'Customer_Name', 'Amount', 'Foreign_Amount', 'Foreign_Currency']]


    #     # Salvare fișier Excel
    #     file_name = os.path.join(TEMP_FOLDER, f"Sold_clienti_{gl}_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx")

    #     with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
    #         gl_df.to_excel(writer, index=False, sheet_name='Date')
    #         pivot.to_excel(writer, index=False, sheet_name='Pivot_Clienti')

    #         workbook = writer.book
    #         worksheet = writer.sheets['Pivot_Clienti']

    #         # Formatare
    #         header_format = workbook.add_format({
    #             'bold': True, 'bg_color': '#BDD7EE',
    #             'border': 1, 'align': 'center'
    #         })
    #         currency_format = workbook.add_format({'num_format': '#,##0.00', 'align': 'right'})
    #         text_format = workbook.add_format({'align': 'left'})

    #         for col_num, value in enumerate(pivot.columns):
    #             worksheet.write(0, col_num, value, header_format)
    #             max_width = max(pivot[value].astype(str).map(len).max(), len(value)) + 2
    #             if value in ['Amount', 'Foreign_Amount']:
    #                 worksheet.set_column(col_num, col_num, max_width, currency_format)
    #             else:
    #                 worksheet.set_column(col_num, col_num, max_width, text_format)

    #     atasamente.append(file_name)
    # actualizeaza_status_facturi_clienti()

    # # ZIP și e-mail
    # if atasamente:
    #     zip_path = os.path.join(TEMP_FOLDER, f"Sold_clienti_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.zip")
    #     with zipfile.ZipFile(zip_path, 'w') as zipf:
    #         for path in atasamente:
    #             zipf.write(path, arcname=os.path.basename(path))

    #     try:
    #         trimitereFilesMail(first_name, email, zip_path)
    #         flash("Fișierele au fost trimise pe e-mail.", "info")
    #     except Exception as e:
    #         print("Eroare trimitere e-mail:", e)
    #         flash("A apărut o eroare la trimiterea e-mailului.", "danger")

    return jsonify({'status': 'success'})
@views.route('/genereaza_raport_sold_furnizori', methods=['POST'])
def genereaza_raport_sold_furnizori():
    start_date = request.form.get('start-dateFz')
    end_date = request.form.get('end-dateFz')
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))

    email = session.get('email')
    first_name = extract_name_from_email(email)

    atasamente = []

    clienti,err = get_sold_furnizori_period_from_db(start_month, start_year, end_month, end_year)
    if not clienti:
        flash("Nu s-au găsit date pentru perioada selectată. Va rugam sa importati GL pentru aceasta perioada", "warning")
        return jsonify({'status': 'no_data'})
    if err:
        flash("Ceva nu a mers bine!", "warning")
        return jsonify({'status': 'no_data'})

    clienti_df = pd.DataFrame(clienti)
    clienti_df["Balance Foreign"] = clienti_df["Balance Foreign"].fillna(0)


    unique_gls = clienti_df['GL'].dropna().unique()

    for gl in unique_gls:
        gl_df = clienti_df[clienti_df['GL'] == gl].copy()

        # Pivot extins pe GCI + Customer_Name
        pivot = gl_df.pivot_table(
            index=['ï»¿GCI', 'Name'],
            values=['reevaluare', 'Balance Foreign'],
            aggfunc='sum'
        ).reset_index()

        pivot['GL'] = gl

        # Detectăm valută unică sau MULTI
                # Pivot extins pe Name + Customer_GCI
        pivot = gl_df.pivot_table(
            index=['ï»¿GCI', 'Name'],
            values=['reevaluare'],
            aggfunc='sum'
        ).reset_index()

        # Asigurăm coloanele necesare
        for col in ['reevaluare']:
            if col not in pivot.columns:
                pivot[col] = 0.0

        pivot['GL'] = gl

        # Mapare valute per client
        currency_map = (
            gl_df.groupby('Name')['CUR']
            .apply(lambda x: ', '.join(sorted(x.dropna().unique())))
            .fillna('RON')
            .to_dict()
        )
        pivot['CUR'] = pivot['Name'].map(currency_map).fillna('RON')
        pivot['CUR']=pivot["CUR"].apply(lambda x: x if x != '' else 'RON')
        # Reordonare finală
        pivot = pivot[['GL', 'ï»¿GCI', 'Name', 'reevaluare', 'CUR']]


        # Salvare fișier Excel
        file_name = os.path.join(TEMP_FOLDER, f"Sold_furnizori_{gl}_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx")

        with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
            gl_df.to_excel(writer, index=False, sheet_name='Date')
            pivot.to_excel(writer, index=False, sheet_name='Pivot_furnizori')

            workbook = writer.book
            worksheet = writer.sheets['Pivot_furnizori']

            # Formatare
            header_format = workbook.add_format({
                'bold': True, 'bg_color': '#BDD7EE',
                'border': 1, 'align': 'center'
            })
            currency_format = workbook.add_format({'num_format': '#,##0.00', 'align': 'right'})
            text_format = workbook.add_format({'align': 'left'})

            for col_num, value in enumerate(pivot.columns):
                worksheet.write(0, col_num, value, header_format)
                max_width = max(pivot[value].astype(str).map(len).max(), len(value)) + 2
                if value in ['reevaluare', 'Balance Foreign']:
                    worksheet.set_column(col_num, col_num, max_width, currency_format)
                else:
                    worksheet.set_column(col_num, col_num, max_width, text_format)

        atasamente.append(file_name)

    # ZIP și e-mail
    if atasamente:
        zip_path = os.path.join(TEMP_FOLDER, f"Sold_furnizori_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.zip")
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for path in atasamente:
                zipf.write(path, arcname=os.path.basename(path))

        try:
            trimitereFilesMail(first_name, email, zip_path)
            flash("Fișierele au fost trimise pe e-mail.", "info")
        except Exception as e:
            print("Eroare trimitere e-mail:", e)
            flash("A apărut o eroare la trimiterea e-mailului.", "danger")
            return jsonify({'status': 'success'})

    return jsonify({'status': 'success'})
@views.route('/generate_reports', methods=['GET', 'POST'])
# @login_required
# @admin_required
def generate_reports_processing():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')
    values_6= get_cont_tb_from_db("6")
    values_7= get_cont_tb_from_db("7")
    
    user = get_user_from_db(email)
    users_list = get_all_users()
    fisier=None
    nume_export=None

    if request.method == 'POST':
        start_date = request.form.get('start-date')  # format: '2025-06'
        end_date = request.form.get('end-date')      # format: '2025-08'
        action = request.form.get('action')          # 'tb' sau 'fisa'
        values_6= get_cont_tb_from_db("6")
        values_7= get_cont_tb_from_db("7")
        if not start_date or not end_date:
            flash("Te rugăm să selectezi atât perioada de început, cât și cea de sfârșit.", "warning")
            print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            return redirect(url_for('views.generate_reports_processing'))

        # Extragem luna și anul din stringuri
        start_year, start_month = map(int, start_date.split('-'))
        end_year, end_month = map(int, end_date.split('-'))
        print(f"Start: {start_month}/{start_year}, End: {end_month}/{end_year}, Action: {action}")

        if action == 'tb':
            # Generează Balanta
            try:
                tb, mesaj= get_tb_from_db(start_month, start_year, end_month, end_year)
                if "succes" in mesaj:
                
                    flash(mesaj, "success")
                    fisier=tb
                    nume_export= f"Balanta_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"
                else:
                    flash(mesaj, "danger")# mesaj de succes
            except Exception as e:
                flash(mesaj, "danger")
                # return redirect(url_for('views.generate_reports_processing'))
            
        elif action == 'fisa':
            # Generează Fișa de cont
            nume_export= f"Fisa_cont_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"
            values_6= get_cont_tb_from_db("6")
            values_7= get_cont_tb_from_db("7")
        elif action == 'cl':
    # Generează Sold clienți
            clienti = generare_sold_clienti(start_month, start_year, end_month, end_year)
            
            clienti_df = pd.DataFrame(clienti)

            # Verificare rapidă coloane esențiale
            required_cols = ['Statutory_GL', 'Customer', 'Amount']
            if not all(col in clienti_df.columns for col in required_cols):
                raise ValueError(f"Coloanele necesare lipsesc: {required_cols}")

            # Grupare după Statutory_GL
            unique_gls = clienti_df['Statutory_GL'].dropna().unique()

            for gl in unique_gls:
                gl_df = clienti_df[clienti_df['Statutory_GL'] == gl]

                # Pivot: sumă Amount pe Customer
                pivot = gl_df.pivot_table(index='Customer', values='Amount', aggfunc='sum').reset_index()

                # Nume fișier și salvare
                file_name = f"{TEMP_FOLDER}/Sold_clienti_{gl}_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"
                with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
                    gl_df.to_excel(writer, index=False, sheet_name='Date')
                    pivot.to_excel(writer, index=False, sheet_name='Pivot_Clienti')

                    # values_6= get_cont_tb_from_db("411")
                    # values_7= get_cont_tb_from_db("7")
                    
                    # return f"Generez fișa de cont de la {start_month}/{start_year} până la {end_month}/{end_year}" 
        elif action == 'gl':
            # Generează Fișa de cont
            gl,err= get_gl_period_from_db(start_month, start_year, end_month, end_year)
            if err==1:
                flash("Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
            # print(gl)
            gl_df= pd.DataFrame(gl)
            gl_df.to_excel(os.path.join(TEMP_FOLDER, f"GL_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
            # gl.to_excel('D:\\Projects\\35. GIT RAS\\RAS_Expeditors\\temp\\REGISTRU JURNAL.xlsx', index=False)  # Salvează DataFrame-ul în Excel
            gl_preview = gl[:100]
            fisier=gl_preview
            nume_export=f"GL_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"
            
            
            flash("GL afisat cu succes! Pentru download click export pe tabelul de mai jos.", "success")
                
            # return f"Generez fișa de cont de la {start_month}/{start_year} până la {end_month}/{end_year}" 
        

    if code == cod:
        return render_template('vizualizare_rapoarte.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id, fisier=fisier, nume_export=nume_export, values_6=values_6, values_7=values_7)
    else:
        return render_template('auth.html')
@views.route('/trimite-mail-export', methods=['POST'])
def trimite_mail_export():
    if 'file' not in request.files:
        return jsonify(status="error", message="Fișier lipsă.")
    email = session.get('email')
    first_name = extract_name_from_email(email)
    file = request.files['file']
    filepath = os.path.join("temp", file.filename)
    if not "GL" in file.filename:
        
        file.save(filepath)
        df = pd.read_excel(filepath, dtype={'GL': str})
        df = df.drop(columns=[col for col in ['#', 'id'] if col in df.columns])
        df["GL"] = df["GL"].astype(str).str.replace("'","") # Asigură că GL este un string cu 6 cifre

        output_path = filepath
                
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Trial Balance', index=False)

            workbook = writer.book
            # worksheet = # Ia primul worksheet creat (dacă nu știi numele)
            worksheet = list(writer.sheets.values())[0]
    # ✅ corectat aici pentru a accesa foaia activă

            # ✅ Header format cu borduri
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'middle',
                'bg_color': '#E0E0E0',
                'border': 1  # ✅ doar aici bordură
            })

            # 🔢 Format numeric simplu și rapid
            money_format = workbook.add_format({
                'num_format': '#,##0.00',
                'align': 'right'
            })

            # 🔤 Format text
            text_format = workbook.add_format({
                'align': 'left'
            })

            # 🔁 D/C centrat
            dc_format = workbook.add_format({
                'align': 'center'
            })

            worksheet.freeze_panes(1, 0)

            numeric_cols = ["MTD_Debit", "MTD_Credit", "YTD_Debit", "YTD_Credit", "Opening_balance", "Ending_balance"]

            for col_num, column in enumerate(df.columns):
                width = 15
                fmt = text_format

                if column in ["GL", "Description"]:
                    width = 25
                elif column in ["Open_DC", "End_DC"]:
                    width = 8
                    fmt = dc_format
                elif column in ["Month", "Year"]:
                    width = 8
                elif column in numeric_cols:
                    width = 18
                    fmt = money_format

                worksheet.set_column(col_num, col_num, width, fmt)
                worksheet.write(0, col_num, column, header_format)  # ✅ header cu bordură
    # Apelezi funcția ta de trimitere email
    
    try:
        trimitereFilesMail(first_name,email, filepath)
        return jsonify(status="success")
    except Exception as e:
        return jsonify(status="error", message=str(e))
@views.route('/generate_tb', methods=['GET', 'POST'])
# @login_required
# @admin_required
def generate_monthlyTB():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()
    tb_data=None
    luni_disponibile = get_balanta_months()
    print(luni_disponibile)
    nume_export=None
    # luni_pe_ani = get_luni_pe_ani([f"{luna['Month']:02d} {luna['Year']}" for luna in luni_disponibile])
    

    if request.method == 'POST':
        
        
        if 'enable-period' in request.form:
        # Checkbox bifat
        # Aici preiei datele pentru perioada start/stop
            start = request.form.get('period-start')
            end = request.form.get('period-end')
            start_year, start_month = map(int, start.split('-'))
            end_year, end_month = map(int, end.split('-'))
            # print(f"Start: {start_month}/{start_year}, End: {end_month}/{end_year}, Action: {action}")
            tb, mesaj = get_tb_from_db(start_month, start_year, end_month, end_year)
    
            if tb is None or len(tb) == 0:
                flash(mesaj, "danger")  # mesaj cu lunile lipsă
            else:
                flash(mesaj, "success")  # mesaj de succes
                tb_df = pd.DataFrame(tb)
                tb_df['GL']=tb_df['GL'].astype(str)
                tb_data = tb_df.to_dict(orient="records")
                nume_export= f"Balanta_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"

        # procesezi balanța pe perioadă
        else:
        # Checkbox nebifat
        # Preiei luna simplă
            luna = request.form.get('start-date')
            start_date = datetime.strptime(str(luna), "%Y-%m")
            luna_cifra = start_date.month  # întoarce un int: 6 pentru Iunie
            # procesezi balanța lunară
            start_date = request.form.get('start-date')  # format: '2025-06'
            end_date = start_date      # format: '2025-08'
            # action = request.form.get('action')          # 'tb' sau 'fisa'
            if not start_date or not end_date:
                flash("Te rugăm să selectezi atât perioada de început, cât și cea de sfârșit.", "warning")
                print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                return redirect(url_for('views.generate_monthlyTB'))

            # Extragem luna și anul din stringuri
            start_year, start_month = map(int, start_date.split('-'))
            end_year, end_month = map(int, end_date.split('-'))
            prev_year, prev_month = get_previous_month(start_year, start_month)
            prev_year_end, prev_month_end = get_previous_month(end_year, end_month)
   
            
            # print(f"Start: {start_month}/{start_year}, End: {end_month}/{end_year}, Action: {action}")

            
                # Generează Balanta
            try:
                gl=get_gl_from_db(start_month, start_year, end_month, end_year)
            except:
                flash("(GL) Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
                return redirect(url_for('views.generate_monthlyTB'))
            if not gl:
                flash("GL -Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
                return redirect(url_for('views.generate_monthlyTB'))
            try:
                tb_prev, mesaj = get_prev_tb_from_db(start_month, start_year, end_month, end_year)
            except Exception as e:
                flash("Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
                return redirect(url_for('views.generate_monthlyTB'))

            if tb_prev is None:
                flash(mesaj, "danger")
                return redirect(url_for('views.generate_monthlyTB'))
            # else:
            #     flash(mesaj, "success")
    # continuă cu prelucrarea tb_prev

            
            # print(gl)
            gl_df= pd.DataFrame(gl)
            
            tb_prev= pd.DataFrame(tb_prev)
            gl_df['D/C'] = gl_df['Amount'].apply(lambda x: 'D' if x > 0 else 'C')
            # print(gl_df)
            gl_df['AbsAmount'] = gl_df['Amount'].abs()
            gl_df.to_excel(os.path.join(TEMP_FOLDER, f"GL_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
# Pivot cu Month, Year, Br, GL ca index
            tb_df = gl_df.pivot_table(
                index=['Month', 'Year','Statutory_GL'],
                columns='D/C',
                values='Amount',
                aggfunc='sum',
                fill_value=0
            ).reset_index()
            tb_df = tb_df.rename(columns={'D': 'MTD Debit', 'C': 'MTD Credit'})
            # tb_df["Description"]=""
            
            tb_prev.to_excel(os.path.join(TEMP_FOLDER, f"TB_Prev_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
            print(tb_prev.columns,"\ntb_df columns:",tb_df.columns)
            all_accounts = tb_prev[['GL']].drop_duplicates().rename(columns={'GL': 'Statutory_GL'})

# Facem un merge ca să adăugăm lipsurile
            tb_df = all_accounts.merge(tb_df, on='Statutory_GL', how='left')

            # 5. Completăm cu 0 acolo unde nu există mișcări
            tb_df['MTD Debit'] = tb_df['MTD Debit'].fillna(0)
            tb_df['MTD Credit'] = tb_df['MTD Credit'].fillna(0)
            tb_df['Month'] = tb_df['Month'].fillna(end_month)  # sau start_month, cum preferi
            tb_df['Year'] = tb_df['Year'].fillna(end_year)
            
            tb_df.to_excel(os.path.join(TEMP_FOLDER, f"TB_DF_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
            tb_df = tb_df.merge(
                tb_prev[['GL', 'Ending_balance', 'End_DC','Description']].rename(columns={
        'Ending_balance': 'Opening_balance',
        'End_DC': 'Open_DC',
        'MTD Debit':"MTD_Debit",
        'MTD Credit':"MTD_Credit"
    }),  # coloanele pe care le aducem
                left_on='Statutory_GL',
    right_on='GL',   # cheia de join
                how='left'  # păstrăm toate rândurile din tb_df, dacă nu găsește în tb_prev pune NaN
        )   
            print(luna_cifra)
            
            tb_df = tb_df.rename(columns={
    "MTD Debit": "MTD_Debit",
    "MTD Credit": "MTD_Credit"
})
            tb_df["YTD_Debit"]=tb_df["MTD_Debit"]
            tb_df["YTD_Credit"]=tb_df["MTD_Credit"]
            print(tb_df)
            for col in ['Opening_balance', 'MTD_Debit', 'MTD_Credit']:
                tb_df[col] = tb_df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)  
            tb_df["Intermediary_col"]= tb_df.apply(
    lambda row: -row['Opening_balance'] + row['MTD_Debit'] - row['MTD_Credit'] if row['Open_DC'] == 'C'
    else row['Opening_balance'] + row['MTD_Debit'] - row['MTD_Credit'],
    axis=1
)
            tb_df["Ending_balance"]=abs(tb_df["Intermediary_col"])
            tb_df["End_DC"]=tb_df["Intermediary_col"].apply(lambda x: 'D' if x > 0 else 'C')
            if str(luna_cifra)=="1":
                print("a intrat aici")
                tb_df.loc[(tb_df["GL"].astype(str).str.startswith("6")) | (tb_df["GL"].astype(str).str.startswith("7")) | (tb_df["GL"].astype(str).str.startswith("8")),"Opening_balance"]=0
                tb_df.loc[(tb_df["GL"].astype(str).str.startswith("6")),"Ending_DC"]="D"
                tb_df.loc[(tb_df["GL"].astype(str).str.startswith("7")),"Ending_DC"]="C"
            tb_df = tb_df[['Month', 'Year', 'GL', 'Description', 'Opening_balance', 'Open_DC', 
            'MTD_Debit', 'MTD_Credit', 'YTD_Debit', 'YTD_Credit', 'Ending_balance', 'End_DC']]
            tb_df["Month"]=start_month
            tb_df["Year"]=start_year
            # Identifici coloanele numerice
            numeric_cols = tb_df.select_dtypes(include=['number']).columns

            # Înlocuiești NaN cu 0 doar în coloanele numerice
            tb_df[numeric_cols] = tb_df[numeric_cols].fillna(0)
            tb_df["MTD_Credit"]=abs(tb_df["MTD_Credit"])
            tb_df["YTD_Credit"]=abs(tb_df["YTD_Credit"])
            for col in tb_df.columns:
                if tb_df[col].dropna().apply(lambda x: isinstance(x, (int, float, complex)) and not isinstance(x, bool)).all():
                    tb_df[col] = tb_df[col].fillna(0)
            print(tb_df)
            
            if tb_df is None or tb_df.empty:
                flash("Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
            else:
                # Verifică dacă există NaN în coloanele 'cont' sau 'amount'
                if tb_df['GL'].isna().any() :
                    flash("Datele conțin valori invalide (NaN / None / empty) pe coloanele 'GL' sau 'Statutory GL'. "
            "Vă rugăm să verificați maparea GL si sa va asigurati ca nu există valori nule sau necompletate.", 
            "danger")
            # tb_df.to_excel('D:\\Projects\\35. GIT RAS\\RAS_Expeditors\\uploads\\balanta.xlsx', index=False)  # Salvează DataFrame-ul în Excel


            err=insert_tb_df_to_db(tb_df)  # Inserăm DataFrame-ul în baza de date
            tb_data = tb_df.to_dict(orient="records")
            nume_export= f"Balanta_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"
            if err==0:
                flash("Balanta lunara generata si importata in baza de date cu succes!", "success")
            else:
                flash("BALANTA NU A FOST GENERATA! Datele conțin valori invalide (NaN / None / empty) pe coloanele 'GL' sau 'Statutory GL'. "
            "Vă rugăm să verificați maparea GL si sa va asigurati ca nu există valori nule sau necompletate.", 
            "danger")
            output_path = os.path.join(TEMP_FOLDER, nume_export + '.xlsx')
            
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                tb_df.to_excel(writer, sheet_name='Trial Balance', index=False)

                workbook = writer.book
                worksheet = writer.sheets['Trial Balance']

                # ✅ Header format cu borduri
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'middle',
                    'bg_color': '#E0E0E0',
                    'border': 1  # ✅ doar aici bordură
                })

                # 🔢 Format numeric simplu și rapid
                money_format = workbook.add_format({
                    'num_format': '#,##0.00',
                    'align': 'right'
                })

                # 🔤 Format text
                text_format = workbook.add_format({
                    'align': 'left'
                })

                # 🔁 D/C centrat
                dc_format = workbook.add_format({
                    'align': 'center'
                })

                worksheet.freeze_panes(1, 0)

                numeric_cols = ["MTD_Debit", "MTD_Credit", "YTD_Debit", "YTD_Credit", "Opening_balance", "Ending_balance"]

                for col_num, column in enumerate(tb_df.columns):
                    width = 15
                    fmt = text_format

                    if column in ["GL", "Description"]:
                        width = 25
                    elif column in ["Open_DC", "End_DC"]:
                        width = 8
                        fmt = dc_format
                    elif column in ["Month", "Year"]:
                        width = 8
                    elif column in numeric_cols:
                        width = 18
                        fmt = money_format

                    worksheet.set_column(col_num, col_num, width, fmt)
                    worksheet.write(0, col_num, column, header_format)  # ✅ header cu bordură

                # worksheet.conditional_format(1, debit_col, row_count, debit_col, {
                #     'type': 'cell',
                #     'criteria': '>',
                #     'value': 0,
                #     'format': workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
                # })

                # worksheet.conditional_format(1, credit_col, row_count, credit_col, {
                #     'type': 'cell',
                #     'criteria': '>',
                #     'value': 0,
                #     'format': workbook.add_format({'bg_color': '#F4CCCC', 'font_color': '#9C0006'})
                # })
                                


    if code == cod:
        return render_template('monthly tb.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id, tb_data=tb_data, luni_disponibile=luni_disponibile, nume_export=nume_export)
    else:
        return render_template('auth.html')

# @views.route('/generate_tb', methods=['GET', 'POST'])
# @login_required
# @admin_required
def generate_monthlyTB_func(start_date, end_date):
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()
    tb_data=None
    luni_disponibile = get_balanta_months()
    print(luni_disponibile)
    nume_export=None
    # luni_pe_ani = get_luni_pe_ani([f"{luna['Month']:02d} {luna['Year']}" for luna in luni_disponibile])
    

    luna = start_month
    start_date = datetime.strptime(str(luna), "%Y-%m")
    luna_cifra = start_date.month  # întoarce un int: 6 pentru Iunie
    # procesezi balanța lunară
    # start_date = request.form.get('start-date')  # format: '2025-06'
    end_date = start_date      # format: '2025-08'
    # action = request.form.get('action')          # 'tb' sau 'fisa'

    # Extragem luna și anul din stringuri
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))
    prev_year, prev_month = get_previous_month(start_year, start_month)
    prev_year_end, prev_month_end = get_previous_month(end_year, end_month)

    
    # print(f"Start: {start_month}/{start_year}, End: {end_month}/{end_year}, Action: {action}")

    
        # Generează Balanta
    # try:
    gl=get_gl_from_db(start_month, start_year, end_month, end_year)

   
    tb_prev, mesaj = get_prev_tb_from_db(start_month, start_year, end_month, end_year)
    gl_df= pd.DataFrame(gl)
    tb_prev= pd.DataFrame(tb_prev)
    gl_df['D/C'] = gl_df['Amount'].apply(lambda x: 'D' if x > 0 else 'C')
    # print(gl_df)
    gl_df['AbsAmount'] = gl_df['Amount'].abs()

# Pivot cu Month, Year, Br, GL ca index
    tb_df = gl_df.pivot_table(
        index=['Month', 'Year','Statutory_GL'],
        columns='D/C',
        values='Amount',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    tb_df = tb_df.rename(columns={'D': 'MTD Debit', 'C': 'MTD Credit'})
    tb_df["Description"]=""
    tb_df = tb_df.merge(
        tb_prev[['GL', 'Ending_balance', 'End_DC']].rename(columns={
'Ending_balance': 'Opening_balance',
'End_DC': 'Open_DC',
'MTD Debit':"MTD_Debit",
'MTD Credit':"MTD_Credit"
}),  # coloanele pe care le aducem
        left_on='Statutory_GL',
right_on='GL',   # cheia de join
        how='left'  # păstrăm toate rândurile din tb_df, dacă nu găsește în tb_prev pune NaN
)   
    print(luna_cifra)
    if luna_cifra=="1":
        tb_df.loc[(tb_df["GL"].astype(str).str.startswith("6")) | (tb_df["GL"].astype(str).str.startswith("7")),"Ending_balance"]=0
        tb_df.loc[(tb_df["GL"].astype(str).str.startswith("6")),"Ending_DC"]="D"
        tb_df.loc[(tb_df["GL"].astype(str).str.startswith("7")),"Ending_DC"]="C"
    tb_df = tb_df.rename(columns={
"MTD Debit": "MTD_Debit",
"MTD Credit": "MTD_Credit"
})
    tb_df["YTD_Debit"]=tb_df["MTD_Debit"]
    tb_df["YTD_Credit"]=tb_df["MTD_Credit"]
    print(tb_df)
    for col in ['Opening_balance', 'MTD_Debit', 'MTD_Credit']:
        tb_df[col] = tb_df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)  
    tb_df["Intermediary_col"]= tb_df.apply(
lambda row: -row['Opening_balance'] + row['MTD_Debit'] - row['MTD_Credit'] if row['Open_DC'] == 'C'
else row['Opening_balance'] + row['MTD_Debit'] - row['MTD_Credit'],
axis=1
)
    tb_df["Ending_balance"]=abs(tb_df["Intermediary_col"])
    tb_df["End_DC"]=tb_df["Intermediary_col"].apply(lambda x: 'D' if x > 0 else 'C')
    tb_df = tb_df[['Month', 'Year', 'GL', 'Description', 'Opening_balance', 'Open_DC', 
    'MTD_Debit', 'MTD_Credit', 'YTD_Debit', 'YTD_Credit', 'Ending_balance', 'End_DC']]
    tb_df["Month"]=start_month
    tb_df["Year"]=start_year
    # Identifici coloanele numerice
    numeric_cols = tb_df.select_dtypes(include=['number']).columns

    # Înlocuiești NaN cu 0 doar în coloanele numerice
    tb_df[numeric_cols] = tb_df[numeric_cols].fillna(0)
    for col in tb_df.columns:
        if tb_df[col].dropna().apply(lambda x: isinstance(x, (int, float, complex)) and not isinstance(x, bool)).all():
            tb_df[col] = tb_df[col].fillna(0)
    print(tb_df)
    
    tb_df.to_excel('D:\\Projects\\35. GIT RAS\\RAS_Expeditors\\uploads\\balanta.xlsx', index=False)  # Salvează DataFrame-ul în Excel


    err=insert_tb_df_to_db(tb_df)  # Inserăm DataFrame-ul în baza de date
    tb_data = tb_df.to_dict(orient="records")
    nume_export= f"Balanta_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"
    # if err==0:
    #     flash("Balanta lunara generata si importata in baza de date cu succes!", "success")
    # else:
    #     flash("BALANTA NU A FOST GENERATA! Datele conțin valori invalide (NaN / None / empty) pe coloanele 'GL' sau 'Statutory GL'. "
    # "Vă rugăm să verificați maparea GL si sa va asigurati ca nu există valori nule sau necompletate.", 
    # "danger")
                




@views.route('/generate_reports')
# @login_required
# @admin_required
def generate_reports():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()

    if code == cod:
        return render_template('vizualizare_rapoarte.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id)
    else:
        return render_template('auth.html')


    
# @views.route('/upload_into_database', methods=['POST'])
# def upload_into_database():
#     """Rută pentru procesarea fișierului Excel în background"""
#     """Rută pentru procesarea fișierului Excel în background"""
#     try:
#         files = os.listdir(Config.UPLOAD_FOLDER)
#         if not files:
#             return jsonify({'message': 'No files found in upload directory'}), 400
 
#         latest_file = max(
#             [os.path.join(Config.UPLOAD_FOLDER, f) for f in files],
#             key=os.path.getctime
#         )
       
#         # Pornește task-ul de import
#         task = import_gl_task.delay(latest_file)

#         connection = mysql.connect(**mysql_config)
#         cursor = connection.cursor()
#         cursor.execute(
#             "INSERT INTO celery_tasks (task_id, start_time, status) VALUES (%s, NOW(), %s)",
#             (task.id, 'PENDING')
#         )
#         connection.commit()
#         cursor.close()
#         connection.close()
       
#         print(f"Task ID: {task.id}")  # Pentru debug în consolă
#         logger.info(f"Started import task with ID: {task.id}")
       
#         return jsonify({
#             'message': 'File import started in background',
#             'task_id': task.id
#         }), 202
       
#     except Exception as e:
#         logger.error(f"Error in upload_into_database: {str(e)}")
#         return jsonify({'message': f'Error: {str(e)}'}), 500
   
# @views.route('/task_status/<task_id>')
# def task_status(task_id):
#     """Verifică statusul unui task Celery"""
#     task = import_gl_task.AsyncResult(task_id)
   
#     if task.state == 'PENDING':
#         response = {
#             'state': task.state,
#             'status': 'Task is pending...'
#         }
#     elif task.state == 'SUCCESS':
#         response = {
#             'state': task.state,
#             'result': task.result
#         }
#     else:
#         response = {
#             'state': task.state,
#             'status': str(task.info)
#         }
#     return jsonify(response)

# @views.route('/start_mapare_gl', methods=['POST'])
# def start_mapare_gl():
#     try:
#         task = mapare_gl_task.delay()

#         connection = mysql.connect(**mysql_config)
#         cursor = connection.cursor()
#         cursor.execute(
#             "INSERT INTO celery_tasks (task_id, start_time, status) VALUES (%s, NOW(), %s)",
#             (task.id, 'PENDING')
#         )
#         connection.commit()
#         cursor.close()
#         connection.close()

#         return jsonify({'message': 'Mapare GL pornită în background', 'task_id': task.id}), 202
#     except Exception as e:
#         return jsonify({'message': f'Error: {str(e)}'}), 500


# @views.route('/stop_task/<task_id>', methods=['POST'])
# def stop_task(task_id):
#     celery.control.revoke(task_id, terminate=True)
#     # Poți updata și statusul în DB dacă vrei
#     return jsonify({'message': f'Task {task_id} a fost oprit.'}) 


@views.route('/upload-nc', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'Niciun fișier trimis'})

    file = request.files['file']
    filename = os.path.join(TEMP_FOLDER, file.filename)
    file.save(filename)
    REQUIRED_COLUMNS = [
    "BR", "Statutory_GL", "Journal", "Open_Item", "File_Ref", "Date",
    "Month", "Year", "Amount", "data_Description", "Post_Date", "TC",
    "Foreign Amount", "Foreign Currency", "GCI","Customer Name"
]


    try:
        # Detectăm extensia
        if filename.endswith('.csv'):
            df = pd.read_csv(filename)
        elif filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filename)
        else:
            return jsonify({'status': 'error', 'message': 'Format fișier invalid. Folosește CSV sau Excel.'})

        # Exemplu validare simplă
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            return jsonify({'status': 'error', 'message': f'Coloane lipsă în fișier: {", ".join(missing)}'})

        # Salvăm temporar pentru import
        # df.read_excel(filename)
        df['JT'] = 'EXT'
        df['rowNumber'] = df.groupby('Journal').cumcount() + 1
        df['timestamp'] = datetime.now()
        # Convertim coloanele Date și Post_Date la datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
        df['Post_Date'] = pd.to_datetime(df['Post_Date'], errors='coerce', dayfirst=True)

        # Convertim timestamp la format MySQL
        df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(df.columns)
        df = df.where(pd.notnull(df), None)  # <- înlocuiește NaN cu None
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, 'nan': None, 'NaN': None})
        email = session.get('email')
        first_name = extract_name_from_email(email)

        user = get_user_from_db(email)
        df["User_id"]=user.id
        df["User_name"]=first_name
        df["User_email"]=email
        journal_uuid_map = {journal: str(uuid.uuid4()) for journal in df['Journal'].unique()}
        df['Acct_Tran_Id'] = df['Journal'].map(journal_uuid_map)
        a=insert_istoric_nc_rows(df)
        print(a)
        temp_path = os.path.join(TEMP_FOLDER, 'Template_NC.xlsx')
        df.to_excel(temp_path, index=False)

        return jsonify({'status': 'ok'})

        # flash("Fișierul a fost încărcat și procesat cu succes!", "success")
        # return redirect(url_for('views.generate_reports_processing')) 

        # return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
    
@views.route('/import-nc-gl', methods=['POST'])
def import_nc_gl():
    temp_path = os.path.join(TEMP_FOLDER, 'Template_NC.xlsx')
    
    if not os.path.exists(temp_path):
        return jsonify({'status': 'error', 'message': 'Fișierul de import nu există. Reîncarcă fișierul.'})
    
    try:
        df = pd.read_excel(temp_path,engine='openpyxl')

        # Conversii dacă e cazul
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Post_Date'] = pd.to_datetime(df['Post_Date'], errors='coerce')

        df = df.where(pd.notnull(df), None)  # Înlocuiește NaN cu None
        # df = df.where(pd.notnull(df), None)  # <- înlocuiește NaN cu None
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, 'nan': None, 'NaN': None})
        

        # Apelezi funcția ta de inserare în general_ledger
        result, msg = insert_nc_into_general_ledger(df)
        print(result,msg)

        if result:
            # opțional: ștergi fișierul după import
            os.remove(temp_path)
            return jsonify({'status': 'ok', 'message': 'Import în general_ledger finalizat cu succes.'})
        else:
            return jsonify({'status': 'error', 'message': msg})
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
@views.route('/manual-nc', methods=['POST'])
def manual_nc():
    try:
        data = request.get_json()
        print(data)

        # REQUIRED_FIELDS = [
        #     'date', 'journalType', 'documentNumber', 'documentDate',
        #     'documentType', 'currency', 'profitCenter',
        #     'debitAmount', 'creditAmount', 'accountCode', 'accountCreditCode',
        #     'partner', 'partnerTaxCode', 'transactionDescription', 'paymentMethod'
        # ]

        # missing = [field for field in REQUIRED_FIELDS if not data.get(field)]
        # print(missing)
        # if missing:
        #     return jsonify({'status': 'error', 'message': f'Câmpuri lipsă: {", ".join(missing)}'})
        
#         df = pd.DataFrame([{
#     "Date": data['Date'],
#     "Journal": data['Journal'],
#     "File_Ref": data['File_Ref'],
#     "Post_Date": data['Post_Date'],
#     "data_Description": data['data_Description'],
#     "Statutory_GL": data['Statutory_GL'],
#     "Open_Item": data['Open_Item'],
#     "Customer Name": data['Customer_Name'],
#     "TC": data['TC'],
#     "Foreign Currency": data['Currency'],
#     "Amount": float(data['Amount']) if data['Amount'] else 0,
#     "Foreign Amount": float(data['Foreign_Amount']) if data['Foreign_Amount'] else 0,
#     "BR": data['BR'],
#     "Month": int(data['Month']) if data['Month'] else None,
#     "Year": int(data['Year']) if data['Year'] else None,
#     "JT": "EXT",
#     "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# }])
        df = pd.DataFrame([
        {
            "Date": data['Date'],
            "Journal": data['Journal'],
            "File_Ref": data['File_Ref'],
            "Post_Date": data['Post_Date'],
            "data_Description": data['data_Description'],
            "Statutory_GL": data['Statutory_GL_D'],  # cont debit
            "Open_Item": data['Open_Item'],
            "Customer Name": data['Customer_Name'],
            "TC": data['TC'],
            "Foreign Currency": data['Currency'],
            "Amount": float(data['Amount']) if data['Amount'] else 0,
            "Foreign Amount": float(data['Foreign_Amount']) if data['Foreign_Amount'] else 0,
            "BR": data['BR'],
            "Month": int(data['Month']) if data['Month'] else None,
            "Year": int(data['Year']) if data['Year'] else None,
            "JT": "EXT",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            # "Line_Type": "Debit"
        },
        {
            "Date": data['Date'],
            "Journal": data['Journal'],
            "File_Ref": data['File_Ref'],
            "Post_Date": data['Post_Date'],
            "data_Description": data['data_Description'],
            "Statutory_GL": data['Statutory_GL_C'],  # cont credit
            "Open_Item": data['Open_Item'],
            "Customer Name": data['Customer_Name'],
            "TC": data['TC'],
            "Foreign Currency": data['Currency'],
            "Amount": float(data['Credit_Amount']) if data['Credit_Amount'] else 0,
            "Foreign Amount": float(data['Credit_Foreign_Amount']) if data['Credit_Foreign_Amount'] else 0,
            "BR": data['BR'],
            "Month": int(data['Month']) if data['Month'] else None,
            "Year": int(data['Year']) if data['Year'] else None,
            "JT": "EXT",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            # "Line_Type": "Credit"
        }
    ])



        email = session.get('email')
        first_name = extract_name_from_email(email)
        user = get_user_from_db(email)

        df["User_id"] = user.id
        df["User_name"] = first_name
        df["User_email"] = email
        df['rowNumber'] = df.groupby('Journal').cumcount() + 1

        df = df.where(pd.notnull(df), None)
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, 'nan': None, 'NaN': None})
        print(df,"----------------------------------")
        journal_uuid_map = {journal: str(uuid.uuid4()) for journal in df['Journal'].unique()}
        df['Acct_Tran_Id'] = df['Journal'].map(journal_uuid_map)
        temp_path = os.path.join(TEMP_FOLDER, 'Template_NC.xlsx')
        df.to_excel(temp_path, index=False)
        result, msg =insert_istoric_nc_rows(df)
        if not result:
            return jsonify({'status': 'error', 'message': msg}), 500
        result, msg = insert_nc_into_general_ledger(df)
        print(result,msg,"------------------------------")

        if not result:
            return jsonify({'status': 'error', 'message': msg}), 500

        return jsonify({'status': 'ok', 'message': 'Înregistrare adăugată cu succes!'})
    
    except Exception as e:
        print(e,"+++++++++++++++++")
        return jsonify({'status': 'error', 'message': str(e)})
    
    
@views.route('/get-notes', methods=['POST'])
def get_notes():
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        if not start_date or not end_date:
            return jsonify({'status': 'error', 'message': 'Perioadă invalidă.'})

        start_year, start_month = map(int, start_date.split('-'))
        end_year, end_month = map(int, end_date.split('-'))

        start_key = start_year * 100 + start_month
        end_key = end_year * 100 + end_month

        # Conectare MySQL
        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = connection.cursor(dictionary=True)

        # Interogare ISTORIC_NC
        query = """
            SELECT *
            FROM ISTORIC_NC
            WHERE (Year * 100 + Month) BETWEEN %s AND %s
            ORDER BY Year, Month
        """
        cursor.execute(query, (start_key, end_key))
        data = cursor.fetchall()

        cursor.close()
        connection.close()
        print(data,"dataaaaaaaaaaa")
        for row in data:
            for key, val in row.items():
                if isinstance(val, (date, datetime)):
                    row[key] = val.strftime('%Y-%m-%d')

        return jsonify({'status': 'ok', 'data': data})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
    
@views.route('/update-note', methods=['POST'])
def update_note():
    try:
        data = request.get_json()
        primary_key = data.get('id')
        if not primary_key:
            return jsonify({'status': 'error', 'message': 'ID-ul lipsește'})

        # Pregătește conexiunea
        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = connection.cursor(dictionary=True)

        # 1. Obține Acct_Tran_Id pentru această notă
        cursor.execute("SELECT Acct_Tran_Id FROM ISTORIC_NC WHERE id = %s", (primary_key,))
        result = cursor.fetchone()
        if not result or not result['Acct_Tran_Id']:
            cursor.close()
            connection.close()
            return jsonify({'status': 'error', 'message': 'Nu s-a găsit Acct_Tran_Id'}), 400

        acct_tran_id = result['Acct_Tran_Id']

        # 2. Construiește actualizarea
        updates = []
        values = []
        for key, val in data.items():
            if key != 'id':
                if val == '' and key in ['Foreign Amount', 'AltCampNumeric']:  # câmpuri numerice
                    val = None
                updates.append(f"`{key}` = %s")
                values.append(val)

        if not updates:
            cursor.close()
            connection.close()
            return jsonify({'status': 'error', 'message': 'Nicio actualizare specificată'})

        update_clause = ', '.join(updates)

        # 3. UPDATE în ISTORIC_NC
        cursor.execute(f"UPDATE ISTORIC_NC SET {update_clause} WHERE id = %s", values + [primary_key])

        # 4. UPDATE în GENERAL_LEDGER (doar acele coloane care există în ambele tabele)
        allowed_gl_fields = set([
            'BR', 'File_Ref', 'Statutory_GL', 'Journal', 'Open_Item',
            'Date', 'Post_Date', 'Month', 'Year', 'Amount',
            'Foreign_Amount', 'Currency', 'data_Description', 'TC',
            'Customer_GCI', 'Customer_Name'
        ])

        gl_updates = []
        gl_values = []
        for key, val in data.items():
            print(key, val, "key and val")
            if key in allowed_gl_fields:
                if val == '' and key in ['Foreign_Amount']:  # numeric fields in GL
                    val = None
                gl_updates.append(f"`{key}` = %s")
                gl_values.append(val)
        print(gl_updates, "gl_updates")
        print(gl_values, "gl_values")
        if gl_updates:
            gl_query = f"UPDATE GENERAL_LEDGER SET {', '.join(gl_updates)} WHERE Acct_Tran_Id = %s"
            cursor.execute(gl_query, gl_values + [acct_tran_id])
            # print(gl_query)

        connection.commit()
        cursor.close()
        connection.close()

        return jsonify({'status': 'ok'})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@views.route('/delete-note', methods=['POST'])
def delete_note():
    try:
        data = request.get_json()
        primary_key = data.get('id')
        if not primary_key:
            return jsonify({'status': 'error', 'message': 'ID-ul lipsește'})

        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = connection.cursor(dictionary=True)

        # 1. Obține Acct_Tran_Id
        cursor.execute("SELECT Acct_Tran_Id FROM ISTORIC_NC WHERE id = %s", (primary_key,))
        result = cursor.fetchone()

        if not result or not result['Acct_Tran_Id']:
            cursor.close()
            connection.close()
            return jsonify({'status': 'error', 'message': 'Nu s-a găsit Acct_Tran_Id asociat'}), 400

        acct_tran_id = result['Acct_Tran_Id']

        # 2. Șterge din ISTORIC_NC
        cursor.execute("DELETE FROM ISTORIC_NC WHERE Acct_Tran_Id= %s", (acct_tran_id,))

        # 3. Șterge din GENERAL_LEDGER
        cursor.execute("DELETE FROM GENERAL_LEDGER WHERE Acct_Tran_Id = %s", (acct_tran_id,))

        connection.commit()
        cursor.close()
        connection.close()

        return jsonify({'status': 'ok', 'message': 'Înregistrarea a fost ștearsă din GL si istoric.'})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = {'xls', 'xlsx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
@views.route('/upload-gl', methods=['POST'])
def upload_mapping_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'Niciun fișier trimis.'})

    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'Numele fișierului este gol.'})

    if not allowed_file(file.filename):
        return jsonify({'status': 'error', 'message': 'Fișier invalid. Trebuie să fie .xls sau .xlsx.'})

    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(TEMP_FOLDER, filename)
        file.save(filepath)
        # ✅ Salvează calea în sesiune sau într-o variabilă globală temporară
        request.environ['mapping_file_path'] = filepath
        session['mapping_file_path'] = filepath
        return jsonify({'status': 'ok', 'message': 'Fișier încărcat cu succes.', 'path': filepath})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)+" Fișierul nu a putut fi încărcat."})

@views.route('/import-gl', methods=['POST'])
def import_mapping_data():
    try:
        # ✅ Găsește ultimul fișier .xls/.xlsx din folderul UPLOAD_FOLDER
        excel_files = [f for f in os.listdir(TEMP_FOLDER) if f.endswith(('.xls', '.xlsx'))]
        if not excel_files:
            return jsonify({'status': 'error', 'message': 'Nu există niciun fișier de import.'})

        latest_file = session.get('mapping_file_path')
        if not latest_file:
            return jsonify({'status': 'error', 'message': 'Fișierul nu a fost găsit în sesiune.'})
        filepath = os.path.join(TEMP_FOLDER, latest_file)

        # ✅ Importă în baza de date
        resp=import_into_db(filepath)
        print(jsonify(resp),"resp")
        

        return jsonify(resp)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@views.route('/mapare-gl-perioada', methods=['POST'])
def mapare_gl_perioada():
    try:
        data = request.get_json()
        
        month = int(data.get('month'))
        year = int(data.get('year'))
        

        if not month or not year:
            return jsonify({'status': 'error', 'message': 'Luna și anul sunt obligatorii.'})

        mesaj = procedura_mapare_period(month, year)
        return jsonify({'status': 'ok', 'message': mesaj})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})
    
@views.route('/delete-gl', methods=['DELETE'])
def delete_gl():
    perioada = request.args.get('perioada')  # Ex: '2025-08'
    
    if not perioada:
        return "Parametrul 'perioada' este necesar (format: YYYY-MM).", 400

    try:
        # Validăm perioada și extragem anul și luna
        data = datetime.strptime(perioada, '%Y-%m')
        anul = data.year
        luna = data.month
    except ValueError:
        return "Formatul perioadei este invalid. Folosește YYYY-MM.", 400

    try:
        connection = mysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        cursor = connection.cursor()

        query = """
            DELETE FROM general_ledger
            WHERE year = %s AND month = %s
        """
        cursor.execute(query, (anul, luna))
        connection.commit()
        
        deleted = cursor.rowcount

        cursor.close()
        connection.close()

        if deleted == 0:
            return jsonify({
                "success": False,
                "deleted_rows": 0,
                "message": f"Nu există rânduri GL pentru perioada {perioada}."
            }), 404

        return jsonify({
            "success": True,
            "deleted_rows": deleted,
            "message": f"{deleted} rânduri GL șterse pentru perioada {perioada}."
        })

    except Exception as e:
        return f"Eroare la ștergere: {str(e)}", 500


        