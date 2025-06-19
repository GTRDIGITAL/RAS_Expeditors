from flask import Blueprint, render_template, redirect, request, session, flash, url_for, jsonify
from flask_login import login_user, login_required, logout_user, current_user
from . import db
from datetime import datetime

# from .utils import extract_name_from_email
from .otp import generate_new_code
from .mail import trimitereMail, trimitereOTPMail
import utils
from .database import get_all_users, get_user_from_db, update_user_in_db
# from .stocareBD import *
import mysql.connector
from .decorators import admin_required
from .models import Users
import pandas as pd
import os
import decimal
from .insert_GL import import_into_db
from celery import Celery  # Import Celery
from celery.result import AsyncResult
import logging
import redis  # Import the redis library
import json
from collections import defaultdict
from .trimitereCodOTP import trimitereFilesMail
def extract_name_from_email(email):
    local_part = email.split('@')[0]
    name_parts = local_part.replace('.', ' ').replace('-', ' ').split(' ')
    formatted_name = ' '.join([part.capitalize() for part in name_parts])
    return formatted_name


# filepath: c:\Dezvoltare\RAS\RAS Expeditors\website\views.py
from dotenv import load_dotenv

views = Blueprint('views', __name__)

UPLOAD_FOLDER = 'C:\\Dezvoltare\\RAS\\RAS Expeditors\\uploads'  # Define the upload folder
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
    connection = mysql.connector.connect(
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
def get_balanta_months():
    from collections import defaultdict
    from datetime import datetime
    connection = mysql.connector.connect(
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
    connection = mysql.connector.connect(
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
    connection = mysql.connector.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT GL, Ending_balance, End_DC
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
    connection = mysql.connector.connect(
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
def insert_tb_df_to_db(tb_df):
    connection = mysql.connector.connect(
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

UPLOAD_FOLDER = 'C:\\Dezvoltare\\RAS\\RAS Expeditors\\uploads'  # Define the upload folder
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@views.route('/load_transform', methods=['GET', 'POST'])
# @login_required
# @admin_required
def load_transform():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()

    if request.method == 'POST':
        if 'file' not in request.files:
            print("No file part in the request")  # Debugging
            return jsonify({'message': 'No file part'}), 400

        file = request.files['file']

        if file.filename == '':
            print("No file selected")  # Debugging
            return jsonify({'message': 'No selected file'}), 400

        if file:
            try:
                filename = file.filename
                filepath = os.path.join(UPLOAD_FOLDER, filename)  # Use os.path.join
                file.save(filepath)

                print(f"File saved to: {filepath}")  # Debugging
                return jsonify({'message': 'File upload successful.'}), 200 # No task started here

            except Exception as e:
                print(f"Error processing file: {e}")  # Detailed error logging
                return jsonify({'message': str(e)}), 500

    if code == cod:
        return render_template('load_transform.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id)
    else:
        return render_template('auth.html')
@views.route('/generate_reports', methods=['GET', 'POST'])
# @login_required
# @admin_required
def generate_reports_processing():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()
    fisier=None
    nume_export=None

    if request.method == 'POST':
        start_date = request.form.get('start-date')  # format: '2025-06'
        end_date = request.form.get('end-date')      # format: '2025-08'
        action = request.form.get('action')          # 'tb' sau 'fisa'
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
            return f"Generez fișa de cont de la {start_month}/{start_year} până la {end_month}/{end_year}" 
        elif action == 'gl':
            # Generează Fișa de cont
            gl,err= get_gl_period_from_db(start_month, start_year, end_month, end_year)
            if err==1:
                flash("Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
            print(gl)
            gl_df= pd.DataFrame(gl)
            gl_df.to_excel(os.path.join(TEMP_FOLDER, f"GL_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}.xlsx"), index=False)
            # gl.to_excel('D:\\Projects\\35. GIT RAS\\RAS_Expeditors\\temp\\REGISTRU JURNAL.xlsx', index=False)  # Salvează DataFrame-ul în Excel
            gl_preview = gl[:100]
            fisier=gl_preview
            nume_export=f"GL_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"
            
            
            flash("GL afisat cu succes! Pentru download click export pe tabelul de mai jos.", "success")
                
            # return f"Generez fișa de cont de la {start_month}/{start_year} până la {end_month}/{end_year}" 
        

    if code == cod:
        return render_template('vizualizare_rapoarte.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id, fisier=fisier, nume_export=nume_export)
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
            print(gl_df)
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
            
            if tb_df is None or tb_df.empty:
                flash("Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
            else:
                # Verifică dacă există NaN în coloanele 'cont' sau 'amount'
                if tb_df['GL'].isna().any() :
                    flash("Datele conțin valori invalide (NaN / None / empty) pe coloanele 'GL' sau 'Statutory GL'. "
            "Vă rugăm să verificați maparea GL si sa va asigurati ca nu există valori nule sau necompletate.", 
            "danger")
            tb_df.to_excel('D:\\Projects\\35. GIT RAS\\RAS_Expeditors\\uploads\\balanta.xlsx', index=False)  # Salvează DataFrame-ul în Excel


            err=insert_tb_df_to_db(tb_df)  # Inserăm DataFrame-ul în baza de date
            tb_data = tb_df.to_dict(orient="records")
            nume_export= f"Balanta_{start_month:02d}_{start_year}_{end_month:02d}_{end_year}"
            if err==0:
                flash("Balanta lunara generata si importata in baza de date cu succes!", "success")
            else:
                flash("BALANTA NU A FOST GENERATA! Datele conțin valori invalide (NaN / None / empty) pe coloanele 'GL' sau 'Statutory GL'. "
            "Vă rugăm să verificați maparea GL si sa va asigurati ca nu există valori nule sau necompletate.", 
            "danger")
                    
                
            # elif action == 'fisa':
            #     # Generează Fișa de cont
            #     return f"Generez fișa de cont de la {start_month}/{start_year} până la {end_month}/{end_year}" 
            # elif action == 'gl':
            #     # Generează Fișa de cont
            #     gl,err= get_gl_period_from_db(start_month, start_year, end_month, end_year)
            #     print(gl)
            #     if err==1:
            #         flash("Nu există date pentru perioada selectată sau nu s-a putut realiza conexiunea la baza de date.", "danger")
            #     else:
            #         flash("GL afisat cu succes! Pentru download click export pe tabelul de mai jos.", "success")
                    
            #     # return f"Generez fișa de cont de la {start_month}/{start_year} până la {end_month}/{end_year}" 
            

    if code == cod:
        return render_template('monthly tb.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id, tb_data=tb_data, luni_disponibile=luni_disponibile, nume_export=nume_export)
    else:
        return render_template('auth.html')



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

@views.route('/upload_into_database', methods=['POST'])
def upload_into_database():
    """
    Rută pentru procesarea fișierului Excel în background
    """
    try:
        # Verifică dacă există fișiere în director
        files = os.listdir(Config.UPLOAD_FOLDER)
        if not files:
            return jsonify({'message': 'No files found in upload directory'}), 400

        # Ia cel mai recent fișier
        latest_file = max(
            [os.path.join(Config.UPLOAD_FOLDER, f) for f in files],
            key=os.path.getctime
        )
        
        # Pornește task-ul Celery
        task = async_import_task.delay(latest_file)
        logger.info(f"Started import task with ID: {task.id}")
        
        return jsonify({
            'message': 'File import started in background',
            'task_id': task.id
        }), 202
        
    except Exception as e:
        logger.error(f"Error in upload_into_database: {str(e)}")
        return jsonify({'message': f'Error: {str(e)}'}), 500