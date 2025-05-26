from flask import Blueprint, render_template, redirect, request, session, flash, send_file, send_from_directory, url_for, jsonify
from flask_login import login_user, login_required, logout_user, current_user
import pyotp
from .models import Users
import json
import base64
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from .trimitereCodOTP import *
from .sendMails import *
from . import db 
from .decorators import *
from .stocareBD import *

def extract_name_from_email(email):
    # Extragem partea de înainte de "@" din email
    local_part = email.split('@')[0]

    # Înlocuim punctele și cratimele cu spații
    name_parts = local_part.replace('.', ' ').replace('-', ' ').split(' ')

    # Formăm numele cu litere mari la început și restul mici
    formatted_name = ' '.join([part.capitalize() for part in name_parts])
    
    return formatted_name

def trimitereMail():
    message_text = "Hello,\n\nConnection test.\n\nThank you,\nGTRDigital"
    
    date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    subj = "Test connect" + str(date)
    mailTo = "cristian.iordache@ro.gt.com"
    # destinatie = "C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors v2 Azure/destinatie/"
    destinatie = ''
    attachment_path = destinatie
    cc_recipients=""
    with open(attachment_path, "rb") as attachment:
        attachment_data = attachment.read()
        attachment_encoded = base64.b64encode(attachment_data).decode()
    send_email_via_graph_api(subj, mailTo,message_text ,attachment_path,cc_recipients)

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']
# print(mysql_config)

views = Blueprint('views', __name__)
lista=[]

@views.route('/main', methods=['GET','POST'])
@login_required
def main():
    email = session.get('email')
    email_parts = email.split("@")[0].split(".")
    first_name = email_parts[0].capitalize()
    print("aici e first name: ", first_name)
    
    user = Users.query.filter_by(username=email).first()
    is_admin = user and user.role == 'admin'
    print('ce tip de user esti cumetre', is_admin)
    
    code = session.get('verified_code')
    cod = session.get('cod')
    
    if code == cod:
        return render_template('main.html', email = user.username, user_name=first_name, is_admin=is_admin)
    else:
        return render_template('auth.html')
    


@views.route('/verify', methods=['GET', 'POST'])
@login_required
def verify():
    email = session.get('email')
    code = None
    if request.method == 'POST':
        user_code = request.form['code']
        cod = session.get('cod')
        if user_code == cod:
            user = Users.query.filter_by(username=email).first()
            print("AVEM ID USER: ", user)
            login_user(user)
            code = user_code
            session['verified_code'] = code
            return redirect(url_for('views.main', email=email))
        else:
            flash('Cod incorect. Încearcă din nou.')
    return render_template('verify.html')

@views.route('/fail', methods=['GET','POST'])
@login_required
def fail():
    email = session.get('email')
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        return render_template('fail.html')
    else:
        return render_template('auth.html')


@views.route('/generate-new-code', methods=['GET', 'POST'])
@login_required
def generate_new_code():
    email = session.get('email')
    if email:  # Verifică dacă există adresa de email în sesiune
        key = pyotp.random_base32()
        totp = pyotp.TOTP(key)
        new_code = totp.now()
        session['cod'] = new_code
        print(new_code)  # Afișează codul în consola serverului (Python)
        trimitereOTPMail(new_code, email)
        return new_code
    return 'Adresa de email nu este prezentă în sesiune.'

@views.route('/view-users')
@login_required
@admin_required
def users():
    email = session.get('email')
    email_parts = email.split("@")[0].split(".")
    first_name = email_parts[0].capitalize()
    cod = session.get('cod')
    code = session.get('verified_code')
    user = Users.query.filter_by(username=email).first()
    
    users_list = get_all_users()  # Ia utilizatorii din baza de date
    
    if code == cod:
        print(users_list)
        return render_template('view_users.html', email = user.username, user_name=first_name, users=users_list, user_id = user.id)
    else:
        return render_template('auth.html')        
    
@views.route('/edit_user/<int:user_id>')
@login_required
@admin_required
def edit_user(user_id):
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        user = get_user_from_db(user_id)  # Funcție care ia utilizatorul din DB
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
        email = session.get('email')
        email_parts = email.split("@")[0].split(".")
        first_name = email_parts[0].capitalize()
        cod = session.get('cod')
        code = session.get('verified_code')
        user = Users.query.filter_by(username=email).first()
        
        users_list = get_all_users()

        if username and role:
            update_user_in_db(user_id, username, role)
        
        # return render_template('view_users.html', email = user.username, user_name=first_name, users=users_list, user_id = user.id)  # Redirecționează către lista de utilizatori
        return redirect(url_for('views.users'))
    else:
       return render_template('auth.html') 

@views.route('/delete-user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    user = Users.query.get(user_id)  # Găsește utilizatorul

    if not user:
        return jsonify({"error": "Userul nu a fost gasit."}), 404

    try:
        db.session.delete(user)  # Șterge utilizatorul
        db.session.commit()  # Comite schimbările în baza de date
        return jsonify({"success": True, "message": "Userul a fost sters cu succes."}), 200
    except Exception as e:
        db.session.rollback()  # Dacă apare o eroare, facem rollback
        return jsonify({"error": f"Eoare stergere user: {str(e)}"}), 500
    
    
    
    
    
