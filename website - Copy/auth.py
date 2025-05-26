from flask import Blueprint, render_template, request, flash, redirect, url_for, session
from .models import Users, PasswordResetToken
from . import db 
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, login_required, logout_user, current_user
import datetime
import pyotp
from .trimitereCodOTP import trimitereOTPMail
import json
from .sendMails import *
import uuid
import datetime
from flask import request, flash, redirect, url_for
from werkzeug.security import generate_password_hash
import random
from .decorators import *

auth = Blueprint('auth', __name__)

        
SESSION_TIMEOUT = 60  # 1 minute in seconds

# Helper function to reset session timeout
def reset_session_timeout():
    session['last_activity'] = datetime.datetime.now()

@auth.before_request
def before_request():
    if current_user.is_authenticated:
        # Reset session timeout on every request
        reset_session_timeout()

        # Check if session has expired
        last_activity = session.get('last_activity')
        if last_activity is not None:
            elapsed_time = datetime.datetime.now() - last_activity
            if elapsed_time.total_seconds() > SESSION_TIMEOUT:
                logout_user()
                flash('Session expired. Please log in again.', category='error')
                return redirect(url_for('auth.login'))

@auth.route('/', methods=['GET', 'POST'])
def login():
    
    if request.method == 'POST':
        email = request.form.get('username')
        password = request.form.get('password')
        print("trece pe aici")
        
        def citeste_configurare(file_path):
            with open(file_path, 'r') as file:
                config = json.load(file)
            return config

        config = citeste_configurare('config.json')
        
        user = Users.query.filter_by(username=email).first()
        
        if user:
            if check_password_hash(user.password, password):
                session['email'] = email
                login_user(user, remember=True)
                print(user)
                key = pyotp.random_base32()
                totp = pyotp.TOTP(key)
                cod = totp.now()
                trimitereOTPMail(cod, email)
                print("ACESTA ESTE CODUL: ", cod)
                session['cod'] = cod
                
                return redirect(url_for('views.verify'))
            
            else:
                flash('incorrect password', category='error')
        else:
            flash('email does not exist', category='error')
        
    return render_template("auth.html", user = current_user)

@auth.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

@auth.route('/sign-up', methods=['GET', 'POST'])
@login_required
def sign_up():
    if request.method == 'POST':
        email = request.form.get('email')
        password1 = request.form.get('password1')
        password2 = request.form.get('password2')
        
        user = Users.query.filter_by(username=email).first()
        if user:
            flash('user already exist', category='error')
        if len(email) < 4:
            flash('Email must be greater than 3 characters.', category='error')
        elif password1 != password2:
            flash('Passwords don\'t match.', category='error')
        elif len(password1) < 2:
            flash('Password must be at least 2 characters.', category='error')
        else:
            new_user = Users(username=email, password=generate_password_hash(
                password1, method='scrypt'))
            db.session.add(new_user)
            db.session.commit()
            login_user(new_user, remember=True)
            flash("account created", category='success')
            return redirect(url_for('views.main'))
            
    return render_template("signup.html", user=current_user)

@auth.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    # Verificăm dacă token-ul este valid
    reset_entry = PasswordResetToken.query.filter_by(token=token).first()

    if not reset_entry:
        flash("Token invalid sau expirat.", category='error')
        return redirect(url_for('auth.login'))  # Redirectăm utilizatorul la pagina de login

    # Verificăm dacă token-ul a expirat
    if reset_entry.expires_at < datetime.datetime.utcnow():
        flash("Tokenul a expirat. Te rugăm să soliciți un alt link.", category='error')
        return redirect(url_for('auth.forgot_password'))  # Redirectăm utilizatorul la pagina de "forgot password"

    # Dacă se face un POST (utilizatorul a trimis formularul cu noua parolă)
    if request.method == 'POST':
        new_password = request.form['new_password']
        confirm_password = request.form['confirm_password']

        # Verificăm dacă parolele se potrivesc
        if new_password != confirm_password:
            flash("Parolele nu se potrivesc. Te rugăm să încerci din nou.", category='error')
        else:
            # Hash-uim noua parolă
            hashed_password = generate_password_hash(new_password, method='sha256')

            # Actualizăm parola utilizatorului
            user = Users.query.get(reset_entry.user_id)
            user.password = hashed_password

            # Salvăm schimbările
            db.session.commit()

            # Ștergem token-ul din baza de date pentru a preveni reutilizarea
            db.session.delete(reset_entry)
            db.session.commit()

            flash("Parola a fost resetată cu succes!", category='success')
            return redirect(url_for('auth.login'))  # Redirecționăm utilizatorul către pagina de login

    # Dacă este un GET (se încarcă formularul pentru resetarea parolei)
    return render_template('reset_password.html', token=token)

@auth.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form.get('email')
        user = Users.query.filter_by(username=email).first()
        first_name = email.split("@")[0].split(".")[0].capitalize()


        if user:
            # Generăm un token unic pentru resetare
            token = str(uuid.uuid4())
            expiry_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)  # Valabil 15 minute

            # Salvăm token-ul în baza de date
            reset_entry = PasswordResetToken(user_id=user.id, token=token, expires_at=expiry_time)
            db.session.add(reset_entry)
            db.session.commit()

            # Trimitem email utilizatorului
            reset_link = url_for('auth.reset_password', token=token, _external=True)
            subject = 'Password Reset Link'
            body = f'Hi {first_name},\n\nHere is a link for password reset: {reset_link}\nIt will expire in 15 minutes.😊\n\nThank you,\nGTR Digital'
            send_email_via_graph_api(subject, email, body)

            flash("Instructions were sent to email.😊", category='success')
        else:
            flash("This account does not exist.😢", category='error')

    return render_template("forgot_password.html")


@auth.route('/create-user', methods=['GET', 'POST'])
@login_required
@admin_required
def create_new_user():
    if request.method == 'POST':
        email = request.form.get('email')
        role = request.form.get("role")

        user = Users.query.filter_by(username=email).first()
        if user:
            flash('User already exists', category='error')
            return render_template("signup.html", user=current_user)

        if len(email) < 4:
            flash('Email must be greater than 3 characters.', category='error')
        else:
            # Generare automată a parolei
            email_parts = email.split("@")[0].split(".")  # Separă numele și prenumele
            first_initial = email_parts[0][0] if email_parts else ''  # Prima literă din prenume
            last_name = email_parts[1] if len(email_parts) > 1 else ''  # Numele de familie
            random_number = random.randint(1, 20)  # Număr aleatoriu între 1 și 20
            generated_password = f"{first_initial}{last_name}_RAS{random_number}"
            
            # Creare utilizator cu parola generată
            new_user = Users(username=email, password=generate_password_hash(generated_password, method='scrypt'), role=role)
            db.session.add(new_user)
            db.session.commit()
            login_user(new_user, remember=True)
            flash("Account created", category='success')

            # Trimite email cu parola generată
            first_name = email_parts[0].capitalize()  # Prima parte a emailului, capitalizată
            link_platforma = url_for('auth.login', _external=True)  # Endpoint valid pentru login
            subject = 'Creare cont RAS'
            body = f'''Bună {first_name},

Contul tău în aplicația RAS tocmai a fost creat. 
Datele tale de autentificare sunt:

- Email: {email}
- Parolă: {generated_password}

Te rugăm să accesezi platforma folosind următorul link: {link_platforma}

Poți schimba parola oricând din secțiunea contul tău, secțiunea Schimbă parola.

Mulțumim,
GTR Digital'''
            
            send_email_via_graph_api(subject, email, body)
            print(f"S-a trimis mail la {first_name} cu parola {generated_password}")

            return redirect(url_for('views.main'))

    return render_template("signup.html", user=current_user)


