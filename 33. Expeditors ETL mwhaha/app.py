
from flask import Flask, Blueprint, render_template, request, flash, redirect, url_for, session, send_from_directory, send_file
# from .models import Users
# from . import db 
# from werkzeug.security import generate_password_hash, check_password_hash
# # from flask_login import LoginManager, login_user, login_required, logout_user, current_user
# import random
# import smtplib
# import ssl
# import datetime
# import pyotp
# import smtplib, ssl
# import base64
# import pandas as pd
# import os
# from werkzeug.utils import secure_filename
# # from trimitereCodOTP import *
# # from sendMails import *
# from time import sleep
# import numpy as np
# import zipfile
# # from models import *
# import shutil
# from .sendMails import *
app = Flask(__name__)

from flask import Flask, render_template

app = Flask(__name__)

# Funcție pentru extragerea numelui dintr-o adresă de email
def extract_name_from_email(email):
    # Extragem partea de înainte de "@" din email
    local_part = email.split('@')[0]

    # Înlocuim punctele și cratimele cu spații
    name_parts = local_part.replace('.', ' ').replace('-', ' ').split(' ')

    # Formăm numele cu litere mari la început și restul mici
    formatted_name = ' '.join([part.capitalize() for part in name_parts])
    
    return formatted_name
@app.route('/login')
def login():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("login.html", user_name=user_name)
@app.route('/verify')
def verify():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("verify.html", user_name=user_name)
@app.route('/forgot-pass')
def forgot():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("forgot-pass.html", user_name=user_name)
@app.route('/change-pass')
def change():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("change-pass.html", user_name=user_name)
@app.route('/')
def home():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("main.html", user_name=user_name)
@app.route('/create-user')
def createuser():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("create-user.html", user_name=user_name)
@app.route('/view-users')
def viewusers():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("view-users.html", user_name=user_name)
@app.route('/transform')
def transform():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("load&transform.html", user_name=user_name)
@app.route('/reports')
def reports():
    email = "Ferdis.Curban@expeditors.com"  # Exemplu de email
    user_name = extract_name_from_email(email)  # Extragem numele
    
    return render_template("vizualizare rapoarte.html", user_name=user_name)
# if __name__ == '__main__':
#     app.run(debug=True)


# @app.route('/')
# def home():
#     email="ferdis.curban@expeditors.com"
#     return render_template("main.html", email=email)


app.run(debug="True",host="0.0.0.0", port=1996) 