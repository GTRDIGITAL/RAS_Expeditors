from flask import Blueprint, render_template, request, flash, redirect, url_for
from models import Users
from __init__ import db 
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, login_required, logout_user, current_user
import random
import smtplib
import ssl
import datetime

auth = Blueprint('auth', __name__)

@auth.route('/', methods=['GET', 'POST'])
def login():
    
    if request.method == 'POST':
        email = request.form.get('username')
        password = request.form.get('password')
        print("trece pe aici")
        
        user = Users.query.filter_by(username=email).first()
        
        if user:
            if check_password_hash(user.password, password):
                flash('Logged in!', category='success')
                login_user(user, remember=True)
                print(user)
                
                return redirect(url_for('views.main'))
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
                password1, method='sha256'), token='12321321321')
            db.session.add(new_user)
            db.session.commit()
            login_user(new_user, remember=True)
            flash("account created", category='success')
            return redirect(url_for('views.main'))
            
    return render_template("signup.html", user=current_user)