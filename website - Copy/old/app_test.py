from flask import Flask, render_template, request, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user

import mysql.connector

app = Flask(__name__)
app.secret_key = 'secret_key_here'

login_manager = LoginManager()
login_manager.init_app(app)

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="efactura"
)

class User(UserMixin):
    pass

@login_manager.user_loader
def load_user(user_id):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE username = %s", (user_id,))
    user_data = cursor.fetchone()
    cursor.close()

    if user_data:
        user = User()
        user.id = user_id
        return user
    return None

@app.route('/')
# @login_required
def index():
    return 'Welcome! You are logged in.'

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s AND password = %s", (username, password))
        user_data = cursor.fetchone()
        cursor.close()

        if user_data:
            user = User()
            user.id = username
            login_user(user)
            return redirect(url_for('index'))

    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
