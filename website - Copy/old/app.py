from flask import session, Flask, request, render_template, redirect, send_file, url_for, jsonify, send_from_directory
from flask_login import LoginManager, login_user, login_required, UserMixin
import time
import pythoncom
import win32com.client as win32
import os
from apeluri_efactura import *
import apeluri_efactura
import xml.etree.ElementTree as ET
from prettytable import PrettyTable
import json
from stocareBD import *
# from login import *
# from userClass import User

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="efactura"
)


app = Flask(__name__)
login_manager = LoginManager()
login_manager.init_app(app)

# @login_manager.user_loader
# def load_user(user_id):
#     return User.get(user_id)

@app.route('/')
def index():    
    return render_template('auth.html')

@app.route('/welcome', methods=['POST', 'GET'])

def welcome():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
    return render_template('pagina_excel.html', username=username, password=password)


@app.route('/main', methods=['POST'])
@login_manager.user_loader
def load_user():
    return render_template('main.html')
    
@app.route('/summary', methods=['POST'])
def summary():
    username = request.form.get('username')
    password = request.form.get('password')
    return render_template('summary.html', username=username, password=password)
    

@app.route('/fail', methods=['GET'])
def fail():
    return render_template('fail.html')

@app.route('/download_excel')
def download_excel():
    excel_file_path = 'C:\\Dezvoltare\\E-Factura\\2023\\demo flint\\templates\\Status.xlsx'
    return send_file(excel_file_path, as_attachment=True, download_name='Raport status.xlsx')

@app.route('/trimitere_anaf', methods=['GET'])
def trimitere_anaf():
    # current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    if request.method == 'GET':
        filename = 'rezultat '+str(current_datetime)+'.zip'
        eFactura()
        listaMesajeEroare2 = listaMesajeEroare
        print("mergi fa ", listaMesajeEroare2)
        

    return send_from_directory('C:\\Dezvoltare\\E-Factura\\2023\\output arhive conversie PDF', filename, as_attachment = True)

@app.route("/statusFacturi")
def statusFacturi():
    listaMesajeRulareCurenta = listaFactt
    print(listaMesajeRulareCurenta, " aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

    mesaje = interogareTabela() 
    return render_template("status spv tabel.html", mesaje=mesaje, listaMesajeRulareCurenta=listaMesajeRulareCurenta)


@app.route('/status', methods=['POST'])
def status():
    username = request.form.get('username')
    password = request.form.get('password')
    # time.sleep(3)
    return render_template('status.html', username=username, password=password)
    # return render_template('status.html')
if __name__ == '__main__':
    app.run(debug="True",host="0.0.0.0", port=3002)

