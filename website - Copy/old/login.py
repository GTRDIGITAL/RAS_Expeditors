import mysql.connector
from werkzeug.security import check_password_hash

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="efactura"
)

mycursor = mydb.cursor()

def authenticate_user(username, password):
    try:
        sql = "SELECT password FROM users WHERE username = %s"
        val = (username,)
        mycursor.execute(sql, val)
        hashed_password = mycursor.fetchone()

        if hashed_password and check_password_hash(hashed_password[0], password):
            print("Autentificare cu succes")
        else:
            raise ValueError("Eroare la autentificare")
    except mysql.connector.Error as err:
        return f"Eroare la interogare: {err}"

username = input("username: ")
password = input("password: ")
authenticate_user(username, password)