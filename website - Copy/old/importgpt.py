import pymysql
import requests

# Conectați-vă la baza de date MySQL
connection = pymysql.connect(host='localhost',
                             user='username',
                             password='password',
                             database='nume_baza_de_date')

# Creați un cursor pentru a executa interogările SQL
cursor = connection.cursor()

# Interogați baza de date pentru a obține toate valorile din coloana `regno`
cursor.execute("SELECT regno FROM nume_tabela")

# Obțineți toate valorile unice din coloana `regno`
regno_values = [row[0] for row in cursor.fetchall()]

# Parcurgeți fiecare valoare `regno`
for regno in regno_values:
    # Faceți cererea către API cu valoarea `regno`
    response = requests.get(f'https://url-api/{regno}')
    
    # Verificați dacă răspunsul este cu succes
    if response.status_code == 200:
        # Extrageți datele din răspunsul API-ului
        data = response.json()
        
        # Actualizați înregistrarea corespunzătoare în baza de date cu informațiile primite
        update_query = "UPDATE nume_tabela SET Name=%s, Country=%s, City=%s, Street=%s, region=%s WHERE regno=%s"
        values = (data['Name'], data['Country'], data['City'], data['Street'], data['region'], regno)
        cursor.execute(update_query, values)
        connection.commit()

# Închideți cursorul și conexiunea la baza de date
cursor.close()
connection.close()
