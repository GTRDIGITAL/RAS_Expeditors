import pandas as pd
import requests
# import mysql.connector
import json
import pymysql

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']

conn = pymysql.connect(
    host=mysql_config['host'],
    user=mysql_config['user'],
    password=mysql_config['password'],
    database=mysql_config['database']
)
cursor = conn.cursor(buffered=True)

# df = pd.read_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/outs/client.xlsx")
df = pd.read_excel("C:\\Dezvoltare\\E-Factura\\2023\\eFactura\\Ferro\\eFacturaFerro\\Baza de date vanzari\\client.xlsx")

info = []
dataCautare = '2024-01-19'
url_api = 'https://webservicesp.anaf.ro/PlatitorTvaRest/api/v8/ws/tva'

for index, row in df.iterrows():
    cui_without_prefix = str(row['RegNo']).replace('RO', '')
    payload = [{'cui': cui_without_prefix, 'data': str(dataCautare)}]
    response = requests.post(url_api, json=payload)
    while True:
        if response.status_code == 200:
            date_api_complete = response.json()

            if 'found' in date_api_complete and date_api_complete['found']:
                for found_item in date_api_complete['found']:
                    clientName = str(found_item["date_generale"]["denumire"])
                    country = 'RO'
                    cui = str(found_item['date_generale']['cui'])
                    city = str(found_item["adresa_sediu_social"]["sdenumire_Localitate"])
                    street = str(found_item["adresa_sediu_social"]["sdenumire_Strada"]) + str(found_item["adresa_sediu_social"]["snumar_Strada"])
                    regiune = str(found_item["adresa_domiciliu_fiscal"]['dcod_JudetAuto'])
                    
                    # Corectare a variabilei "sector"
                    sector = "SECTOR" + str(found_item["adresa_sediu_social"]["scod_Localitate"])

                    # Printează informațiile (opțional)
                    # print(f'Client: {clientName}, Țară: {country}, CUI: {cui}, Oraș: {city}, Stradă: {street}, Regiune: {regiune}, Sector: {sector}')
                    
                    # Construiește dicționarul cu informațiile și adaugă-l la lista "info"
                    info_dict = {
                        "clientName": clientName,
                        'country': country,
                        'cui': cui,
                        "city": sector if 'Sector' in city else city,  # Verifică și înlocuiește "Bucuresti" cu "sector" dacă este cazul
                        "street": street,
                        'regiune': regiune,
                    }
                    info.append(info_dict)
            else:
                print(f'Nu s-au găsit informații pentru CUI-ul: {cui_without_prefix}')
            break
# print("ASTA E INFO ", info)


for item in info:
    cui = "RO" + str(item['cui'])
    print(cui, item['clientName'], item["regiune"], item["city"])
    regiune = item['regiune']
    city = item['city']
    print(city)

    try:
        cursor.execute("SELECT * FROM clients WHERE regno = %s", (item["cui"],))
        existing_data = cursor.fetchone()
        print(existing_data)
        try:
            if existing_data:
                cursor.execute("UPDATE clients SET name = %s, country=%s, regno=%s, city=%s, street=%s, region = %s WHERE regno = %s", (item["clientName"], item["country"], cui, item['city'], item["street"],item["regiune"], item["cui"]))
            else:
                # cursor.execute("INSERT INTO clients (name, country, regno, city, street, region) VALUES (%s, %s, %s, %s, %s, %s)", (item["clientName"], item["country"], item["cui"], item["city"], item["street"],item["regiune"]))
                print("ia la muie")
                # print(existing_data)
        except Exception as e:
                print(f'Eroare: {e}')
                
    except pymysql.Error as err:
        print(f"Eroare MySQL: {err}")
        print(f"Datele care au provocat eroarea: cui={cui}, regiune={regiune}")
        pass
# Facem commit după ce am terminat operațiile cu baza de date
conn.commit()
conn.close()

