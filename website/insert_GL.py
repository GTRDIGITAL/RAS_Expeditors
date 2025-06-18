# import pandas as pd
# import mysql.connector
# from datetime import datetime
# import time
# import json


# start_time = time.time()

# def citeste_configurare(file_path):
#     with open(file_path, 'r') as file:
#         config = json.load(file)
#     return config

# config = citeste_configurare('config.json')
# mysql_config = config['mysql']


# def import_into_db(file):
# # Conectare la baza de date
#     conn = mysql.connector.connect(
#             host=mysql_config['host'],
#             user=mysql_config['user'],
#             password=mysql_config['password'],
#             database=mysql_config['database']
#         )
#     cursor = conn.cursor()
#     conn.autocommit = False  # pentru performanță

#     # Citește fișierul Excel
#     df = pd.read_excel(file)
#     df.columns = df.columns.str.strip()

#     # Funcție pentru a converti datele în format datetime.date
#     def parse_date(value):
#         if pd.isna(value):
#             return None
#         if isinstance(value, datetime):
#             return value.date()
#         for fmt in ("%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
#             try:
#                 return datetime.strptime(str(value), fmt).date()
#             except ValueError:
#                 continue
#         return None

#     # Interogare INSERT
#     insert_query = """
#         INSERT INTO general_ledger (
#             JT, GL, BR, statutory, Prod, GL_Type, GL_Group, GL_Subtype, GL_Cat,
#             Journal, GCI, GCI_Br, Company, Open_Item, File_Ref, Date,
#             Month, Year, TC, Amount, Foreign_Amount, Foreign_Currency,
#             External_Ref, MBL, IC, House, BC, Billing_Description,
#             Customer_GCI, Customer_Name, data_Description, GL_Description,
#             GL_Local_Description, Post_Date, Last_Modifier, Approver,
#             Acct_Tran_Id, RowNumber, Commissionable, data_Source
#         )
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """

#     # Batch insert
#     batch = []
#     batch_size = 1000
#     total_inserted = 0

#     for idx, row in df.iterrows():
#         row = row.where(pd.notnull(row), None)
#         try:
#             values = (
#                 row.get("JT"), row.get("GL"), row.get("BR"), row.get("Statutory"),
#                 row.get("Prod"), row.get("GL Type"), row.get("GL Group"), row.get("GL Subtype"),
#                 row.get("GL Cat"), row.get("Journal"), row.get("GCI"), row.get("GCI Br"),
#                 row.get("Company"), row.get("Open Item"), row.get("File Ref"), parse_date(row.get("Date")),
#                 int(row.get("Month")) if row.get("Month") is not None else None,
#                 int(row.get("Year")) if row.get("Year") is not None else None,
#                 row.get("TC"), float(row.get("Amount")) if row.get("Amount") is not None else None,
#                 float(row.get("Foreign Amount")) if row.get("Foreign Amount") is not None else None,
#                 row.get("Foreign Currency"), row.get("External Ref"), row.get("MBL"), row.get("IC"),
#                 row.get("House"), row.get("BC"), row.get("Billing Description"), row.get("Customer GCI"),
#                 row.get("Customer Name"), row.get("Description"), row.get("GL Description"),
#                 row.get("GL Local Description"), parse_date(row.get("Post Date")), row.get("Last Modifier"),
#                 row.get(" Approver"), row.get("Acct Tran Id"),
#                 int(row.get("Row Number")) if row.get("Row Number") is not None else None,
#                 row.get("Commissionable"), row.get("Source")
#             )
#             batch.append(values)
#         except Exception as e:
#             print(f"❌ Eroare la rândul {idx + 2}: {e}")

#         if len(batch) >= batch_size:
#             cursor.executemany(insert_query, batch)
#             total_inserted += len(batch)
#             batch = []

#     # Inserează ultimele rânduri
#     if batch:
#         cursor.executemany(insert_query, batch)
#         total_inserted += len(batch)

#     # Commit o singură dată
#     conn.commit()
#     cursor.close()
#     conn.close()

#     end_time = time.time()
#     duration = end_time - start_time
#     minutes = int(duration // 60)
#     seconds = duration % 60

#     print(f"✅ Inserarea a fost finalizată. {total_inserted} rânduri au fost inserate.")
#     print(f"⏱️ Durata inserării: {minutes} minute și {seconds:.2f} secunde.")

import pandas as pd
import mysql.connector
from datetime import datetime
import json

def import_into_db(file):
    """Imports data from an Excel file into a MySQL database."""

    # Database configuration
    def citeste_configurare(file_path):
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config

    config = citeste_configurare('config.json')
    mysql_config = config['mysql']


    # Conectare la baza de date
    conn = mysql.connector.connect(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database']
    )
    cursor = conn.cursor()
    conn.autocommit = False  # pentru performanță

    # Citește fișierul Excel
    df = pd.read_excel(file)
    df.columns = df.columns.str.strip()
    print(df.columns, 'astea sunt coloanele')  # Debug: afișează coloanele citite

    # Funcție pentru a converti datele în format datetime.date
    def parse_date(value):
        if pd.isna(value):
            return None
        if isinstance(value, datetime):
            return value.date()
        for fmt in ("%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(str(value), fmt).date()
            except ValueError:
                continue
        return None

    # Interogare INSERT
    insert_query = """
        INSERT INTO general_ledger (
            JT, GL, BR, statutory, Prod, GL_Type, GL_Group, GL_Subtype, GL_Cat,
            Journal, GCI, GCI_Br, Company, Open_Item, File_Ref, Date,
            Month, Year, TC, Amount, Foreign_Amount, Foreign_Currency,
            External_Ref, MBL, IC, House, BC, Billing_Description,
            Customer_GCI, Customer_Name, data_Description, GL_Description,
            GL_Local_Description, Post_Date, Last_Modifier, Approver,
            Acct_Tran_Id, RowNumber, Commissionable, data_Source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Batch insert
    batch = []
    batch_size = 1000
    total_inserted = 0

    for idx, row in df.iterrows():
        row = row.where(pd.notnull(row), None)
        try:
            values = (
                row.get("JT"), row.get("GL"), row.get("BR"), row.get("Statutory"),
                row.get("Prod"), row.get("GL Type"), row.get("GL Group"), row.get("GL Subtype"),
                row.get("GL Cat"), row.get("Journal"), row.get("GCI"), row.get("GCI Br"),
                row.get("Company"), row.get("Open Item"), row.get("File Ref"), parse_date(row.get("Date")),
                int(row.get("Month")) if row.get("Month") is not None else None,
                int(row.get("Year")) if row.get("Year") is not None else None,
                row.get("TC"), float(row.get("Amount")) if row.get("Amount") is not None else None,
                float(row.get("Foreign Amount")) if row.get("Foreign Amount") is not None else None,
                row.get("Foreign Currency"), row.get("External Ref"), row.get("MBL"), row.get("IC"),
                row.get("House"), row.get("BC"), row.get("Billing Description"), row.get("Customer GCI"),
                row.get("Customer Name"), row.get("Description"), row.get("GL Description"),
                row.get("GL Local Description"), parse_date(row.get("Post Date")), row.get("Last Modifier"),
                row.get(" Approver"), row.get("Acct Tran Id"),
                int(row.get("Row Number")) if row.get("Row Number") is not None else None,
                row.get("Commissionable"), row.get("Source")
            )
            batch.append(values)
        except Exception as e:
            print(f"❌ Eroare la rândul {idx + 2}: {e}")

        if len(batch) >= batch_size:
            cursor.executemany(insert_query, batch)
            total_inserted += len(batch)
            batch = []

    # Inserează ultimele rânduri
    if batch:
        cursor.executemany(insert_query, batch)
        total_inserted += len(batch)

    # Commit o singură dată
    conn.commit()
    cursor.close()
    conn.close()
    
# import_into_db('D:\\Projects\\21. SAF-T Expeditors\\2. Fisiere input\\1\\REGISTRU JURNAL.xlsx')  # Exemplu de apelare a funcției