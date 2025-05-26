import pandas as pd
import mysql.connector

def parse_transaction_types(s):
    if pd.isnull(s) or not str(s).strip():
        return []
    s = str(s).strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    items = [item.strip().strip("'\"") for item in s.split(",") if item.strip()]
    return items

# Conectare la baza de date
conn = mysql.connector.connect(
    host="efactura.mysql.database.azure.com",
    user="useradmin",
    password="Ho1*,3v1PBRY075a^d5-",
    database="soft_contabil_database"
)
cursor = conn.cursor()
conn.autocommit = False  # pentru performanță

# Citește fișierul Excel
df = pd.read_excel("C:\\Dezvoltare\\RAS\\RAS Expeditors\\Fisiere expeditors\\Chart of accounts 1224.xlsx")
df.columns = df.columns.str.strip()  # elimină spațiile de la început/final

# Query inserare mapping
insert_mapping_query = """
    INSERT INTO mapping (GL, BR, Statutory_GL, Statutory_Type, Headers)
    VALUES (%s, %s, %s, %s, %s)
"""

# Query inserare transaction types
insert_transaction_type_query = """
    INSERT INTO mapping_transaction_types (GL, BR, transaction_type)
    VALUES (%s, %s, %s)
"""

mapping_batch = []
transaction_batch = []
batch_size = 1000

for idx, row in df.iterrows():
    try:
        GL_raw = row.get("GL")
        if pd.isnull(GL_raw):
            # GL e obligatoriu, sarim peste randul asta
            continue
        GL = str(int(GL_raw))

        BR_raw = row.get("Br")
        if pd.isnull(BR_raw):
            BR = None
        else:
            BR = str(int(BR_raw))

        Statutory_GL = row.get("Statutory GL")
        Statutory_Type = row.get("Statutory Type")
        Headers = row.get("Headers")

        mapping_batch.append((GL, BR, Statutory_GL, Statutory_Type, Headers))

        # Transaction Types
        transaction_types = parse_transaction_types(row.get("Transaction Types"))
        for tt in transaction_types:
            transaction_batch.append((GL, BR, tt))

        # Inserăm pe batch dacă s-a atins dimensiunea
        if len(mapping_batch) >= batch_size:
            cursor.executemany(insert_mapping_query, mapping_batch)
            cursor.executemany(insert_transaction_type_query, transaction_batch)
            conn.commit()
            mapping_batch.clear()
            transaction_batch.clear()

    except Exception as e:
        print(f"❌ Eroare la rândul {idx + 2}: {e}")

# Ultimul flush pentru restul datelor
if mapping_batch:
    cursor.executemany(insert_mapping_query, mapping_batch)
if transaction_batch:
    cursor.executemany(insert_transaction_type_query, transaction_batch)
conn.commit()

cursor.close()
conn.close()

print("✅ Inserare completă în mapping și mapping_transaction_types.")
