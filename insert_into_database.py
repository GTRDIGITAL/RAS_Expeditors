import pandas as pd
import mysql.connector

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
df = pd.read_excel(r"C:\Dezvoltare\RAS\RAS Expeditors\Chart of accounts 1224 (003).xlsx.xlsm")
df.columns = df.columns.str.strip()  # elimină spațiile de la început/final

# Query de inserare
insert_mapping_query = """
    INSERT INTO mapping (GL, Br, Statutory_GL, Statutory_Type, Transaction_Type, Headers)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

mapping_batch = []
batch_size = 1000

for idx, row in df.iterrows():
    try:
        GL_raw = row.get("GL")
        if pd.isnull(GL_raw):
            continue
        GL = int(GL_raw)

        Br_raw = row.get("Br")
        Br = int(Br_raw) if not pd.isnull(Br_raw) else None

        Statutory_GL = row.get("Statutory GL")
        Statutory_GL = None if pd.isnull(Statutory_GL) else str(Statutory_GL).strip()

        Statutory_Type = row.get("Statutory Type")
        Statutory_Type = None if pd.isnull(Statutory_Type) else str(Statutory_Type).strip()

        Transaction_Type = row.get("Transaction Types")
        Transaction_Type = None if pd.isnull(Transaction_Type) else str(Transaction_Type).strip()

        Headers = row.get("Headers")
        Headers = None if pd.isnull(Headers) else str(Headers).strip()

        # Opțional: sari peste rândurile fără Transaction_Type dacă vrei
        # if not Transaction_Type:
        #     continue

        mapping_batch.append((GL, Br, Statutory_GL, Statutory_Type, Transaction_Type, Headers))

        if len(mapping_batch) >= batch_size:
            cursor.executemany(insert_mapping_query, mapping_batch)
            conn.commit()
            mapping_batch.clear()

    except Exception as e:
        print(f"❌ Eroare la rândul {idx + 2}: {e}")

# Flush final
if mapping_batch:
    cursor.executemany(insert_mapping_query, mapping_batch)
    conn.commit()

cursor.close()
conn.close()

print("✅ Inserare completă în tabela 'mapping'.")
