import mysql.connector

def procedura_mapare():
    conn = mysql.connector.connect(
        host="efactura.mysql.database.azure.com",
        user="useradmin",
        password="Ho1*,3v1PBRY075a^d5-",
        database="soft_contabil_database"
    )
    cursor = conn.cursor(dictionary=True)

    batch_size = 1000
    offset = 0

    # Preluăm toate mapările odată, în memorie
    cursor.execute("""
        SELECT 
            CONCAT(GL, '-', IFNULL(BR, 'NULL'), '-', transaction_type) AS key_full,
            GL, BR, transaction_type,
            Statutory_GL
        FROM mapping
    """)
    mapari = cursor.fetchall()

    # Cream două dictionare:
    # 1. pentru GL+BR+JT
    # 2. fallback pentru GL+JT (când BR e null)
    map_full = {}
    map_fallback = {}

    for m in mapari:
        key = f"{m['GL']}-{m['BR'] if m['BR'] is not None else 'NULL'}-{m['transaction_type']}"
        map_full[key] = m['Statutory_GL']

        if m['BR'] is None:
            key_fb = f"{m['GL']}-{m['transaction_type']}"
            map_fallback[key_fb] = m['Statutory_GL']

    print("✅ Mapările au fost încărcate în memorie.")

    # Procesăm batch-uri din general_ledger
    while True:
        cursor.execute(f"""
            SELECT id, GL, BR, JT
            FROM general_ledger
            ORDER BY id
            LIMIT {batch_size} OFFSET {offset}
        """)
        rows = cursor.fetchall()
        if not rows:
            break

        updates = []

        for row in rows:
            gl = row["GL"]
            br = row["BR"]
            jt = row["JT"]
            id_ = row["id"]

            key_full = f"{gl}-{br if br is not None else 'NULL'}-{jt}"
            statutory = map_full.get(key_full)

            if not statutory and br is not None:
                key_fallback = f"{gl}-{jt}"
                statutory = map_fallback.get(key_fallback)

            if statutory:
                updates.append((statutory, id_))

        if updates:
            cursor.executemany("""
                UPDATE general_ledger
                SET statutory_GL = %s
                WHERE id = %s
            """, updates)
            conn.commit()

        print(f"✅ Batch complet: {offset} - {offset + batch_size}")
        offset += batch_size

    cursor.close()
    conn.close()
    print("🎉 Actualizare finalizată cu succes.")
