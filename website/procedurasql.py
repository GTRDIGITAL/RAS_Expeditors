import mysql.connector

def procedura_mapare():
    conn = mysql.connector.connect(
        host="efactura.mysql.database.azure.com",
        user="useradmin",
        password="Ho1*,3v1PBRY075a^d5-",
        database="soft_contabil_database"
    )
    cursor = conn.cursor(dictionary=True)

    batch_size = 500
    offset = 0

    while True:
        # Selectăm batch de rânduri din general_ledger
        cursor.execute(f"""
            SELECT id, GL, BR, JT
            FROM general_ledger
            ORDER BY id
            LIMIT {batch_size} OFFSET {offset}
        """)
        rows = cursor.fetchall()

        if not rows:
            break  # Gata

        updates = []
        for row in rows:
            gl = row["GL"]
            br = row["BR"]
            jt = row["JT"]
            id_ = row["id"]

            # Mai întâi încercăm match pe GL + BR + JT
            cursor.execute("""
                SELECT Statutory_GL
                FROM mapping
                WHERE GL = %s AND BR = %s AND transaction_type = %s
                LIMIT 1
            """, (gl, br, jt))
            result = cursor.fetchone()

            if not result and br is not None:
                # Dacă nu merge cu BR, mai încercăm fără BR
                cursor.execute("""
                    SELECT Statutory_GL
                    FROM mapping
                    WHERE GL = %s AND BR IS NULL AND transaction_type = %s
                    LIMIT 1
                """, (gl, jt))
                result = cursor.fetchone()

            if result:
                updates.append((result["Statutory_GL"], id_))

        # Executăm update-urile
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
