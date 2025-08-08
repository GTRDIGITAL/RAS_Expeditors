import mysql.connector as mysql
# from .views import abc

def procedura_mapare():
    conn = mysql.connect(
        host="efactura.mysql.database.azure.com",
        user="useradmin",
        password="Ho1*,3v1PBRY075a^d5-",
        database="soft_contabil_database"
    )
    cursor = conn.cursor()

    print("[INFO] Încep maparea completă GL+BR+JT...")
    cursor.execute("""
        UPDATE general_ledger gl
        JOIN mapping m
          ON gl.GL = m.GL AND gl.BR = m.BR AND gl.JT = m.transaction_type AND gl.TC = m.TC
        SET gl.statutory_GL = m.Statutory_GL
    """)
    conn.commit()
    print(f"[INFO] Mapare completă terminată. Rânduri afectate: {cursor.rowcount}")

    print("[INFO] Încep maparea fallback GL+JT (BR IS NULL)...")
    cursor.execute("""
        UPDATE general_ledger gl
        JOIN mapping m
          ON gl.GL = m.GL AND m.BR IS NULL AND gl.JT = m.transaction_type  AND  m.TC IS NULL
        SET gl.statutory_GL = m.Statutory_GL
        WHERE gl.statutory_GL IS NULL
    """)
    conn.commit()
    print(f"[INFO] Mapare fallback terminată. Rânduri afectate: {cursor.rowcount}")
    print("[INFO] Pasul 3: Mapare fallback GL + BR + JT (TC IS NULL)...")
    cursor.execute("""
    UPDATE general_ledger gl
    JOIN mapping m
      ON gl.GL = m.GL
     AND gl.BR = m.BR
     AND gl.JT = m.transaction_type
     AND m.TC IS NULL
    SET gl.statutory_GL = m.Statutory_GL
    WHERE gl.statutory_GL IS NULL
""")
    conn.commit()
    print(f"[INFO] ✅ Pasul 3 terminat. Rânduri afectate: {cursor.rowcount}")
    print("[INFO] Încep maparea completă GL+BR+JT...")
    cursor.execute("""
        UPDATE general_ledger gl
        JOIN mapping m
          ON gl.GL = m.GL  AND gl.JT = m.transaction_type AND gl.TC = m.TC
        SET gl.statutory_GL = m.Statutory_GL
    """)
    conn.commit()
    print(f"[INFO] Mapare completă terminată. Rânduri afectate: {cursor.rowcount}")
#     cursor.execute("""
#     DELETE FROM general_ledger
#     WHERE JT = 'EXT'
# """)
#     conn.commit()
    
    cursor.close()
    conn.close()
    print("🎉 Actualizare finalizată cu succes.")
    return "Actualizare finalizată cu succes."
  
  
def generare_sold_clienti():
    connection = mysql.connect(
        host="efactura.mysql.database.azure.com",
        user="useradmin",
        password="Ho1*,3v1PBRY075a^d5-",
        database="soft_contabil_database"
    )
    cursor = connection.cursor()  # Nu e nevoie de dictionary=True pentru INSERT

    query = """
        INSERT INTO sold_clienti
        SELECT *
        FROM general_ledger_test
        WHERE JT IN ('INV', 'XINV', 'ICR')
       
        ORDER BY Year, Month;
    """

    cursor.execute(query)
    connection.commit()  # Salvează modificările

    rows_inserted = cursor.rowcount  # Numărul de rânduri inserate

    cursor.close()
    connection.close()

    return f"Actualizare finalizată cu succes. {rows_inserted} rânduri inserate."
# procedura_mapare()
def procedura_mapare_period(month, year):
    conn = mysql.connect(
        host="efactura.mysql.database.azure.com",
        user="useradmin",
        password="Ho1*,3v1PBRY075a^d5-",
        database="soft_contabil_database"
    )
    cursor = conn.cursor()

    print(f"[INFO] Încep maparea pentru luna {month}/{year}...")

    period_filter = "WHERE gl.Month = %s AND gl.Year = %s"
    period_filter_where = "AND gl.Month = %s AND gl.Year = %s"
    params = (month, year)

    queries = [
        ("""
            UPDATE general_ledger gl
            JOIN mapping m ON gl.GL = m.GL AND gl.BR = m.BR AND gl.JT = m.transaction_type AND gl.TC = m.TC
            SET gl.statutory_GL = m.Statutory_GL
        """ + period_filter, params),

        ("""
            UPDATE general_ledger gl
            JOIN mapping m ON gl.GL = m.GL AND m.BR IS NULL AND gl.JT = m.transaction_type AND m.TC IS NULL
            SET gl.statutory_GL = m.Statutory_GL
            WHERE gl.statutory_GL IS NULL
        """ + period_filter_where, params),

        ("""
            UPDATE general_ledger gl
            JOIN mapping m ON gl.GL = m.GL AND gl.BR = m.BR AND gl.JT = m.transaction_type AND m.TC IS NULL
            SET gl.statutory_GL = m.Statutory_GL
            WHERE gl.statutory_GL IS NULL
        """ + period_filter_where, params),

        ("""
            UPDATE general_ledger gl
            JOIN mapping m ON gl.GL = m.GL AND gl.JT = m.transaction_type AND gl.TC = m.TC
            SET gl.statutory_GL = m.Statutory_GL
        """ + period_filter, params),
    ]

    for idx, (query, p) in enumerate(queries, 1):
        print(f"[INFO] Execut query {idx}...")
        cursor.execute(query, p)
        conn.commit()
        print(f"[INFO] Rânduri afectate: {cursor.rowcount}")

    # print(f"[INFO] Șterg EXT pentru {month}/{year}...")
    # cursor.execute("""
    #     DELETE FROM general_ledger
    #     WHERE JT = 'EXT' AND Month = %s AND Year = %s
    # """, params)
    # conn.commit()
    # print(f"[INFO] Rânduri EXT șterse: {cursor.rowcount}")

    cursor.close()
    conn.close()
    print("🎉 Mapare finalizată pentru luna selectată.")
    return "Mapare finalizată cu succes."
