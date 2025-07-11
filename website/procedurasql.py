import mysql.connector as mysql
from .views import abc

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
        UPDATE general_ledger_test gl
        JOIN mapping m
          ON gl.GL = m.GL AND gl.BR = m.BR AND gl.JT = m.transaction_type
        SET gl.statutory_GL = m.Statutory_GL
    """)
    conn.commit()
    print(f"[INFO] Mapare completă terminată. Rânduri afectate: {cursor.rowcount}")

    print("[INFO] Încep maparea fallback GL+JT (BR IS NULL)...")
    cursor.execute("""
        UPDATE general_ledger_test gl
        JOIN mapping m
          ON gl.GL = m.GL AND m.BR IS NULL AND gl.JT = m.transaction_type
        SET gl.statutory_GL = m.Statutory_GL
        WHERE gl.statutory_GL IS NULL
    """)
    conn.commit()
    print(f"[INFO] Mapare fallback terminată. Rânduri afectate: {cursor.rowcount}")

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
          AND GL_Type != 'Asset'
        ORDER BY Year, Month;
    """

    cursor.execute(query)
    connection.commit()  # Salvează modificările

    rows_inserted = cursor.rowcount  # Numărul de rânduri inserate

    cursor.close()
    connection.close()

    return f"Actualizare finalizată cu succes. {rows_inserted} rânduri inserate."