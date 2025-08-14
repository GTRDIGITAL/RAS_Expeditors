# Documentație Detaliată RAS Expeditors

## 1. Procesul Principal - Import și Procesare GL (General Ledger)

### 1.1 Încărcarea Fișierului GL
**Locație:** `/upload_into_database` (în views.py)
```python
@views.route('/upload_into_database', methods=['POST'])
def upload_into_database():
```
**Ce face?**
1. Când utilizatorul apasă butonul de import:
   - Verifică folderul 'uploads' pentru fișiere noi
   - Ia cel mai recent fișier Excel încărcat
   - Pornește un task Celery pentru procesare

### 1.2 Procesarea GL (Task Celery)
**Locație:** `tasks.py`
```python
@celery.task(bind=True)
def import_gl_task(self, filepath):
```
**Ce face?**
1. Citește fișierul Excel cu pandas
2. Pentru fiecare rând:
   - Validează datele (Month, Year, GL, etc.)
   - Formatează valorile numerice
   - Pregătește pentru import în baza de date
3. Inserează datele în tabela `general_ledger`
4. Returnează status și statistici

### 1.3 Structura Tabelei general_ledger
```sql
CREATE TABLE general_ledger (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Month INT,
    Year INT,
    GL VARCHAR(255),
    Description TEXT,
    Opening_balance DECIMAL(15,2),
    Open_DC VARCHAR(1),
    MTD_Debit DECIMAL(15,2),
    MTD_Credit DECIMAL(15,2),
    YTD_Debit DECIMAL(15,2),
    YTD_Credit DECIMAL(15,2),
    Ending_balance DECIMAL(15,2),
    End_DC VARCHAR(1),
    Statutory_GL VARCHAR(255) DEFAULT NULL
);
```

## 2. Procesul de Mapare

### 2.1 Maparea GL-urilor
**Locație:** `procedurasql.py`
**Ce face?**
1. După import, sistemul mapează GL-urile:
   - Citește din tabela `mapping`
   - Pentru fiecare GL, găsește corespondentul Statutory_GL
   - Actualizează înregistrările din general_ledger
2. Exemplu de mapare:
   ```
   GL Original: 401.01 -> Statutory_GL: 401000
   GL Original: 411.02 -> Statutory_GL: 411000
   ```

### 2.2 Tabela de Mapare
```sql
CREATE TABLE mapping (
    id INT AUTO_INCREMENT PRIMARY KEY,
    GL VARCHAR(255),
    Br VARCHAR(255),
    Statutory_GL VARCHAR(255),
    Statutory_Type VARCHAR(255),
    Transaction_Type VARCHAR(255),
    Headers VARCHAR(255),
    TC VARCHAR(255)
);
```

## 3. Generarea Rapoartelor

### 3.1 Trial Balance
**Locație:** `views.py - /generate_trial_balance`
**Ce face?**
1. Primește parametri:
   - Perioada (lună început, an început, lună sfârșit, an sfârșit)
   - Tip raport (statutory sau original)
2. Extrage date din general_ledger
3. Calculează:
   - Sold inițial
   - Rulaje perioada
   - Sold final
4. Generează Excel cu formatare:
   - Headere cu fundal gri
   - Numere formatate cu 2 zecimale
   - Coloane înghețate

### 3.2 Raport Sold Clienți
**Locație:** `views.py - /genereaza_raport_sold_clienti`
**Ce face?**
1. Filtrează conturi 411*
2. Grupează după client
3. Calculează:
   - Sold inițial client
   - Rulaje debit/credit
   - Sold final
4. Generează Excel pentru fiecare GL

## 4. Sistem de Email-uri

### 4.1 Configurare Microsoft Graph API
**Locație:** `sendMails.py`
```python
tenant_id = '52089378-759b-4f24-a522-881ef92534ec'
client_id = '091ff128-d742-4f8d-96dc-2649866c782c'
```
**Ce face?**
1. Se conectează la Microsoft Graph
2. Autentifică aplicația
3. Trimite email-uri cu:
   - Rapoarte atașate
   - Template HTML personalizat
   - CC către alți destinatari (opțional)

## 5. Task-uri Asincrone (Celery)

### 5.1 Configurare Celery
**Locație:** `tasks.py`
```python
celery = Celery(
    'tasks',
    broker=app.config['CELERY_BROKER_URL'],
    backend=app.config['CELERY_RESULT_BACKEND']
)
```
**Ce face?**
1. Se conectează la Redis Azure:
   - Ca message broker (pentru task-uri)
   - Ca backend (pentru rezultate)
2. Configurează SSL pentru conexiuni sigure
3. Gestionează task-uri lungi în background

### 5.2 Monitorizare Task-uri
**Cum funcționează?**
1. Când pornește un task:
   - Frontend primește un task_id
   - Polling la fiecare 2 secunde pentru status
   - Actualizare interfață cu progres
2. Statusuri posibile:
   - PENDING: Task în așteptare
   - RUNNING: În execuție
   - SUCCESS: Complet cu succes
   - FAILURE: Eroare

## 6. Procesul de Import Pas cu Pas

### 6.1 Upload Fișier
1. Utilizatorul încarcă Excel în interfață
2. Fișierul este salvat în folder-ul 'uploads'
3. Se verifică formatul și extensia

### 6.2 Validare Date
1. Se citește Excel-ul cu pandas
2. Se verifică:
   - Prezența coloanelor obligatorii
   - Formate numere corecte
   - GL-uri valide

### 6.3 Import în Bază
1. Se pregătesc date pentru insert
2. Insert în loturi de 1000 rânduri
3. Commit la fiecare lot
4. Logging pentru erori

### 6.4 Mapare
1. Se caută GL-uri în tabela mapping
2. Se actualizează Statutory_GL
3. Se calculează totaluri pe statutory

### 6.5 Notificare
1. Se generează raport rezultate
2. Se trimite email cu status
3. Se actualizează interfața

## 7. Troubleshooting Comun

### 7.1 Erori Import
1. "Unknown column 'statutory'":
   - Verifică structura tabelei
   - Verifică query-ul de insert
   - Asigură-te că toate coloanele există

2. "Duplicate entry":
   - Verifică chei primare
   - Verifică date duplicate în Excel
   - Folosește INSERT IGNORE dacă e cazul

### 7.2 Erori Celery
1. Task-uri blocate:
   - Verifică conexiunea Redis
   - Restart worker dacă e necesar
   - Verifică memory usage

2. Task-uri multiple:
   - Verifică event listeners dublați
   - Verifică apeluri API duplicate
   - Implementează debounce în frontend

## 8. Cod Important de Urmărit

### 8.1 Import GL
```python
def import_into_db(filepath):
    try:
        df = pd.read_excel(filepath)
        print("Coloane disponibile:", df.columns.tolist())
        
        values = []
        for _, row in df.iterrows():
            value = (
                row['Month'],
                row['Year'],
                row['GL'],
                row['Description'],
                row['Opening_balance'],
                row['Open_DC'],
                row['MTD_Debit'],
                row['MTD_Credit'],
                row['YTD_Debit'],
                row['YTD_Credit'],
                row['Ending_balance'],
                row['End_DC']
            )
            values.append(value)
```
**Ce face fiecare parte:**
1. `pd.read_excel()`: Citește toate datele din Excel
2. `df.columns.tolist()`: Verifică ce coloane există
3. Loop prin rânduri pentru procesare
4. Creare tuple-uri pentru insert

### 8.2 Mapare GL
```python
def procedura_mapare():
    try:
        # Ia toate GL-urile nemapate
        select_query = """
            SELECT DISTINCT GL 
            FROM general_ledger 
            WHERE Statutory_GL IS NULL
        """
        
        # Găsește mapping pentru fiecare
        update_query = """
            UPDATE general_ledger gl
            JOIN mapping m ON gl.GL = m.GL
            SET gl.Statutory_GL = m.Statutory_GL
            WHERE gl.Statutory_GL IS NULL
        """
```
**Ce face fiecare parte:**
1. Selectează GL-uri care nu au statutory
2. Join cu tabela de mapping
3. Actualizează statutory_gl

## 9. Configurații Importante

### 9.1 Bază de Date
```json
{
    "mysql": {
        "host": "efactura.mysql.database.azure.com",
        "user": "useradmin",
        "password": "xxx",
        "database": "soft_contabil_database"
    }
}
```

### 9.2 Redis Azure
```python
CELERY_BROKER_URL = "rediss://:password@RedisBoxGT.redis.cache.windows.net:6380/0"
CELERY_RESULT_BACKEND = "rediss://:password@RedisBoxGT.redis.cache.windows.net:6380/0"
```

## 10. Workflow Complet

### 10.1 Import Normal
1. User încarcă Excel
2. Sistem validează format
3. Pornește task Celery
4. Procesează date
5. Importă în bază
6. Mapează GL-uri
7. Trimite notificare

### 10.2 Generare Rapoarte
1. User selectează perioadă
2. Sistem extrage date
3. Calculează totaluri
4. Formatează Excel
5. Trimite pe email

## 11. Tips pentru Dezvoltare

### 11.1 Debugging
1. Verifică mereu log-urile Celery
2. Printează coloanele din Excel
3. Verifică tipurile de date
4. Monitorizează task-urile

### 11.2 Optimizare
1. Folosește insert batch
2. Indexează coloanele folosite în WHERE
3. Folosește EXPLAIN pentru query-uri
4. Monitorizează memoria Redis
