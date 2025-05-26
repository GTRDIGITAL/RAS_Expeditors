import datetime
import pandas as pd
import io
import os
import unicodedata
import math
import json
from sqlalchemy import create_engine
from datetime import timedelta

#de avut grija la sume (daca sunt cu , sau . si de schimbat codul la TB IN FUNCTIE DE ASTA)

class MyDict(dict):
    def __missing__(self, key):

        return str(key)
def normal_round(n, decimals=0):
    expoN = n * 10 ** decimals
    if abs(expoN) - abs(math.floor(expoN)) < 0.5:
        return math.floor(expoN) / 10 ** decimals
    return math.ceil(expoN) / 10 ** decimals

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

def generare_fisier_text(mesaj, informatii):
    # Generare conținut fișier text
    text_content = f"\n{mesaj}\n"
    for info in informatii:
        text_content += f"{info}, "
    text_content += "\n"  # Adăugare linie goală între secțiuni

    # Salvare fișier text
    # log_folder = "C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/logs"
    log_folder = "/home/efactura/efactura_expeditors/logs"
    log_path = os.path.join(log_folder, "informatii.txt")
    with open(log_path, "a", encoding="utf-8") as text_file:  # Utilizăm "a" pentru a adăuga la fișier
        text_file.write(text_content)
        
def generare_fisier_text_BUH(mesaj, informatii):
    # Generare conținut fișier text
    text_content = f"\n{mesaj}\n"
    for info in informatii:
        text_content += f"{info}, "
    text_content += "\n"  # Adăugare linie goală între secțiuni

    # Salvare fișier text
    # log_folder = "C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/logs"
    log_folder = "/home/efactura/efactura_expeditors/logs"
    log_path = os.path.join(log_folder, "informatii BUH.txt")
    with open(log_path, "a", encoding="utf-8") as text_file:  # Utilizăm "a" pentru a adăuga la fișier
        text_file.write(text_content)
            
coloane_obligatorii = ["GCI", "Journal", "Foreign Amount", 'Billing Description', 'File Ref', 'Amount', 'TC']
coloane_obligatorii_comentarii = ["File Number","Origin", "Destination", "Charge Weight", "Arrival", "MB", "HB", "PO", "Container", "Qty", "Weight", "Charge Weight", 'Shipper', 'Consignee']


facturi_nule = []

config = citeste_configurare('config.json')
mysql_config = config['mysql']
# Sales_EFACTURA=pd.read_excel("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/reg jurnal 0124.xlsx")
# fisierComentarii=pd.read_excel("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/BUH_data_Jan_import.xlsx")
def prelucrareDate(Sales_EFACTURA, fisierComentarii):
    def stergeFisiere(directory_path, file_extension):
        try:
            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)
                if filename.endswith(file_extension):
                    os.remove(file_path)
                    print(f"Fisierul {filename} a fost sters.")
        except Exception as e:
            print(f"Eroare la stergerea fișierelor: {str(e)}")
    
    # stergeFisiere('C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/outs', '.xml')
    stergeFisiere("/home/efactura/efactura_expeditors/outs", ".xml")
    # Calea către directorul de loguri
    # log_folder = "C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/logs"
    date=datetime.datetime.now().date()
    periodStart="12"
    periodStartYear="2023"
    LEGRAND_TAXFILENUM_Header="RO23045052" 
    LEGRAND_TAXFILENUM="0023045052" #PENTRU A COMPLETA CAMPURILE CUSTOMER SAU SUPPLIERID CU DATELE EXPUR ACOLO UNDE E CAZUL-CERINTA ESTE SA FIE FORMAT DIN 00+CUI, fara atribut RO
    contactPersonFirstName="JONI"
    contactPersonLastName="TIRRONIEMI"
    contactPhone="0722632420"
    headerComment="L" #L PT DECL LUNARA, VA TREBUI MODIFICATA DACA AVEM CEVA LA CERERE
    IBAN="RO34INGB0001008222848910"
    strada = "Str. Bucuresti-Ploiesti"
    oras = "SECTOR1"
    codPostal = "000000"
    countrySubentity = "RO-B"
    country = "RO"
    vatID ="RO23045052" 
    numeCompanie = "EXPEDITORS INTERNATIONAL ROMANIA SRL"
    
    Sales_EFACTURA=pd.read_excel(Sales_EFACTURA, engine='openpyxl')
    fisierComentarii=pd.read_excel(fisierComentarii, engine = 'openpyxl')
    
    # for coloana in coloane_obligatorii:
    #     try:
    #         # Verificăm dacă coloana există în dataframe
    #         if coloana not in Sales_EFACTURA.columns:
    #             print(f"Coloana {coloana} lipsă!")
                
    #             generare_fisier_text(f"-------------------Coloane lipsa------------------------\n\n'Coloana {coloana} lipseste din fisierul Excel:\n")
                
    #             # logging.error(f"Coloana {coloana} conține valori nule la liniile: {pozitii_nule.tolist()}")
    #     except KeyError:
    #         print("Coloana missing")
    
    coloane_lipsa = [col for col in coloane_obligatorii if col not in Sales_EFACTURA.columns]
    if coloane_lipsa:
        # Generare fișier text cu informații despre coloanele lipsă
        generare_fisier_text("-------------------COLOANE LIPSA------------------------\n\n'Următoarele coloane obligatorii lipsesc din fișierul Excel:\n", coloane_lipsa)
        
            
    # for coloana_comentarii in coloane_obligatorii_comentarii:
    #     try:
    #          if coloana_comentarii not in fisierComentarii.columns:
    #             print(f"Coloana {coloana_comentarii} lipsă!")
                
    #             generare_fisier_text_BUH(f"-------------------Coloane lipsa------------------------\n\n'Coloana {coloana_comentarii} lipseste din fisierul Excel:\n")
                
    #             # logging.error(f"Coloana {coloana} conține valori nule la liniile: {pozitii_nule.tolist()}")
    #     except KeyError:
    #         print("Coloana comentarii missing")
    
    coloane_lipsa_BUH = [col for col in coloane_obligatorii if col not in fisierComentarii.columns]
    if coloane_lipsa_BUH:
        # Generare fișier text cu informații despre coloanele lipsă
        generare_fisier_text_BUH("-------------------COLOANE LIPSA------------------------\n\n'Următoarele coloane obligatorii lipsesc din fișierul Excel:\n", coloane_lipsa_BUH)
        


    # Clients=pd.read_csv("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/customer vendor database 0124.csv")
    engine = create_engine(f"mysql://{config['mysql']['user']}:{config['mysql']['password']}@{config['mysql']['host']}/{config['mysql']['database']}")
    print("CONECTAT LA BAZA")
    query = "SELECT * FROM clients WHERE region IS NOT NULL"
    Clients = pd.read_sql(query, engine)

    Clients["CUST#"]=Clients["CUST#"].str.lstrip("0").str.replace(r'\.0$', '', regex=True)
    Clients.loc[Clients["regno"].astype(str).str.startswith("RO"), "COUNTRY_CODE"]="RO"
    Clients=Clients.loc[Clients["Country"].astype(str)=="RO"]
    dictClients_CUI=Clients.set_index('CUST#').to_dict()['regno']
    dictClients_City=Clients.set_index('CUST#').to_dict()['City']
    dictClients_Country=Clients.set_index('CUST#').to_dict()['Country']
    dictClients_Street=Clients.set_index('CUST#').to_dict()['Street']
    dictClients_Region=Clients.set_index('CUST#').to_dict()['region']


    # Clients.to_excel("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/Clients.xlsx")
    
    
    
    # Sales_EFACTURA=pd.read_excel("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/reg jurnal 0124.xlsx")
    Sales_EFACTURA=Sales_EFACTURA.loc[(Sales_EFACTURA["Ext GL"].astype(str).str.startswith("704")) | (Sales_EFACTURA["Ext GL"].astype(str).str.startswith("411"))]
    Sales_EFACTURA=Sales_EFACTURA.loc[Sales_EFACTURA["Ext GL"].astype(str)!="704-70E"]
    Sales_EFACTURA.loc[(Sales_EFACTURA["Foreign Amount"].astype(str)=="nan") & (Sales_EFACTURA["Ext GL"].astype(str).str.startswith('411')),'Foreign Amount']=Sales_EFACTURA["Amount"]
    Sales_EFACTURA.loc[Sales_EFACTURA["Ext GL"].astype(str).str.startswith("411"), "FX Inv"]=Sales_EFACTURA["Amount"]/(Sales_EFACTURA["Foreign Amount"].fillna(Sales_EFACTURA["Amount"]))
    Sales_EFACTURA.loc[Sales_EFACTURA["Ext GL"].astype(str).str.startswith("411"), "Foreign Currency"]=Sales_EFACTURA["Foreign Currency"].fillna("RON")
    Sales_EFACTURA['FX Inv'] = Sales_EFACTURA.groupby('Journal')['FX Inv'].transform(lambda x: x.fillna(method='ffill'))
    Sales_EFACTURA['FX Inv'] = Sales_EFACTURA.groupby('Journal')['FX Inv'].transform(lambda x: x.fillna(method='bfill'))
    currency_mapping = Sales_EFACTURA.loc[Sales_EFACTURA['Ext GL'].astype(str).str.startswith('411')].set_index('Journal')['Foreign Currency'].to_dict()

    # Se actualizează moneda pentru liniile cu contul 704 folosind dicționarul creat
    Sales_EFACTURA['Foreign Currency'] = Sales_EFACTURA['Journal'].map(currency_mapping).fillna(Sales_EFACTURA['Foreign Currency'])
    
    Sales_EFACTURA=Sales_EFACTURA.loc[Sales_EFACTURA["TC"].astype(str)!="nan"]
    Sales_EFACTURA["COUNTRY_CLIENT"]=Sales_EFACTURA["GCI"].str.lstrip("0").str.replace(r'\.0$', '', regex=True).map(dictClients_Country)
    Sales_EFACTURA=Sales_EFACTURA.loc[Sales_EFACTURA["COUNTRY_CLIENT"]=="RO"]
    # Sales_EFACTURA=Sales_EFACTURA.loc[~Sales_EFACTURA["GL Cat"].astype(str).str.contains("Currency Adjustment Factor")]
    # Sales_EFACTURA.to_excel("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/RJ GRUPAT.xlsx")
    Sales_EFACTURA["CUI_CLIENT"]=Sales_EFACTURA["GCI"].str.lstrip("0").str.replace(r'\.0$', '', regex=True).map(dictClients_CUI)
    Sales_EFACTURA["CITY_CLIENT"]=Sales_EFACTURA["GCI"].str.lstrip("0").str.replace(r'\.0$', '', regex=True).map(dictClients_City)
    Sales_EFACTURA["STREET_CLIENT"]=Sales_EFACTURA["GCI"].str.lstrip("0").str.replace(r'\.0$', '', regex=True).map(dictClients_Street)
    dictCota={"E":0, "S":19.00 }
    Sales_EFACTURA["Flag CN/INV"]=Sales_EFACTURA["Journal"].astype(str).str[0]
    Sales_EFACTURA.loc[Sales_EFACTURA["Flag CN/INV"]=="E","Inv Type code"]=380
    Sales_EFACTURA.loc[Sales_EFACTURA["Flag CN/INV"]=="C","Inv Type code"]=381
    # Sales_EFACTURA.loc[Sales_EFACTURA["Flag CN/INV"]=="C", "Quantity"]=-1
    Sales_EFACTURA["Quantity"]=1
    Sales_EFACTURA.loc[Sales_EFACTURA["Flag CN/INV"]=="E", "Amount"]=Sales_EFACTURA["Amount"]*(-1)
    Sales_EFACTURA.loc[Sales_EFACTURA["Flag CN/INV"]=="E", "Foreign Amount"]=Sales_EFACTURA["Foreign Amount"]*(-1)
    Sales_EFACTURA.loc[(Sales_EFACTURA["Flag CN/INV"]=="E") & (Sales_EFACTURA["Amount"]<0) , "Quantity"]=-1
    Sales_EFACTURA.loc[Sales_EFACTURA["Flag CN/INV"]=="C", "Amount"]=Sales_EFACTURA["Amount"]
    Sales_EFACTURA.loc[Sales_EFACTURA["Flag CN/INV"]=="C", "Foreign Amount"]=Sales_EFACTURA["Foreign Amount"]
    Sales_EFACTURA['Amount'] = Sales_EFACTURA.groupby(['Billing Description', 'Journal'])['Amount'].transform('sum').round(2)
    Sales_EFACTURA['Foreign Amount'] = Sales_EFACTURA.groupby(['Billing Description', 'Journal'])['Foreign Amount'].transform('sum').round(2)
    
    Sales_EFACTURA.loc[(Sales_EFACTURA["Flag CN/INV"]=="E") & (Sales_EFACTURA["Amount"]<0) , "Quantity"]=-1
    Sales_EFACTURA.loc[(Sales_EFACTURA["Flag CN/INV"]=="E") & (Sales_EFACTURA["Amount"]>0) , "Quantity"]=1

    # După ce am calculat suma prețurilor, eliminăm duplicatele
    Sales_EFACTURA = Sales_EFACTURA.drop_duplicates(subset=['Journal', 'Billing Description'])
    Sales_EFACTURA.loc[Sales_EFACTURA["Foreign Amount"]!="nan", 'Foreign Amount']=Sales_EFACTURA["Amount"]/Sales_EFACTURA["FX Inv"]
    Sales_EFACTURA['Foreign Amount'] = Sales_EFACTURA['Foreign Amount'].round(2)
    Sales_EFACTURA["Cota"]=Sales_EFACTURA["TC"].map(dictCota)
    Sales_EFACTURA["Valoare linia TVA"]=Sales_EFACTURA["Amount"]*(Sales_EFACTURA["Cota"]/100)
    Sales_EFACTURA["Total Factura"]=Sales_EFACTURA.groupby('Journal')['Amount'].transform('sum').round(2)
    Sales_EFACTURA["Valoare linie cu TVA"]=Sales_EFACTURA["Amount"]+Sales_EFACTURA["Valoare linia TVA"]

    Sales_EFACTURA["Cod Unitate Masura"]="H87"
    Sales_EFACTURA.loc[Sales_EFACTURA["TC"]=="E", "ID TVA"]="E"
    Sales_EFACTURA.loc[Sales_EFACTURA["TC"]=="S", "ID TVA"]="S"
    Sales_EFACTURA.loc[Sales_EFACTURA["TC"]=="EU", "ID TVA"]="AE"
    Sales_EFACTURA["REGION"]=Sales_EFACTURA["GCI"].str.lstrip("0").str.replace(r'\.0$', '', regex=True).map(dictClients_Region)
    # Sales_EFACTURA.loc[Sales_EFACTURA["Foreign Currency"]!="nan", "FX"]=Sales_EFACTURA["Amount"]/Sales_EFACTURA["Foreign Amount"]
    # Sales_EFACTURA['FX'] = Sales_EFACTURA.groupby('Journal')['FX'].transform(lambda x: x.fillna(method='ffill'))
    # Sales_EFACTURA['Foreign Currency'] = Sales_EFACTURA.groupby('Journal')['Foreign Currency'].transform(lambda x: x.fillna(method='ffill'))
    # Sales_EFACTURA.loc[Sales_EFACTURA["Foreign Amount"].astype(str)=="nan", "Foreign Amount" ]=Sales_EFACTURA["Amount"]/Sales_EFACTURA['FX']
    Sales_EFACTURA["Valoare linia TVA (Valuta)"]=Sales_EFACTURA["Foreign Amount"]*(Sales_EFACTURA["Cota"]/100)
    Sales_EFACTURA["Total Factura (Valuta)"]=Sales_EFACTURA.groupby('Journal')['Foreign Amount'].transform('sum').round(2)
    Sales_EFACTURA["Valoare linie cu TVA (Valuta)"]=Sales_EFACTURA["Foreign Amount"]+Sales_EFACTURA["Valoare linia TVA (Valuta)"]
    listaNumarFact = list(set(list(Sales_EFACTURA["Journal"])))

    totalFactura=Sales_EFACTURA["Amount"].sum()
    primaFactura = list(Sales_EFACTURA["Journal"])[0]
    ultimaFactura=list(Sales_EFACTURA["Journal"])[-1]
    print(totalFactura, primaFactura, ultimaFactura)
    print("asta e prima factura in prelucrare_date.py ",primaFactura)
    # Sales_EFACTURA.to_excel("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/Sales.xlsx")

    coloaneComentarii = ["File Number","Origin", "Destination", "Charge Weight", "Arrival", "MB", "HB", "PO", "Container", "Qty", "Weight", "Charge Weight", 'Shipper', 'Consignee']
    Sales_EFACTURA["File Ref"]=Sales_EFACTURA["File Ref"].astype(str).replace(r'\.0$', '', regex=True)
    print(fisierComentarii)
    fisierComentarii["File Number"]=fisierComentarii["File Number"].astype(str).str.replace(r'\.0$', '', regex=True)
    fisierComentarii["File Number"]=fisierComentarii["File Number"].astype(str).str.strip()
    fisierComentarii_first_line = fisierComentarii.drop_duplicates(subset=['File Number'])
    Sales_EFACTURA = pd.merge(Sales_EFACTURA, fisierComentarii_first_line[coloaneComentarii], left_on='File Ref', right_on='File Number', how='left')
    # Sales_EFACTURA = pd.merge(Sales_EFACTURA, fisierComentarii[coloaneComentarii], left_on='File Ref', right_on='File Number', how='left')
    # Sales_EFACTURA.to_excel("C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/Baza de date vanzari/Sales.xlsx")
    
    
    nrFacturiTrimise = len(listaNumarFact)
    facturiNuleUnice = 0

    for i in range(0, len(listaNumarFact)):
        if listaNumarFact[i][:1]=="E":
    #-------------------------------------------------------------------INVOICE IN LEI--------------------------------------------------------------------------------------------------
            df_fact_curenta = Sales_EFACTURA.groupby(["Journal"]).get_group(listaNumarFact[i])
            issue_date = pd.to_datetime(df_fact_curenta["Date"]).dt.strftime('%Y-%m-%d').iloc[0]
            issue_date_datetime = pd.to_datetime(issue_date)

            # Calculează duedate adăugând 30 de zile la issue_date
            duedate = issue_date_datetime + pd.Timedelta(days=30)

            # Convert duedate la formatul 'YYYY-MM-DD'
            duedate_str = duedate.strftime('%Y-%m-%d')

            print("Due Date:", duedate_str)
            if str(df_fact_curenta["Foreign Currency"].iloc[0])=="RON":    
                listaCote = list(set(list(df_fact_curenta["Cota"])))
                subtotalTva = df_fact_curenta.groupby("Cota")["Valoare linia TVA"].sum().reset_index()
                subtotalBaza=df_fact_curenta.groupby("Cota")["Amount"].sum().reset_index()
                subtotalIDTVA=df_fact_curenta.groupby("ID TVA")["Cota"].sum().reset_index()
                totalcaractere=4+int(len(str(df_fact_curenta["HB"].iloc[0])))
                totalcaracter=totalcaractere+21+int(len(str(df_fact_curenta["Arrival"].iloc[0])))
                totalcaracter=totalcaractere+16+int(len(str(df_fact_curenta["Origin"].iloc[0])))
                totalcaracter=totalcaractere+18+int(len(str(df_fact_curenta["Destination"].iloc[0])))
                totalcaracter=totalcaractere+11+int(len(str(df_fact_curenta["MB"].iloc[0])))
                totalcaracter=totalcaractere+16+int(len(str(df_fact_curenta["Charge Weight"].values[0][0])))
                totalcaracter=totalcaractere+14+int(len(str(df_fact_curenta["PO"].iloc[0]).replace("nan","")))
                totalcaracter=totalcaractere+10+9+int(len(str(df_fact_curenta["Qty"].iloc[0])))
                
                total_amount = 0
                tva_total=0

                XML_Header = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n
                <Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2" xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"\n xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2" xmlns:ns4="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"\n xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2 http://docs.oasis-open.org/ubl/os-UBL-2.1/xsd/maindoc/UBL-Invoice-2.1.xsd">
                <cbc:CustomizationID>urn:cen.eu:en16931:2017#compliant#urn:efactura.mfinante.ro:CIUS-RO:1.0.1</cbc:CustomizationID>
                <cbc:ID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:ID>
                <cbc:IssueDate>{issue_date}</cbc:IssueDate>
                <cbc:DueDate>{duedate_str}</cbc:DueDate>
                <cbc:InvoiceTypeCode>{str(df_fact_curenta["Inv Type code"].iloc[0]).replace(".0", "")}</cbc:InvoiceTypeCode>
                <cbc:Note>VVL:{str(df_fact_curenta["HB"].iloc[0])}</cbc:Note>
                <cbc:Note>Sailing/arrival date:{str(df_fact_curenta["Arrival"].iloc[0])}</cbc:Note>
                <cbc:Note>Port of loading:{str(df_fact_curenta["Origin"].iloc[0])}</cbc:Note>
                <cbc:Note>Port of discharge:{str(df_fact_curenta["Destination"].iloc[0])}</cbc:Note>
                <cbc:Note>B/L Number:{str(df_fact_curenta["MB"].iloc[0])}</cbc:Note>
                <cbc:Note>QUANTITY:{str(df_fact_curenta["Qty"].iloc[0])}</cbc:Note>
                <cbc:Note>CONTAINER:{str(df_fact_curenta["Container"].iloc[0]).replace("nan", "")[:300-totalcaractere]}</cbc:Note>
                <cbc:Note>CHARGEABLE WGT.:{str(df_fact_curenta["Charge Weight"].values[0][0])}</cbc:Note>
                <cbc:Note>Other Details:{str(df_fact_curenta["PO"].iloc[0]).replace("nan","")}</cbc:Note>'''
                if "CAMERON ROMANIA" in str(df_fact_curenta["Company"].iloc[0]):
                    XML_Header=XML_Header+ f'''<cbc:Note>Shipper.:{str(df_fact_curenta["Shipper"].iloc[0])}</cbc:Note>
                <cbc:Note>Consignee:{str(df_fact_curenta["Consignee"].iloc[0]).replace("nan","")}</cbc:Note>'''
                XML_Header = XML_Header + f'''
                <cbc:DocumentCurrencyCode>RON</cbc:DocumentCurrencyCode>
                '''

                AccountingSupplierParty = '''
                <cac:AccountingSupplierParty>
                    <cac:Party>
                        <cac:PostalAddress>
                            <cbc:StreetName>'''+str(strada)+'''</cbc:StreetName>
                            <cbc:CityName>'''+str(oras)+'''</cbc:CityName>
                            <cbc:CountrySubentity>'''+str(countrySubentity)+'''</cbc:CountrySubentity>
                            <cac:Country>
                                <cbc:IdentificationCode>'''+str(country)+'''</cbc:IdentificationCode>
                            </cac:Country>
                        </cac:PostalAddress>
                        <cac:PartyTaxScheme>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:PartyTaxScheme>
                        <cac:PartyLegalEntity>
                            <cbc:RegistrationName>'''+str(numeCompanie)+'''</cbc:RegistrationName>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                        </cac:PartyLegalEntity>
                    </cac:Party>
                </cac:AccountingSupplierParty>
                '''
                
                if str(df_fact_curenta["STREET_CLIENT"].iloc[0]) == "  ":
                    AccountingCustomerPartyXML=f'''
                    <cac:AccountingCustomerParty>
                        <cac:Party>
                            <cac:PostalAddress>
                                <cbc:StreetName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:StreetName>
                                <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                                <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                                <cac:Country>
                                    <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                                </cac:Country>
                            </cac:PostalAddress>
                            <cac:PartyTaxScheme>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                                <cac:TaxScheme>
                                    <cbc:ID>VAT</cbc:ID>
                                </cac:TaxScheme>
                            </cac:PartyTaxScheme>
                            <cac:PartyLegalEntity>
                                <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            </cac:PartyLegalEntity>
                        </cac:Party>
                    </cac:AccountingCustomerParty>'''
                else:
                    AccountingCustomerPartyXML=f'''
                <cac:AccountingCustomerParty>
                    <cac:Party>
                        <cac:PostalAddress>
                            <cbc:StreetName>{str(df_fact_curenta["STREET_CLIENT"].iloc[0])}</cbc:StreetName>
                            <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                            <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                            <cac:Country>
                                <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                            </cac:Country>
                        </cac:PostalAddress>
                        <cac:PartyTaxScheme>
                            <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:PartyTaxScheme>
                        <cac:PartyLegalEntity>
                            <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                            <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                        </cac:PartyLegalEntity>
                    </cac:Party>
                </cac:AccountingCustomerParty>'''
                # invoiceLine += xml_efactura + AccountingCustomerPartyXML 
                # Variabilă pentru a număra elementele din fiecare factură
                invoiceLine = ""
                line_count = 1
                total_tva=0
                # print(subtotalTva)
                # <cbc:ID>{row["ID TVA"]}</cbc:ID>
                TAXTOTAL="\n<cac:TaxTotal>\n"
                TaxTotal =""
                for index, row in subtotalTva.iterrows():
                    taxamount=subtotalTva["Valoare linia TVA"][index].sum()
                    taxamounttotal=subtotalTva["Valoare linia TVA"].sum()
                    taxamounttotal=normal_round(taxamounttotal, decimals=2)
                    baza = subtotalBaza["Amount"][index].sum()
                    baza=normal_round(baza, decimals=2)
                    taxamount=normal_round(taxamount, decimals=2)

                    if str(subtotalIDTVA["ID TVA"][index])=="AE":

                        TaxExemptionReasonCode="VATEX-EU-AE"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="RON">{str(round(float(str(baza)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="RON">{str(round(float(str(row["Valoare linia TVA"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReasonCode>{TaxExemptionReasonCode}</cbc:TaxExemptionReasonCode>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    elif str(subtotalIDTVA["ID TVA"][index])=="E":

                        TaxExemptionReasonCode="Art.294,alin.1,lit.g din Codul fiscal-sunt scutite de TVA transport și serviciile auxiliare de import"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="RON">{str(round(float(str(baza)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="RON">{str(round(float(str(row["Valoare linia TVA"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReason>{TaxExemptionReasonCode}</cbc:TaxExemptionReason>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    else:
                        TaxTotal = TaxTotal + f'''

                            <cac:TaxSubtotal>
                                    <cbc:TaxableAmount currencyID="RON">{str(round(float(str(baza)),2))}</cbc:TaxableAmount>
                                    <cbc:TaxAmount currencyID="RON">{str(round(float(str(row["Valoare linia TVA"])),2))}</cbc:TaxAmount>
                                    <cac:TaxCategory>
                                        <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                        <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                        <cac:TaxScheme>
                                            <cbc:ID>VAT</cbc:ID>
                                        </cac:TaxScheme>
                                    </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                        # print("abc")
                TAXTOTAL = TAXTOTAL + '<cbc:TaxAmount currencyID="RON">' + str(round(float(str(taxamounttotal)),2)) +'</cbc:TaxAmount>' + TaxTotal + "\n</cac:TaxTotal>\n"
                for index, row in df_fact_curenta.iterrows():
                    line_amount = row["Amount"]
                    # line_amount=normal_round(line_amount, decimals=2)
                    val_cu_tva = row["Valoare linie cu TVA"]
                    tva = row["Valoare linia TVA"]
                    # tva = normal_round(tva, decimals=2)
                    
                    total_tva += val_cu_tva
                    tva_total += tva
                    total_amount += line_amount
                    # total_amount=normal_round(total_amount, decimals=2)
                    invoiceLine += f'''<cac:InvoiceLine>
                            <cbc:ID>{line_count}</cbc:ID>
                            <cbc:InvoicedQuantity unitCode="{row["Cod Unitate Masura"]}">{row["Quantity"]}</cbc:InvoicedQuantity>
                            <cbc:LineExtensionAmount currencyID="RON">{str(round(float(str(row["Amount"])),2))}</cbc:LineExtensionAmount>
                            <cac:Item>
                                <cbc:Name>{row["Billing Description"]}</cbc:Name>
                                <cac:ClassifiedTaxCategory>
                                    <cbc:ID>{row["ID TVA"]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:ClassifiedTaxCategory>
                            </cac:Item>
                            <cac:Price>
                                <cbc:PriceAmount currencyID="RON">{str(abs(round(float(str(row["Amount"])),2)))}</cbc:PriceAmount>
                            </cac:Price>
                        </cac:InvoiceLine>'''
                    
                    
                    
                    
                    # Incrementați numărul elementului pentru următoarea linie din factură
                    line_count += 1
                total_amount_with_vat = total_amount + tva_total
                # total_amount_with_vat=normal_round(total_amount_with_vat, decimals=2)
                # print(row["Journal"], total_tva)
                # print(str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "") ,total_amount_without_vat)

                PaymentMeans = f'''
                    <cac:PaymentMeans>
                    <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                    <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                    <cac:PayeeFinancialAccount>
                        <cbc:ID>RO34INGB0001008222848910</cbc:ID>
                        <cbc:Name>CONT ING BANK SA IN RON</cbc:Name>
                        <cac:FinancialInstitutionBranch>
                            <cbc:ID>INGBROBU</cbc:ID>
                        </cac:FinancialInstitutionBranch>
                    </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO80INGB0001008222840710</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN EUR</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO26INGB0001008222844010</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN USD</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>'''    


                LegalMonetary = f'''
                <cac:LegalMonetaryTotal>
                    <cbc:LineExtensionAmount currencyID="RON">{str(round(float(str(total_amount)),2))}</cbc:LineExtensionAmount>
                    <cbc:TaxExclusiveAmount currencyID="RON">{str(round(float(str(total_amount)),2))}</cbc:TaxExclusiveAmount>
                    <cbc:TaxInclusiveAmount currencyID="RON">{str(round(float(str(total_amount_with_vat)),2))}</cbc:TaxInclusiveAmount>
                    <cbc:AllowanceTotalAmount currencyID="RON">0.00</cbc:AllowanceTotalAmount>
                    <cbc:ChargeTotalAmount currencyID="RON">0.00</cbc:ChargeTotalAmount>
                    <cbc:PrepaidAmount currencyID="RON">0.00</cbc:PrepaidAmount>
                    <cbc:PayableRoundingAmount currencyID="RON">0.00</cbc:PayableRoundingAmount>
                    <cbc:PayableAmount currencyID="RON">{str(round(float(str(total_amount_with_vat)),2))}</cbc:PayableAmount>
                </cac:LegalMonetaryTotal>'''


                # print(total_amount)
                # eFacturaXML = meta + XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + " TAX TOTAL " + " LEGAL MONETARY TOOL " + invoiceLine +"</Invoice>"
                # Scrieți fișierul XML pentru fiecare factură în parte
                eFacturaXML = XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + PaymentMeans + TAXTOTAL + LegalMonetary + invoiceLine +"\n</Invoice>"
                def remove_diacritics(input_str):
                    nfkd_form = unicodedata.normalize('NFKD', input_str)
                    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

                eFacturaXML = remove_diacritics(eFacturaXML)
                eFacturaXML=eFacturaXML.replace("&"," ")

                # Scrie conținutul în fișierul XML
                # with io.open(f"C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/outs/SalesInvoice_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                #     f.write(eFacturaXML)
                with io.open(f"/home/efactura/efactura_expeditors/outs/SalesInvoice_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                    f.write(eFacturaXML)

                print("A PRELUCRAT DATELE")
        #-------------------------------------------------------------------INVOICE IN VALUTA--------------------------------------------------------------------------------------------------
            else:
                df_fact_curenta = Sales_EFACTURA.groupby(["Journal"]).get_group(listaNumarFact[i])
                issue_date = pd.to_datetime(df_fact_curenta["Date"]).dt.strftime('%Y-%m-%d').iloc[0]
                issue_date_datetime = pd.to_datetime(issue_date)

                # Calculează duedate adăugând 30 de zile la issue_date
                duedate = issue_date_datetime + pd.Timedelta(days=30)

                # Convert duedate la formatul 'YYYY-MM-DD'
                duedate_str = duedate.strftime('%Y-%m-%d')

                print("Due Date:", duedate_str)
                currency=str(df_fact_curenta["Foreign Currency"].iloc[0])
                
                listaCote = list(set(list(df_fact_curenta["Cota"])))
                subtotalTvaLEI=df_fact_curenta.groupby("Cota")["Valoare linia TVA"].sum().reset_index()
                subtotalTva = df_fact_curenta.groupby("Cota")["Valoare linia TVA (Valuta)"].sum().reset_index()
                subtotalBaza=df_fact_curenta.groupby("Cota")["Amount"].sum().reset_index()
                subtotalBazaValuta=df_fact_curenta.groupby("Cota")["Foreign Amount"].sum().reset_index()
                subtotalTvaValuta=df_fact_curenta.groupby("Cota")["Valoare linia TVA (Valuta)"].sum().reset_index()
                subtotalIDTVA=df_fact_curenta.groupby("ID TVA")["Cota"].sum().reset_index()
                
                total_amount = 0
                tva_total=0
                #{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}
                journal_value = str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")
                if journal_value.isdigit():
                    # Convertește în întreg doar dacă este un număr
                    journal_value = int(journal_value)

                totalcaractere=4+int(len(str(df_fact_curenta["HB"].iloc[0])))
                totalcaracter=totalcaractere+21+int(len(str(df_fact_curenta["Arrival"].iloc[0])))
                totalcaracter=totalcaractere+16+int(len(str(df_fact_curenta["Origin"].iloc[0])))
                totalcaracter=totalcaractere+18+int(len(str(df_fact_curenta["Destination"].iloc[0])))
                totalcaracter=totalcaractere+11+int(len(str(df_fact_curenta["MB"].iloc[0])))
                totalcaracter=totalcaractere+16+int(len(str(df_fact_curenta["Charge Weight"].values[0][0])))
                totalcaracter=totalcaractere+14+int(len(str(df_fact_curenta["PO"].iloc[0]).replace("nan","")))
                totalcaracter=totalcaractere+10+9+int(len(str(df_fact_curenta["Qty"].iloc[0])))

                XML_Header = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n
                <Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2" xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"\n xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2" xmlns:ns4="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"\n xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2 http://docs.oasis-open.org/ubl/os-UBL-2.1/xsd/maindoc/UBL-Invoice-2.1.xsd">
                <cbc:CustomizationID>urn:cen.eu:en16931:2017#compliant#urn:efactura.mfinante.ro:CIUS-RO:1.0.1</cbc:CustomizationID>
                <cbc:ID>{journal_value}</cbc:ID>
                <cbc:IssueDate>{issue_date}</cbc:IssueDate>
                <cbc:DueDate>{duedate_str}</cbc:DueDate>
                <cbc:InvoiceTypeCode>{str(df_fact_curenta["Inv Type code"].iloc[0]).replace(".0", "")}</cbc:InvoiceTypeCode>
                <cbc:Note>VVL:{str(df_fact_curenta["HB"].iloc[0])}</cbc:Note>
                <cbc:Note>Sailing/arrival date:{str(df_fact_curenta["Arrival"].iloc[0])}</cbc:Note>
                <cbc:Note>Port of loading:{str(df_fact_curenta["Origin"].iloc[0])}</cbc:Note>
                <cbc:Note>Port of discharge:{str(df_fact_curenta["Destination"].iloc[0])}</cbc:Note>
                <cbc:Note>B/L Number:{str(df_fact_curenta["MB"].iloc[0])}</cbc:Note>
                <cbc:Note>QUANTITY:{str(df_fact_curenta["Qty"].iloc[0])}</cbc:Note>
                <cbc:Note>CONTAINER:{str(df_fact_curenta["Container"].iloc[0]).replace("nan","")[:300-totalcaractere]}</cbc:Note>
                <cbc:Note>CHARGEABLE WGT.:{str(df_fact_curenta["Charge Weight"].values[0][0])}</cbc:Note>
                <cbc:Note>Other Details:{str(df_fact_curenta["PO"].iloc[0]).replace("nan","")}</cbc:Note>'''
                if "CAMERON ROMANIA" in str(df_fact_curenta["Company"].iloc[0]):
                    XML_Header=XML_Header+ f'''<cbc:Note>Shipper.:{str(df_fact_curenta["Shipper"].iloc[0])}</cbc:Note>
                <cbc:Note>Consignee:{str(df_fact_curenta["Consignee"].iloc[0]).replace("nan","")}</cbc:Note>'''
                XML_Header = XML_Header + f'''
                <cbc:DocumentCurrencyCode>{str(df_fact_curenta['Foreign Currency'].iloc[0])}</cbc:DocumentCurrencyCode>
                <cbc:TaxCurrencyCode>RON</cbc:TaxCurrencyCode>
                '''

                AccountingSupplierParty = '''
                <cac:AccountingSupplierParty>
                    <cac:Party>
                        <cac:PostalAddress>
                            <cbc:StreetName>'''+str(strada)+'''</cbc:StreetName>
                            <cbc:CityName>'''+str(oras)+'''</cbc:CityName>
                            <cbc:CountrySubentity>'''+str(countrySubentity)+'''</cbc:CountrySubentity>
                            <cac:Country>
                                <cbc:IdentificationCode>'''+str(country)+'''</cbc:IdentificationCode>
                            </cac:Country>
                        </cac:PostalAddress>
                        <cac:PartyTaxScheme>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:PartyTaxScheme>
                        <cac:PartyLegalEntity>
                            <cbc:RegistrationName>'''+str(numeCompanie)+'''</cbc:RegistrationName>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                        </cac:PartyLegalEntity>
                    </cac:Party>
                </cac:AccountingSupplierParty>
                '''
                
                if str(df_fact_curenta["STREET_CLIENT"].iloc[0]) == "  ":
                    AccountingCustomerPartyXML=f'''
                    <cac:AccountingCustomerParty>
                        <cac:Party>
                            <cac:PostalAddress>
                                <cbc:StreetName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:StreetName>
                                <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                                <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                                <cac:Country>
                                    <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                                </cac:Country>
                            </cac:PostalAddress>
                            <cac:PartyTaxScheme>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                                <cac:TaxScheme>
                                    <cbc:ID>VAT</cbc:ID>
                                </cac:TaxScheme>
                            </cac:PartyTaxScheme>
                            <cac:PartyLegalEntity>
                                <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            </cac:PartyLegalEntity>
                        </cac:Party>
                    </cac:AccountingCustomerParty>'''
                else:
                    AccountingCustomerPartyXML=f'''
                <cac:AccountingCustomerParty>
                    <cac:Party>
                        <cac:PostalAddress>
                            <cbc:StreetName>{str(df_fact_curenta["STREET_CLIENT"].iloc[0])}</cbc:StreetName>
                            <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                            <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                            <cac:Country>
                                <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                            </cac:Country>
                        </cac:PostalAddress>
                        <cac:PartyTaxScheme>
                            <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:PartyTaxScheme>
                        <cac:PartyLegalEntity>
                            <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                            <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                        </cac:PartyLegalEntity>
                    </cac:Party>
                </cac:AccountingCustomerParty>'''
                # invoiceLine += xml_efactura + AccountingCustomerPartyXML 
                # Variabilă pentru a număra elementele din fiecare factură
                invoiceLine = ""
                line_count = 1
                total_tva=0
                # print(subtotalTva)
                # <cbc:ID>{row["ID TVA"]}</cbc:ID>
                TAXTOTAL="\n<cac:TaxTotal>\n"
                TaxTotal =""
                for index, row in subtotalTva.iterrows():
                    taxamount=subtotalTvaValuta["Valoare linia TVA (Valuta)"][index].sum()
                    taxamounttotal=subtotalTvaValuta["Valoare linia TVA (Valuta)"].sum()
                    taxamounttotalLEI=subtotalTvaLEI["Valoare linia TVA"].sum()
                    taxamounttotal=normal_round(taxamounttotal, decimals=2)
                    taxamounttotalLEI=normal_round(taxamounttotalLEI, decimals=2)
                    bazaV = subtotalBazaValuta["Foreign Amount"][index].sum()
                    baza= subtotalBaza["Amount"][index].sum()
                    baza=normal_round(baza, decimals=2)
                    bazaV=normal_round(bazaV, decimals=2)
                    taxamount=normal_round(taxamount, decimals=2)

                    if str(subtotalIDTVA["ID TVA"][index])=="AE":

                        TaxExemptionReasonCode="VATEX-EU-AE"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="{str(currency)}">{str(round(float(str(bazaV)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="{str(currency)}">{str(round(float(str(row["Valoare linia TVA (Valuta)"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReasonCode>{TaxExemptionReasonCode}</cbc:TaxExemptionReasonCode>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    elif str(subtotalIDTVA["ID TVA"][index])=="E":

                        TaxExemptionReasonCode="Art.294,alin.1,lit.g din Codul fiscal-sunt scutite de TVA transport și serviciile auxiliare de import"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="{str(currency)}">{str(round(float(str(bazaV)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="{str(currency)}">{str(round(float(str(row["Valoare linia TVA (Valuta)"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReason>{TaxExemptionReasonCode}</cbc:TaxExemptionReason>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    else:
                        TaxTotal = TaxTotal + f'''

                            <cac:TaxSubtotal>
                                    <cbc:TaxableAmount currencyID="{str(currency)}">{str(round(float(str(bazaV)),2))}</cbc:TaxableAmount>
                                    <cbc:TaxAmount currencyID="{str(currency)}">{str(round(float(str(row["Valoare linia TVA (Valuta)"])),2))}</cbc:TaxAmount>
                                    <cac:TaxCategory>
                                        <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                        <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                        <cac:TaxScheme>
                                            <cbc:ID>VAT</cbc:ID>
                                        </cac:TaxScheme>
                                    </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                        # print("abc")
                TAXTOTAL = TAXTOTAL + '<cbc:TaxAmount currencyID="RON">' + str(round(float(str(taxamounttotalLEI)),2)) +'</cbc:TaxAmount>' + "\n</cac:TaxTotal>\n"+ TAXTOTAL + '<cbc:TaxAmount currencyID="'+str(currency)+'">' + str(round(float(str(taxamounttotal)),2)) +'</cbc:TaxAmount>' + TaxTotal + "\n</cac:TaxTotal>\n"
                for index, row in df_fact_curenta.iterrows():
                    line_amount = row["Foreign Amount"]
                    currency=row["Foreign Currency"]
                    # line_amount=normal_round(line_amount, decimals=2)
                    val_cu_tva = row["Valoare linie cu TVA (Valuta)"]
                    tva = row["Valoare linia TVA (Valuta)"]
                    # tva = normal_round(tva, decimals=2)
                    
                    total_tva += val_cu_tva
                    tva_total += tva
                    
                    total_amount += line_amount
                    # total_amount=normal_round(total_amount, decimals=2)
                    invoiceLine += f'''<cac:InvoiceLine>
                            <cbc:ID>{line_count}</cbc:ID>
                            <cbc:InvoicedQuantity unitCode="{row["Cod Unitate Masura"]}">{row["Quantity"]}</cbc:InvoicedQuantity>
                            <cbc:LineExtensionAmount currencyID="{str(row["Foreign Currency"])}">{str(round(float(str(row["Foreign Amount"])),2))}</cbc:LineExtensionAmount>
                            <cac:Item>
                                <cbc:Name>{row["Billing Description"]}</cbc:Name>
                                <cac:ClassifiedTaxCategory>
                                    <cbc:ID>{row["ID TVA"]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:ClassifiedTaxCategory>
                            </cac:Item>
                            <cac:Price>
                                <cbc:PriceAmount currencyID="{str(row["Foreign Currency"])}">{str(abs(round(float(str(row["Foreign Amount"])),2)))}</cbc:PriceAmount>
                            </cac:Price>
                        </cac:InvoiceLine>'''
                        
                    
                    
                    # Incrementați numărul elementului pentru următoarea linie din factură
                    line_count += 1
                tva_total = normal_round(tva_total, decimals = 2)
                total_amount_with_vat = total_amount + tva_total
                # total_amount_with_vat=normal_round(total_amount_with_vat, decimals=2)
                # print(row["Journal"], total_tva)
                # print(str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "") ,total_amount_without_vat)

                PaymentMeans = f'''
                    <cac:PaymentMeans>
                    <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                    <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                    <cac:PayeeFinancialAccount>
                        <cbc:ID>RO34INGB0001008222848910</cbc:ID>
                        <cbc:Name>CONT ING BANK SA IN RON</cbc:Name>
                        <cac:FinancialInstitutionBranch>
                            <cbc:ID>INGBROBU</cbc:ID>
                        </cac:FinancialInstitutionBranch>
                    </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO80INGB0001008222840710</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN EUR</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO26INGB0001008222844010</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN USD</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>''' 


                LegalMonetary = f'''
                <cac:LegalMonetaryTotal>
                    <cbc:LineExtensionAmount currencyID="{str(currency)}">{str(round(float(str(total_amount)),2))}</cbc:LineExtensionAmount>
                    <cbc:TaxExclusiveAmount currencyID="{str(currency)}">{str(round(float(str(total_amount)),2))}</cbc:TaxExclusiveAmount>
                    <cbc:TaxInclusiveAmount currencyID="{str(currency)}">{str(round(float(str(total_amount_with_vat)),2))}</cbc:TaxInclusiveAmount>
                    <cbc:AllowanceTotalAmount currencyID="{str(currency)}">0.00</cbc:AllowanceTotalAmount>
                    <cbc:ChargeTotalAmount currencyID="{str(currency)}">0.00</cbc:ChargeTotalAmount>
                    <cbc:PrepaidAmount currencyID="{str(currency)}">0.00</cbc:PrepaidAmount>
                    <cbc:PayableRoundingAmount currencyID="{str(currency)}">0.00</cbc:PayableRoundingAmount>
                    <cbc:PayableAmount currencyID="{str(currency)}">{str(round(float(str(total_amount_with_vat)),2))}</cbc:PayableAmount>
                </cac:LegalMonetaryTotal>'''


                # print(total_amount)
                # eFacturaXML = meta + XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + " TAX TOTAL " + " LEGAL MONETARY TOOL " + invoiceLine +"</Invoice>"
                # Scrieți fișierul XML pentru fiecare factură în parte
                eFacturaXML = XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + PaymentMeans + TAXTOTAL + LegalMonetary + invoiceLine +"\n</Invoice>"
                def remove_diacritics(input_str):
                    nfkd_form = unicodedata.normalize('NFKD', input_str)
                    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

                eFacturaXML = remove_diacritics(eFacturaXML)
                eFacturaXML=eFacturaXML.replace("&"," ")

                # Scrie conținutul în fișierul XML
                # with io.open(f"C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/outs/SalesInvoiceValuta_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                #     f.write(eFacturaXML)
                    
                with io.open(f"/home/efactura/efactura_expeditors/outs/SalesInvoiceValuta_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                    f.write(eFacturaXML)

                print("A PRELUCRAT DATELE")
        #------------------------------CREDIT NOTE LEI--------------------------------------------------------------------------------------------------------------------------------

        else:
        # else:
            df_fact_curenta = Sales_EFACTURA.groupby(["Journal"]).get_group(listaNumarFact[i])
            issue_date = pd.to_datetime(df_fact_curenta["Date"]).dt.strftime('%Y-%m-%d').iloc[0]
            issue_date_datetime = pd.to_datetime(issue_date)

            # Calculează duedate adăugând 30 de zile la issue_date
            duedate = issue_date_datetime + pd.Timedelta(days=30)

            # Convert duedate la formatul 'YYYY-MM-DD'
            duedate_str = duedate.strftime('%Y-%m-%d')

            print("Due Date:", duedate_str)
            if str(df_fact_curenta["Foreign Currency"].iloc[0])=="RON":

                listaCote = list(set(list(df_fact_curenta["Cota"])))
                subtotalTva = df_fact_curenta.groupby("Cota")["Valoare linia TVA"].sum().reset_index()
                subtotalBaza=df_fact_curenta.groupby("Cota")["Amount"].sum().reset_index()
                subtotalIDTVA=df_fact_curenta.groupby("ID TVA")["Cota"].sum().reset_index()

                total_amount = 0
                tva_total=0
                creditNoteId = str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")
                if creditNoteId.isdigit():
                    # Convertește în întreg doar dacă este un număr
                    creditNoteId = int(creditNoteId)

                XML_Header = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n
                <CreditNote\nxmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2" 
            xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
            xmlns="urn:oasis:names:specification:ubl:schema:xsd:CreditNote-2">
            
            <cbc:CustomizationID>urn:cen.eu:en16931:2017#compliant#urn:efactura.mfinante.ro:CIUS-RO:1.0.1</cbc:CustomizationID>
                <cbc:ID>{creditNoteId}</cbc:ID>
                <cbc:IssueDate>{issue_date}</cbc:IssueDate>
                
                <cbc:CreditNoteTypeCode>{str(df_fact_curenta["Inv Type code"].iloc[0]).replace(".0", "")}</cbc:CreditNoteTypeCode>
                <cbc:DocumentCurrencyCode>RON</cbc:DocumentCurrencyCode>
                '''

                AccountingSupplierParty = '''
                <cac:AccountingSupplierParty>
                    <cac:Party>
                        <cac:PostalAddress>
                            <cbc:StreetName>'''+str(strada)+'''</cbc:StreetName>
                            <cbc:CityName>'''+str(oras)+'''</cbc:CityName>
                            <cbc:CountrySubentity>'''+str(countrySubentity)+'''</cbc:CountrySubentity>
                            <cac:Country>
                                <cbc:IdentificationCode>'''+str(country)+'''</cbc:IdentificationCode>
                            </cac:Country>
                        </cac:PostalAddress>
                        <cac:PartyTaxScheme>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:PartyTaxScheme>
                        <cac:PartyLegalEntity>
                            <cbc:RegistrationName>'''+str(numeCompanie)+'''</cbc:RegistrationName>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                        </cac:PartyLegalEntity>
                    </cac:Party>
                </cac:AccountingSupplierParty>
                '''
                
                if str(df_fact_curenta["STREET_CLIENT"].iloc[0]) == "  ":
                    AccountingCustomerPartyXML=f'''
                    <cac:AccountingCustomerParty>
                        <cac:Party>
                            <cac:PostalAddress>
                                <cbc:StreetName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:StreetName>
                                <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                                <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                                <cac:Country>
                                    <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                                </cac:Country>
                            </cac:PostalAddress>
                            <cac:PartyTaxScheme>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                                <cac:TaxScheme>
                                    <cbc:ID>VAT</cbc:ID>
                                </cac:TaxScheme>
                            </cac:PartyTaxScheme>
                            <cac:PartyLegalEntity>
                                <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            </cac:PartyLegalEntity>
                        </cac:Party>
                    </cac:AccountingCustomerParty>'''
                else:
                        AccountingCustomerPartyXML=f'''
                    <cac:AccountingCustomerParty>
                        <cac:Party>
                            <cac:PostalAddress>
                                <cbc:StreetName>{str(df_fact_curenta["STREET_CLIENT"].iloc[0])}</cbc:StreetName>
                                <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                                <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                                <cac:Country>
                                    <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                                </cac:Country>
                            </cac:PostalAddress>
                            <cac:PartyTaxScheme>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                                <cac:TaxScheme>
                                    <cbc:ID>VAT</cbc:ID>
                                </cac:TaxScheme>
                            </cac:PartyTaxScheme>
                            <cac:PartyLegalEntity>
                                <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            </cac:PartyLegalEntity>
                        </cac:Party>
                    </cac:AccountingCustomerParty>'''
                # invoiceLine += xml_efactura + AccountingCustomerPartyXML 
                # Variabilă pentru a număra elementele din fiecare factură
                invoiceLine = ""
                line_count = 1
                total_tva=0

                # print(subtotalTva)
                # <cbc:ID>{row["ID TVA"]}</cbc:ID>
                TAXTOTAL="\n<cac:TaxTotal>\n"
                TaxTotal =""
                for index, row in subtotalTva.iterrows():
                    taxamount=subtotalTva["Valoare linia TVA"][index].sum()
                    taxamounttotal=subtotalTva["Valoare linia TVA"].sum()
                    # taxamounttotal=normal_round(taxamounttotal, decimals=2)
                    baza = subtotalBaza["Amount"][index].sum()
                    baza=normal_round(baza, decimals=2)
                    # taxamount=normal_round(taxamount, decimals=2)
                    if str(subtotalIDTVA["ID TVA"][index])=="AE":
                        TaxExemptionReasonCode="VATEX-EU-AE"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="RON">{str(round(float(str(baza)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="RON">{str(round(float(str(row["Valoare linia TVA"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReasonCode>{TaxExemptionReasonCode}</cbc:TaxExemptionReasonCode>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    elif str(subtotalIDTVA["ID TVA"][index])=="E":
                        TaxExemptionReasonCode="Art.294,alin.1,lit.g din Codul fiscal-sunt scutite de TVA transport și serviciile auxiliare de import"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="RON">{str(round(float(str(baza)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="RON">{str(round(float(str(row["Valoare linia TVA"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReason>{TaxExemptionReasonCode}</cbc:TaxExemptionReason>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    else:
                        TaxTotal = TaxTotal + f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="RON">{str(round(float(str(baza)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="RON">{str(round(float(str(row["Valoare linia TVA"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                TAXTOTAL=TAXTOTAL+'<cbc:TaxAmount currencyID="RON">'+str(round(float(str(taxamounttotal)),2))+'</cbc:TaxAmount>'+TaxTotal+"\n</cac:TaxTotal>\n"

                for index, row in df_fact_curenta.iterrows():
                    line_amount = row["Amount"]
                    # line_amount=normal_round(line_amount, decimals=2)
                    val_cu_tva = row["Valoare linie cu TVA"]
                    tva=row["Valoare linia TVA"]
                    # tva=normal_round(tva, decimals=2)
                    # val_cu_tva=normal_round(val_cu_tva, decimals=2)
                    
                    total_tva += val_cu_tva
                    tva_total+=tva
                    total_amount += line_amount
                    # total_amount=normal_round(total_amount, decimals=2)
                    # tva_total=normal_round(tva_total, decimals=2)

                    invoiceLine += f'''<cac:CreditNoteLine>
                            <cbc:ID>{line_count}</cbc:ID>
                            <cbc:CreditedQuantity unitCode="{row["Cod Unitate Masura"]}">{row["Quantity"]}</cbc:CreditedQuantity>
                            <cbc:LineExtensionAmount currencyID="RON">{str(round(float(str(row["Amount"])),2))}</cbc:LineExtensionAmount>
                            <cac:Item>
                                <cbc:Name>{row["Billing Description"]}</cbc:Name>
                                <cac:ClassifiedTaxCategory>
                                    <cbc:ID>{row["ID TVA"]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:ClassifiedTaxCategory>
                            </cac:Item>
                            <cac:Price>
                                <cbc:PriceAmount currencyID="RON">{str(abs(round(float(str(row["Amount"])),2)))}</cbc:PriceAmount>
                            </cac:Price>
                        </cac:CreditNoteLine>'''
                        
                    

                    # Incrementați numărul elementului pentru următoarea linie din factură
                    line_count += 1
                total_amount_with_vat =total_amount +tva_total
                # total_amount_with_vat=normal_round(total_amount_with_vat, decimals=2)
                # total_amount_with_vat=normal_round(total_amount_with_vat, decimals=2) 
                
                PaymentMeans = f'''
                    <cac:PaymentMeans>
                    <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                    <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                    <cac:PayeeFinancialAccount>
                        <cbc:ID>RO34INGB0001008222848910</cbc:ID>
                        <cbc:Name>CONT ING BANK SA IN RON</cbc:Name>
                        <cac:FinancialInstitutionBranch>
                            <cbc:ID>INGBROBU</cbc:ID>
                        </cac:FinancialInstitutionBranch>
                    </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO80INGB0001008222840710</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN EUR</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO26INGB0001008222844010</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN USD</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>'''  


                

                LegalMonetary = f'''
                <cac:LegalMonetaryTotal>
                    <cbc:LineExtensionAmount currencyID="RON">{str(round(float(str(total_amount)),2))}</cbc:LineExtensionAmount>
                    <cbc:TaxExclusiveAmount currencyID="RON">{str(round(float(str(total_amount)),2))}</cbc:TaxExclusiveAmount>
                    <cbc:TaxInclusiveAmount currencyID="RON">{str(round(float(str(total_amount_with_vat)),2))}</cbc:TaxInclusiveAmount>
                    <cbc:AllowanceTotalAmount currencyID="RON">0.00</cbc:AllowanceTotalAmount>
                    <cbc:ChargeTotalAmount currencyID="RON">0.00</cbc:ChargeTotalAmount>
                    <cbc:PrepaidAmount currencyID="RON">0.00</cbc:PrepaidAmount>
                    <cbc:PayableRoundingAmount currencyID="RON">0.00</cbc:PayableRoundingAmount>
                    <cbc:PayableAmount currencyID="RON">{str(round(float(str(total_amount_with_vat)),2))}</cbc:PayableAmount>
                </cac:LegalMonetaryTotal>'''
                
                

                eFacturaXML = XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + PaymentMeans + TAXTOTAL + LegalMonetary + invoiceLine +"\n</CreditNote>"
                eFacturaXML=eFacturaXML.replace("&"," ")
                def remove_diacritics(input_str):
                    nfkd_form = unicodedata.normalize('NFKD', input_str)
                    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

                eFacturaXML = remove_diacritics(eFacturaXML)

                # Scrie conținutul în fișierul XML
                # with io.open(f"C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/outs/SalesCreditNote_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                #     f.write(eFacturaXML)
                with io.open(f"/home/efactura/efactura_expeditors/outs/SalesCreditNote_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                    f.write(eFacturaXML)

                print("A PRELUCRAT DATELE")


        #------------------------------CREDIT NOTE VALUTA--------------------------------------------------------------------------------------------------------------------------------
            else:
                df_fact_curenta = Sales_EFACTURA.groupby(["Journal"]).get_group(listaNumarFact[i])
                issue_date = pd.to_datetime(df_fact_curenta["Date"]).dt.strftime('%Y-%m-%d').iloc[0]
                issue_date_datetime = pd.to_datetime(issue_date)

                # Calculează duedate adăugând 30 de zile la issue_date
                duedate = issue_date_datetime + pd.Timedelta(days=30)

                # Convert duedate la formatul 'YYYY-MM-DD'
                duedate_str = duedate.strftime('%Y-%m-%d')

                print("Due Date:", duedate_str)
                currency=str(df_fact_curenta["Foreign Currency"].iloc[0])
                
                listaCote = list(set(list(df_fact_curenta["Cota"])))
                subtotalTvaLEI=df_fact_curenta.groupby("Cota")["Valoare linia TVA"].sum().reset_index()
                subtotalTva = df_fact_curenta.groupby("Cota")["Valoare linia TVA (Valuta)"].sum().reset_index()
                subtotalBaza=df_fact_curenta.groupby("Cota")["Amount"].sum().reset_index()
                subtotalBazaValuta=df_fact_curenta.groupby("Cota")["Foreign Amount"].sum().reset_index()
                subtotalTvaValuta=df_fact_curenta.groupby("Cota")["Valoare linia TVA (Valuta)"].sum().reset_index()
                subtotalIDTVA=df_fact_curenta.groupby("ID TVA")["Cota"].sum().reset_index()
                
                total_amount = 0
                tva_total=0
                creditNoteId2 = str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")
                if creditNoteId2.isdigit():
                    creditNoteId2 = int(creditNoteId2)
                

                XML_Header = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n
                <CreditNote\nxmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2" 
            xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
            xmlns="urn:oasis:names:specification:ubl:schema:xsd:CreditNote-2">
            
            <cbc:CustomizationID>urn:cen.eu:en16931:2017#compliant#urn:efactura.mfinante.ro:CIUS-RO:1.0.1</cbc:CustomizationID>
                <cbc:ID>{creditNoteId2}</cbc:ID>
                <cbc:IssueDate>{issue_date}</cbc:IssueDate>
                
                <cbc:CreditNoteTypeCode>{str(df_fact_curenta["Inv Type code"].iloc[0]).replace(".0", "")}</cbc:CreditNoteTypeCode>
                <cbc:DocumentCurrencyCode>{str(df_fact_curenta['Foreign Currency'].iloc[0])}</cbc:DocumentCurrencyCode>
                <cbc:TaxCurrencyCode>RON</cbc:TaxCurrencyCode>
                '''

                AccountingSupplierParty = '''
                <cac:AccountingSupplierParty>
                    <cac:Party>
                        <cac:PostalAddress>
                            <cbc:StreetName>'''+str(strada)+'''</cbc:StreetName>
                            <cbc:CityName>'''+str(oras)+'''</cbc:CityName>
                            <cbc:CountrySubentity>'''+str(countrySubentity)+'''</cbc:CountrySubentity>
                            <cac:Country>
                                <cbc:IdentificationCode>'''+str(country)+'''</cbc:IdentificationCode>
                            </cac:Country>
                        </cac:PostalAddress>
                        <cac:PartyTaxScheme>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:PartyTaxScheme>
                        <cac:PartyLegalEntity>
                            <cbc:RegistrationName>'''+str(numeCompanie)+'''</cbc:RegistrationName>
                            <cbc:CompanyID>'''+str(vatID)+'''</cbc:CompanyID>
                        </cac:PartyLegalEntity>
                    </cac:Party>
                </cac:AccountingSupplierParty>
                '''
                
                if str(df_fact_curenta["STREET_CLIENT"].iloc[0]) == "  ":
                    AccountingCustomerPartyXML=f'''
                    <cac:AccountingCustomerParty>
                        <cac:Party>
                            <cac:PostalAddress>
                                <cbc:StreetName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:StreetName>
                                <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                                <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                                <cac:Country>
                                    <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                                </cac:Country>
                            </cac:PostalAddress>
                            <cac:PartyTaxScheme>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                                <cac:TaxScheme>
                                    <cbc:ID>VAT</cbc:ID>
                                </cac:TaxScheme>
                            </cac:PartyTaxScheme>
                            <cac:PartyLegalEntity>
                                <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                                <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            </cac:PartyLegalEntity>
                        </cac:Party>
                    </cac:AccountingCustomerParty>'''
                else:
                    AccountingCustomerPartyXML=f'''
                <cac:AccountingCustomerParty>
                    <cac:Party>
                        <cac:PostalAddress>
                            <cbc:StreetName>{str(df_fact_curenta["STREET_CLIENT"].iloc[0])}</cbc:StreetName>
                            <cbc:CityName>{str(df_fact_curenta["CITY_CLIENT"].iloc[0])}</cbc:CityName>
                            <cbc:CountrySubentity>RO-{df_fact_curenta["REGION"].iloc[0]}</cbc:CountrySubentity>
                            <cac:Country>
                                <cbc:IdentificationCode>{str(df_fact_curenta["COUNTRY_CLIENT"].iloc[0])}</cbc:IdentificationCode>
                            </cac:Country>
                        </cac:PostalAddress>
                        <cac:PartyTaxScheme>
                            <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:PartyTaxScheme>
                        <cac:PartyLegalEntity>
                            <cbc:RegistrationName>{str(df_fact_curenta["Company"].iloc[0])}</cbc:RegistrationName>
                            <cbc:CompanyID>{str(df_fact_curenta["CUI_CLIENT"].iloc[0])}</cbc:CompanyID>
                        </cac:PartyLegalEntity>
                    </cac:Party>
                </cac:AccountingCustomerParty>'''
                # invoiceLine += xml_efactura + AccountingCustomerPartyXML 
                # Variabilă pentru a număra elementele din fiecare factură
                invoiceLine = ""
                line_count = 1
                total_tva=0
                # print(subtotalTva)
                # <cbc:ID>{row["ID TVA"]}</cbc:ID>
                TAXTOTAL="\n<cac:TaxTotal>\n"
                TaxTotal =""
                for index, row in subtotalTva.iterrows():
                    taxamount=subtotalTvaValuta["Valoare linia TVA (Valuta)"][index].sum()
                    taxamounttotal=subtotalTvaValuta["Valoare linia TVA (Valuta)"].sum()
                    taxamounttotalLEI=subtotalTvaLEI["Valoare linia TVA"].sum()
                    taxamounttotal=normal_round(taxamounttotal, decimals=2)
                    taxamount=normal_round(taxamount, decimals=2)
                    taxamounttotalLEI=normal_round(taxamounttotalLEI, decimals=2)
                    bazaV = subtotalBazaValuta["Foreign Amount"][index].sum()
                    baza= subtotalBaza["Amount"][index].sum()
                    # baza=normal_round(baza, decimals=2)
                    # bazaV=normal_round(bazaV, decimals=2)

                    if str(subtotalIDTVA["ID TVA"][index])=="AE":

                        TaxExemptionReasonCode="VATEX-EU-AE"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="{str(currency)}">{str(round(float(str(bazaV)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="{str(currency)}">{str(round(float(str(row["Valoare linia TVA (Valuta)"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReasonCode>{TaxExemptionReasonCode}</cbc:TaxExemptionReasonCode>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    elif str(subtotalIDTVA["ID TVA"][index])=="E":

                        TaxExemptionReasonCode="Art.294,alin.1,lit.g din Codul fiscal-sunt scutite de TVA transport și serviciile auxiliare de import"
                        TaxTotal = TaxTotal+f'''
                        
                            
                            <cac:TaxSubtotal>
                                <cbc:TaxableAmount currencyID="{str(currency)}">{str(round(float(str(bazaV)),2))}</cbc:TaxableAmount>
                                <cbc:TaxAmount currencyID="{str(currency)}">{str(round(float(str(row["Valoare linia TVA (Valuta)"])),2))}</cbc:TaxAmount>
                                <cac:TaxCategory>
                                    <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cbc:TaxExemptionReason>{TaxExemptionReasonCode}</cbc:TaxExemptionReason>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                    else:
                        TaxTotal = TaxTotal + f'''

                            <cac:TaxSubtotal>
                                    <cbc:TaxableAmount currencyID="{str(currency)}">{str(round(float(str(bazaV)),2))}</cbc:TaxableAmount>
                                    <cbc:TaxAmount currencyID="{str(currency)}">{str(round(float(str(row["Valoare linia TVA (Valuta)"])),2))}</cbc:TaxAmount>
                                    <cac:TaxCategory>
                                        <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                                        <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                        <cac:TaxScheme>
                                            <cbc:ID>VAT</cbc:ID>
                                        </cac:TaxScheme>
                                    </cac:TaxCategory>
                            </cac:TaxSubtotal>
                        \n'''
                        # print("abc")
                TAXTOTAL = TAXTOTAL + '<cbc:TaxAmount currencyID="RON">' + str(round(float(str(taxamounttotalLEI)),2)) +'</cbc:TaxAmount>' + "\n</cac:TaxTotal>\n"+ TAXTOTAL + '<cbc:TaxAmount currencyID="'+str(currency)+'">' + str(round(float(str(taxamounttotal)),2)) +'</cbc:TaxAmount>' + TaxTotal + "\n</cac:TaxTotal>\n"
                for index, row in df_fact_curenta.iterrows():
                    line_amount = row["Foreign Amount"]
                    currency=row["Foreign Currency"]
                    # line_amount=normal_round(line_amount, decimals=2)
                    val_cu_tva = row["Valoare linie cu TVA (Valuta)"]
                    tva = row["Valoare linia TVA (Valuta)"]
                    # tva = normal_round(tva, decimals=2)
                    
                    total_tva += val_cu_tva
                    tva_total += tva
                    total_amount += line_amount
                    total_tva=normal_round(total_tva, decimals=2)
                    total_amount=normal_round(total_amount, decimals=2)
                    invoiceLine += f'''<cac:CreditNoteLine>
                            <cbc:ID>{line_count}</cbc:ID>
                            <cbc:CreditedQuantity unitCode="{row["Cod Unitate Masura"]}">{row["Quantity"]}</cbc:CreditedQuantity>
                            <cbc:LineExtensionAmount currencyID="{str(row["Foreign Currency"])}">{str(round(float(str(row["Foreign Amount"])),2))}</cbc:LineExtensionAmount>
                            <cac:Item>
                                <cbc:Name>{row["Billing Description"]}</cbc:Name>
                                <cac:ClassifiedTaxCategory>
                                    <cbc:ID>{row["ID TVA"]}</cbc:ID>
                                    <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                    <cac:TaxScheme>
                                        <cbc:ID>VAT</cbc:ID>
                                    </cac:TaxScheme>
                                </cac:ClassifiedTaxCategory>
                            </cac:Item>
                            <cac:Price>
                                <cbc:PriceAmount currencyID="{str(row["Foreign Currency"])}">{str(abs(round(float(str(row["Foreign Amount"])),2)))}</cbc:PriceAmount>
                            </cac:Price>
                        </cac:CreditNoteLine>'''
                        
                    
                    
                    # Incrementați numărul elementului pentru următoarea linie din factură
                    line_count += 1
                total_amount_with_vat = total_amount + tva_total
                # total_amount_with_vat=normal_round(total_amount_with_vat, decimals=2)
                # print(row["Journal"], total_tva)
                # print(str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "") ,total_amount_without_vat)

                PaymentMeans = f'''
                    <cac:PaymentMeans>
                    <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                    <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                    <cac:PayeeFinancialAccount>
                        <cbc:ID>RO34INGB0001008222848910</cbc:ID>
                        <cbc:Name>CONT ING BANK SA IN RON</cbc:Name>
                        <cac:FinancialInstitutionBranch>
                            <cbc:ID>INGBROBU</cbc:ID>
                        </cac:FinancialInstitutionBranch>
                    </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO80INGB0001008222840710</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN EUR</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>
                    <cac:PaymentMeans>
                        <cbc:PaymentMeansCode>42</cbc:PaymentMeansCode>
                        <cbc:PaymentID>{str(df_fact_curenta["Journal"].iloc[0]).replace(".0", "")}</cbc:PaymentID>
                        <cac:PayeeFinancialAccount>
                            <cbc:ID>RO26INGB0001008222844010</cbc:ID>
                            <cbc:Name>CONT ING BANK SA IN USD</cbc:Name>
                            <cac:FinancialInstitutionBranch>
                                <cbc:ID>INGBROBU</cbc:ID>
                            </cac:FinancialInstitutionBranch>
                        </cac:PayeeFinancialAccount>
                    </cac:PaymentMeans>'''  


                LegalMonetary = f'''
                <cac:LegalMonetaryTotal>
                    <cbc:LineExtensionAmount currencyID="{str(currency)}">{str(round(float(str(total_amount)),2))}</cbc:LineExtensionAmount>
                    <cbc:TaxExclusiveAmount currencyID="{str(currency)}">{str(round(float(str(total_amount)),2))}</cbc:TaxExclusiveAmount>
                    <cbc:TaxInclusiveAmount currencyID="{str(currency)}">{str(round(float(str(total_amount_with_vat)),2))}</cbc:TaxInclusiveAmount>
                    <cbc:AllowanceTotalAmount currencyID="{str(currency)}">0.00</cbc:AllowanceTotalAmount>
                    <cbc:ChargeTotalAmount currencyID="{str(currency)}">0.00</cbc:ChargeTotalAmount>
                    <cbc:PrepaidAmount currencyID="{str(currency)}">0.00</cbc:PrepaidAmount>
                    <cbc:PayableRoundingAmount currencyID="{str(currency)}">0.00</cbc:PayableRoundingAmount>
                    <cbc:PayableAmount currencyID="{str(currency)}">{str(round(float(str(total_amount_with_vat)),2))}</cbc:PayableAmount>
                </cac:LegalMonetaryTotal>'''


                # print(total_amount)
                # eFacturaXML = meta + XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + " TAX TOTAL " + " LEGAL MONETARY TOOL " + invoiceLine +"</Invoice>"
                # Scrieți fișierul XML pentru fiecare factură în parte
                eFacturaXML = XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + PaymentMeans + TAXTOTAL +LegalMonetary + invoiceLine +"\n</CreditNote>"
                def remove_diacritics(input_str):
                    nfkd_form = unicodedata.normalize('NFKD', input_str)
                    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

                eFacturaXML = remove_diacritics(eFacturaXML)
                eFacturaXML=eFacturaXML.replace("&"," ")

                # Scrie conținutul în fișierul XML
                # with io.open(f"C:/Dezvoltare/E-Factura/2023/eFactura/Expeditors/eFacturaExpeditors/outs/SalesCreditNoteValuta_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                #     f.write(eFacturaXML)
                with io.open(f"/home/efactura/efactura_expeditors/outs/SalesCreditNoteValuta_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                    f.write(eFacturaXML)

                print("A PRELUCRAT DATELE")
                
    return primaFactura, ultimaFactura, totalFactura, nrFacturiTrimise, facturiNuleUnice
# prelucrareDate(Sales_EFACTURA, fisierComentarii)
