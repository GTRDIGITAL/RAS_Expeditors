import pandas as pd
import datetime
import xml.etree.ElementTree as ET  
# import mysql.connector
import sys
import logging
from datetime import datetime
import os
from sqlalchemy import create_engine
import unicodedata
from flask import session
import json
import math
def normal_round(n, decimals=0):
    expoN = n * 10 ** decimals
    if abs(expoN) - abs(math.floor(expoN)) < 0.5:
        return math.floor(expoN) / 10 ** decimals
    return math.ceil(expoN) / 10 ** decimals

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']
# global primaFactura, ultimaFactura, totalFactura, numarFacturiTrimise
# facturi_nule=[]
# facturi_nule.clear()
facturiNuleUnice=""
# ultimaFactura = session.get('ultimaFactura')


fisierDeVanzari = "C:/Dezvoltare/E-Factura/2023/eFactura/Ferro/eFacturaFerro/Baza de date vanzari/3370_Vanzari SAP.xlsx"
def prelucrareDate(fisierDeVanzari):
    def stergeFisiere(directory_path, file_extension):
        try:
            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)
                if filename.endswith(file_extension):
                    os.remove(file_path)
                    print(f"Fisierul {filename} a fost sters.")
        except Exception as e:
            print(f"Eroare la stergerea fișierelor: {str(e)}")
    
    stergeFisiere('C:/Dezvoltare/E-Factura/2023/eFactura/Ferro/eFacturaFerro/outs', '.xml')
    # stergeFisiere("/home/efactura/efactura_ferro/outs", ".xml")
        # Calea către directorul de loguri
    log_folder = "C:/Dezvoltare/E-Factura/2023/eFactura/Ferro/eFacturaFerro/logs"
    # log_folder = "/home/efactura/efactura_ferro/logs"
    facturi_nule=[]
    print(fisierDeVanzari,'prelucrare date----------')
    os.makedirs(log_folder, exist_ok=True)
    log_path = os.path.join(log_folder, "informatii.txt")
    if os.path.exists(log_path):
        os.remove(log_path)

    file_path = fisierDeVanzari
    

    # Numele coloanelor obligatorii
    coloane_obligatorii = ["Billing Document", "Material", "Billing Date", "sold-to party", "Sold-to-name",
                            "bill qty ZSDSABIL", "AC bill net val", "Description", "Net value", "Document Currency",
                            "Sales unit"]

    # Încărcare fișier Excel
    try:
        df = pd.read_excel(file_path, sheet_name='vanzari')
        df=df.loc[df['Billing Document'].astype(str)!="nan"]
        dp = pd.read_excel(file_path, sheet_name='Fisa cont venituri')
        
    except Exception as e:
        # er="Fisier invalid!"
        generare_fisier_text("Eroare la citirea fisierului Excel-va rugam sa va asigurati ca nu exista Sheet-uri in plus in fisierul importat, iar cele existente au denumirea corecta!De asemenea, coloanele 'Billing Document si Document Currency trebuie sa fie prezenta si sa nu aiba valori nule!'", {e})
        return
    # print(df)
    # df=df.dropna(subset=["Billing Document"])
    print('bac')
    # Verificare dacă toate coloanele obligatorii există în fișierul Excel
    coloane_lipsa = [col for col in coloane_obligatorii if col not in df.columns]
    if coloane_lipsa:
        # Generare fișier text cu informații despre coloanele lipsă
        generare_fisier_text("-------------------COLOANE LIPSA------------------------\n\n'Următoarele coloane obligatorii lipsesc din fișierul Excel:\n", coloane_lipsa)
        # logging.error(f"Coloanele obligatorii lipsesc din fișierul Excel: {', '.join(coloane_lipsa)}")

    # Verificare dacă există valori nule în coloanele obligatorii și generare informații despre pozițiile lor
    filtrareRO = df.loc[df['Document Currency'] == 'RON']
    print(filtrareRO)
    for coloana in coloane_obligatorii:
        try:
            if filtrareRO[coloana].isnull().any():
                # Obținere poziții cu valori nule
                pozitii_nule = filtrareRO[filtrareRO[coloana].isnull()].index + 2
                
                # print(facturi_nule)
                generare_fisier_text(f"-------------------VALORI NULE------------------------\n\n'Coloana {coloana} conține valori nule la linia/liniile din fisierul Excel:\n", pozitii_nule)
                print(coloana)
                if coloana!='Billing Document':
                    print(filtrareRO.loc[filtrareRO[coloana].isnull(), 'Billing Document'])
                    facturi_nule.extend(filtrareRO.loc[filtrareRO[coloana].isnull(), 'Billing Document'].values)
                else:
                    facturi_nule.append("nan")
                    
                # logging.error(f"Coloana {coloana} conține valori nule la liniile: {pozitii_nule.tolist()}")
        except KeyError:
            print("Coloana missing")
            

    print(coloana, 'plm')
    facturiNuleUnice = len(list(set(list(facturi_nule))))
    print(facturi_nule,"---------------------------")
    #daca avem x valori "nan" in lista
    # date=datetime.datetime.now().date()
    strada = "ALEEA SINAIA, 120"
    oras = "DOICESTI"
    codPostal = "137195"
    countrySubentity = "RO-DB"
    country = "RO"
    vatID ="RO901512" 
    numeCompanie = "Vibrantz Performance Pigments Romania SRL"
    contactPersonFirstName="IULIANA"
    contactPersonLastName="CONSTANTIN"
    contactPhone="0731490713"
    IBAN="RO38RNCB0128045426650001"


    # print(xml_efactura)

    # with open("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/outs/Header.xml", "w", encoding="utf-8") as f:

    #     f.write(xml)
    # if exista informatii.txt:
    #     returnam fisier erori
    # else:
        
    # filtrareRO = df.loc[df['Document Currency'] == 'RON']
    Sales_EFACTURA=pd.DataFrame()

    
    
    Sales_EFACTURA["Inv. No"] = filtrareRO["Billing Document"]
   
    
    Sales_EFACTURA["Material"] = filtrareRO["Material"]
    
    
    Sales_EFACTURA["Inv. Date"] = filtrareRO["Billing Date"]
    
    
    Sales_EFACTURA["ClientNo"] = filtrareRO["sold-to party"]
    
    
    Sales_EFACTURA["ClientName"] = filtrareRO["Sold-to-name"]
    
    # Sales_EFACTURA["AccountID"] = filtrareRO["Alternative Account"]

    
    Sales_EFACTURA["Quantity"] = filtrareRO["bill qty ZSDSABIL"]
    
    
    Sales_EFACTURA["UnitPrice"] = filtrareRO["AC bill net val"] / filtrareRO["bill qty ZSDSABIL"]
    
    
    Sales_EFACTURA["Description"] = filtrareRO["Description"]
    
    
    Sales_EFACTURA["Amount in doc ccy"] = filtrareRO["Net value"]
    
    
    Sales_EFACTURA["Amount in local ccy"] = filtrareRO["AC bill net val"]
    
    
    Sales_EFACTURA["CcyCode"] = filtrareRO["Document Currency"]
    
    
    Sales_EFACTURA["Unitate Masura"] = filtrareRO["Sales unit"]
    
    listaClientSales = list(Sales_EFACTURA["ClientNo"])

    # try:
    listaNumarFact = list(set(list(Sales_EFACTURA["Inv. No"])))
    # except Exception as e:
    #     logging.error(f"Coloana Billing Document nu exista. Eroare: {e}")

 



    listaClientNameSalesrap=list(Sales_EFACTURA["ClientName"])
    listaCUIRaportdepus=[]
    listaSalesCountry=[]
    listaLocalitateRap=[]

    try:
        for i in range(0, len(listaClientSales)):
            listaClientSales[i]=str(listaClientSales[i]).replace(" ","").replace(".0","")
        listaCUIRaportdepus.append("")
        listaSalesCountry.append("")
        listaLocalitateRap.append("")
        # print(listaClientSales)

        # print(Sales_EFACTURA)
        # abc = Sales_EFACTURA.groupby('Inv. No').size()


        # COD CLIENTI COMENTAT:
        #    # --------------------------------------------------------------CLIENTI--------------------------------------------------------------------

        #     Clients = pd.read_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/Baza de date vanzari/Clients.xlsx")

        # conexiune baza de date
        # engine = create_engine('mysql+mysqlconnector://userAdmin:some_pass@192.168.1.222/efacturaferro')
        engine = create_engine(f"mysql://{config['mysql']['user']}:{config['mysql']['password']}@{config['mysql']['host']}/{config['mysql']['database']}")
        print("CONECTAT LA BAZA")
        query = "SELECT * FROM clients WHERE region IS NOT NULL"

        bazaClienti = pd.read_sql(query, engine)
        # print(bazaClienti)
        engine.dispose()
        print("Deconectat de la baza de date")

        SalesInv=""
        listaInvoiceNo=list(set(listaClientSales))
        # print('asta e un len ',listaInvoiceNo)

        dictClientName=bazaClienti.set_index('CUST#').to_dict()['Name']
        dictClientCountry=bazaClienti.set_index('CUST#').to_dict()['Country']
        dictClientCity=bazaClienti.set_index('CUST#').to_dict()['City']
        dictClientRegNo=bazaClienti.set_index('CUST#').to_dict()['regno']
        dictClientStreet=bazaClienti.set_index('CUST#').to_dict()['Street']
        dictClientRegiune=bazaClienti.set_index('CUST#').to_dict()['region']
        dpinVanzari=dp[dp['DocumentNo'].isin(list(Sales_EFACTURA["Inv. No"]))]
        dictCodCota = dpinVanzari.set_index('DocumentNo').to_dict()['Tx']
        dictCota={'A1': 19.00, 'B0': 0.00}
        # print(dictClientCity)
        # Sales_EFACTURA["Name"] = Sales_EFACTURA["ClientName"].map(dictClientName)
        Sales_EFACTURA["Country"] = Sales_EFACTURA["ClientNo"].map(dictClientCountry)
        Sales_EFACTURA["City"] = Sales_EFACTURA["ClientNo"].map(dictClientCity)
        Sales_EFACTURA["RegNo"] = Sales_EFACTURA["ClientNo"].map(dictClientRegNo)
        Sales_EFACTURA["Street"] = Sales_EFACTURA["ClientNo"].map(dictClientStreet)
        Sales_EFACTURA["CodRegiune"] = Sales_EFACTURA["ClientNo"].map(dictClientRegiune)
        Sales_EFACTURA["Tx"] = Sales_EFACTURA['Inv. No'].map(dictCodCota)
        Sales_EFACTURA["Cota"] = Sales_EFACTURA['Tx'].map(dictCota)
        # print(Sales_EFACTURA["ClientNo"])


        dictTaxCodeVzIDTVA={'B0':"E",	'A1':"S"}
        dictUnitateMasura={"KG":"KGM"}
        # dictTaxCodeVzDescriere={310301:"Livrări intracomunitare de bunuri, scutite",310306:"Prestări de servicii intracomunitare care beneficiază de scutire in statul membru in care taxa este datorată",	310309:"Livrări de bunuri şi prestări de servicii taxabile cu cota 19%",	310310:"Livrări de bunuri şi prestări de servicii taxabile cu cota 9%",	310312:"Livrari de bunuri si prestari de servicii supuse masurilor de simplificare (taxare inversa)",	310313:"Livrari de bunuri scutite cu drept de deducere cf Art. 294 alin (1) lit a) si b) din Codul Fiscal  (Exporturi)"}
        # dictTaxCodeVzTaxType={310301:300,	310309:300,	310310:300,310306:300,	310312:300,	310313:300, None:0}
        # JurnalVz["Cota TVA"]=JurnalVz["TaxCode"].map(dictTaxCodeVzCotaTVA)
        # JurnalVz["TaxType"]=JurnalVz["TaxCode"].map(dictTaxCodeVzTaxType)
        # JurnalVz["TaxAmount"]=JurnalVz["FinalAmount"]*JurnalVz["Cota TVA"]

        # #-------------aducere taxcode din jurnal in raport de sales----------------------------

        # listaTaxCodeJurnal=list(JurnalVz["TaxCode"])
        # listaFactJurnal=list(JurnalVz["Numar document"])

        # try:
        #     listaFacturaRaportVanzari=list(Sales_EFACTURA["Inv. No"])
        # except:
        #     print("Coloana Billing Document nu exista.")
        # taxcoderaportsales=[]
        # while True:
        #     try:
        #         for i in range(0, len(listaFacturaRaportVanzari)):
        #             ok=0
        #             for j in range(0, len(listaFactJurnal)):
        #                 if ok==0:
        #                     # print(str(listaFacturaRaportVanzari[i]).replace(".0",""),str(listaFactJurnal[j]).replace(".0","") )
        #                     if str(listaFacturaRaportVanzari[i]).replace(".0","")==str(listaFactJurnal[j]).replace(".0",""):
        #                         taxcoderaportsales.append(listaTaxCodeJurnal[j])
        #                         ok=1
        #             if ok==0:
        #                 taxcoderaportsales.append(None)	
        #     except:
        #         print("Coloana Billing Document nu exista.")
        #     break
            

        # Sales_EFACTURA["TaxCode"]=taxcoderaportsales
        # # print(Sales_EFACTURA)
        # Sales_EFACTURA["Cota"] = Sales_EFACTURA["TaxCode"].map(dictTaxCodeVzCotaTVA)
        Sales_EFACTURA["ID TVA"] = Sales_EFACTURA["Tx"].map(dictTaxCodeVzIDTVA)
        Sales_EFACTURA["Pret Unitar"] = Sales_EFACTURA['Amount in doc ccy']/Sales_EFACTURA["Quantity"]
        Sales_EFACTURA["Cod Unitate Masura"] = Sales_EFACTURA["Unitate Masura"].map(dictUnitateMasura)
        Sales_EFACTURA["Pret cu TVA"] = Sales_EFACTURA["Pret Unitar"] * Sales_EFACTURA["Cota"]
        Sales_EFACTURA["Valoare linia TVA"] = Sales_EFACTURA["Amount in local ccy"] * (Sales_EFACTURA["Cota"] / 100)
        Sales_EFACTURA["Valoare linie cu TVA"] = Sales_EFACTURA["Valoare linia TVA"] + Sales_EFACTURA["Amount in local ccy"]
        Sales_EFACTURA['is_positive_flagOB'] = Sales_EFACTURA['Amount in local ccy'] >= 0
        Sales_EFACTURA['is_positive_flagOB']=Sales_EFACTURA['is_positive_flagOB'].astype(str).str.replace("True","D")
        Sales_EFACTURA['is_positive_flagOB']=Sales_EFACTURA['is_positive_flagOB'].astype(str).str.replace("False","C")
        
        total_factura=Sales_EFACTURA.groupby("Inv. No")["Amount in local ccy"].transform('sum')
        Sales_EFACTURA["Total Factura"]=total_factura
        totalFactura=Sales_EFACTURA["Amount in local ccy"].sum()
        primaFactura = list(Sales_EFACTURA["Inv. No"])[0]
        ultimaFactura=list(Sales_EFACTURA["Inv. No"])[-1]
        print(totalFactura, primaFactura, ultimaFactura)
        print("asta e prima factura in prelucrare_date.py ",primaFactura)
        
        # Sales_EFACTURA.to_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/outs/Output.xlsx", index=False)
        # Sales_EFACTURA.to_excel("/home/efactura/efactura_ferro/bazadatevanzari/Output.xlsx", index=False)

        issue_date = pd.to_datetime(Sales_EFACTURA["Inv. Date"]).dt.strftime('%Y-%m-%d').iloc[0]
        nrFacturiTrimise = len(listaNumarFact)

        for i in range(0, len(listaNumarFact)):
            df_fact_curenta = Sales_EFACTURA.groupby(["Inv. No"]).get_group(listaNumarFact[i])
            
            listaCote = list(set(list(df_fact_curenta["Cota"])))
            subtotalTva = df_fact_curenta.groupby("Cota")["Valoare linia TVA"].sum().reset_index()
            subtotalBaza=df_fact_curenta.groupby("Cota")["Amount in local ccy"].sum().reset_index()
            subtotalIDTVA=df_fact_curenta.groupby("ID TVA")["Cota"].sum().reset_index()
            
            total_amount = 0

            XML_Header = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n
            <Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2" xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"\n xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2" xmlns:ns4="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"\n xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2 http://docs.oasis-open.org/ubl/os-UBL-2.1/xsd/maindoc/UBL-Invoice-2.1.xsd">
            <cbc:CustomizationID>urn:cen.eu:en16931:2017#compliant#urn:efactura.mfinante.ro:CIUS-RO:1.0.1</cbc:CustomizationID>
            <cbc:ID>{str(df_fact_curenta["Inv. No"].iloc[0]).replace(".0", "")}</cbc:ID>
            <cbc:IssueDate>{issue_date}</cbc:IssueDate>
            <cbc:DueDate>1999-01-01</cbc:DueDate>
            <cbc:InvoiceTypeCode>380</cbc:InvoiceTypeCode>
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
            AccountingCustomerPartyXML=f'''
            <cac:AccountingCustomerParty>
                <cac:Party>
                    <cac:PostalAddress>
                        <cbc:StreetName>{str(df_fact_curenta["Street"].iloc[0])}</cbc:StreetName>
                        <cbc:CityName>{str(df_fact_curenta["City"].iloc[0])}</cbc:CityName>
                        <cbc:CountrySubentity>RO-{df_fact_curenta["CodRegiune"].iloc[0]}</cbc:CountrySubentity>
                        <cac:Country>
                            <cbc:IdentificationCode>{str(df_fact_curenta["Country"].iloc[0])}</cbc:IdentificationCode>
                        </cac:Country>
                    </cac:PostalAddress>
                    <cac:PartyTaxScheme>
                        <cbc:CompanyID>{str(df_fact_curenta["RegNo"].iloc[0])}</cbc:CompanyID>
                        <cac:TaxScheme>
                            <cbc:ID>VAT</cbc:ID>
                        </cac:TaxScheme>
                    </cac:PartyTaxScheme>
                    <cac:PartyLegalEntity>
                        <cbc:RegistrationName>{str(df_fact_curenta["ClientName"].iloc[0])}</cbc:RegistrationName>
                        <cbc:CompanyID>RO37623474</cbc:CompanyID>
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
            for index, row in subtotalTva.iterrows():
                taxamount=subtotalTva["Valoare linia TVA"].sum()
                baza = subtotalBaza["Amount in local ccy"].sum()
                taxamount = normal_round(taxamount, decimals=2)
                taxamount2 = row["Valoare linia TVA"]
                taxamount2 = normal_round(taxamount2, decimals=2)
                TaxTotal = f'''
                <cac:TaxTotal>
                    <cbc:TaxAmount currencyID="RON">{(str(taxamount))}</cbc:TaxAmount>
                    <cac:TaxSubtotal>
                        <cbc:TaxableAmount currencyID="RON">{str(round(float(str(baza)),2))}</cbc:TaxableAmount>
                        <cbc:TaxAmount currencyID="RON">{(str(taxamount2))}</cbc:TaxAmount>
                        <cac:TaxCategory>
                            <cbc:ID>{subtotalIDTVA["ID TVA"][index]}</cbc:ID>
                            <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                            <cac:TaxScheme>
                                <cbc:ID>VAT</cbc:ID>
                            </cac:TaxScheme>
                        </cac:TaxCategory>
                    </cac:TaxSubtotal>
                </cac:TaxTotal>\n'''
            
            for index, row in df_fact_curenta.iterrows():
                line_amount = row["Amount in doc ccy"]
                val_cu_tva = row["Valoare linie cu TVA"]
                
                total_tva += val_cu_tva
                total_amount += line_amount
                invoiceLine += f'''<cac:InvoiceLine>
                        <cbc:ID>{line_count}</cbc:ID>
                        <cbc:InvoicedQuantity unitCode="{row["Cod Unitate Masura"]}">{row["Quantity"]}</cbc:InvoicedQuantity>
                        <cbc:LineExtensionAmount currencyID="RON">{str(round(float(str(row["Amount in doc ccy"])),2))}</cbc:LineExtensionAmount>
                        <cac:Item>
                            <cbc:Name>{row["Description"]}</cbc:Name>
                            <cac:ClassifiedTaxCategory>
                                <cbc:ID>{row["ID TVA"]}</cbc:ID>
                                <cbc:Percent>{str(round(float(str(row["Cota"])),2))}</cbc:Percent>
                                <cac:TaxScheme>
                                    <cbc:ID>VAT</cbc:ID>
                                </cac:TaxScheme>
                            </cac:ClassifiedTaxCategory>
                        </cac:Item>
                        <cac:Price>
                            <cbc:PriceAmount currencyID="RON">{str(round(float(str(row["UnitPrice"])),2))}</cbc:PriceAmount>
                        </cac:Price>
                    </cac:InvoiceLine>'''
                    
                
                
                # Incrementați numărul elementului pentru următoarea linie din factură
                line_count += 1
            total_amount_with_vat = total_amount * (1 + row["Cota"] / 100)
            # print(row["Inv. No"], total_tva)
            # print(str(df_fact_curenta["Inv. No"].iloc[0]).replace(".0", "") ,total_amount_without_vat)
            
            PaymentMeans = f'''
            <cac:PaymentMeans>
                <cbc:PaymentMeansCode>10</cbc:PaymentMeansCode>
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
            eFacturaXML = XML_Header + AccountingSupplierParty + AccountingCustomerPartyXML + TaxTotal + LegalMonetary + invoiceLine +"\n</Invoice>"
            def remove_diacritics(input_str):
                nfkd_form = unicodedata.normalize('NFKD', input_str)
                return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

            eFacturaXML = remove_diacritics(eFacturaXML)

            # Scrie conținutul în fișierul XML
            with open(f"C:/Dezvoltare/E-Factura/2023/eFactura/Ferro/eFacturaFerro/outs/SalesInvoice_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
            # with open(f"/home/efactura/efactura_ferro/outs/SalesInvoice_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
                f.write(eFacturaXML)

            # print("A PRELUCRAT DATELE")
        return primaFactura, ultimaFactura, totalFactura, nrFacturiTrimise, facturiNuleUnice
    except:
        print("avem erori")
    if os.path.exists(fisierDeVanzari):
        os.remove(fisierDeVanzari)
    out_folder = "C:/Dezvoltare/E-Factura/2023/Baze de vanzari/outs"
    # out_folder = "/home/efactura/efactura_ferro/outs"
    for file in os.listdir(out_folder):
        if file.endswith(".xml"):
            os.remove(os.path.join(out_folder, file))        

def generare_fisier_text(mesaj, informatii):
        # Generare conținut fișier text
        text_content = f"\n{mesaj}\n"
        for info in informatii:
            text_content += f"{info}, "
        text_content += "\n"  # Adăugare linie goală între secțiuni

        # Salvare fișier text
        log_folder = "C:/Dezvoltare/E-Factura/2023/eFactura/Ferro/eFacturaFerro/logs"
        # log_folder = "/home/efactura/efactura_ferro/logs"
        log_path = os.path.join(log_folder, "informatii.txt")
        with open(log_path, "a", encoding="utf-8") as text_file:  # Utilizăm "a" pentru a adăuga la fișier
            text_file.write(text_content)
                   
prelucrareDate(fisierDeVanzari)