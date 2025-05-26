import pandas as pd
import datetime
import xml.etree.ElementTree as ET  

df = pd.read_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/Baza de date vanzari/3370_Vanzari SAP_11.2023.xlsx", sheet_name='vanzari')



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
    
filtrareRO = df.loc[df['Customer group'] == 'CD']
Sales_EFACTURA=pd.DataFrame()

Sales_EFACTURA["Inv. No"]=filtrareRO["Billing Document"]
Sales_EFACTURA["Material"]=filtrareRO["Material"]

Sales_EFACTURA["Inv. Date"]=filtrareRO["Billing Date"]

Sales_EFACTURA["ClientNo"]=filtrareRO["sold-to party"]
Sales_EFACTURA["ClientName"]=filtrareRO["Sold-to-name"]
# Sales_EFACTURA["AccountID"]=filtrareRO["Alternative Account"]
Sales_EFACTURA["Quantity"]=filtrareRO["bill qty ZSDSABIL"]
try:
	Sales_EFACTURA["UnitPrice"]=filtrareRO["AC bill net val"]/filtrareRO["bill qty ZSDSABIL"]
except:
	Sales_EFACTURA["UnitPrice"]=filtrareRO["AC bill net val"]
Sales_EFACTURA["Description"]=filtrareRO["Description"]
Sales_EFACTURA["Amount in doc ccy"]=filtrareRO["Net value"]
Sales_EFACTURA["Amount in local ccy"]=filtrareRO["AC bill net val"]
Sales_EFACTURA["CcyCode"]=filtrareRO["Document Currency"]
Sales_EFACTURA["Unitate Masura"] = filtrareRO["Sales unit"]
listaClientSales=list(Sales_EFACTURA["ClientNo"])

listaNumarFact = list(set(list(Sales_EFACTURA["Inv. No"])))
# print(list(Sales_EFACTURA["Inv. No"]))
listaClientNameSalesrap=list(Sales_EFACTURA["ClientName"])
listaCUIRaportdepus=[]
listaSalesCountry=[]
listaLocalitateRap=[]

for i in range(0, len(listaClientSales)):
	listaClientSales[i]=str(listaClientSales[i]).replace(" ","").replace(".0","")
listaCUIRaportdepus.append("")
listaSalesCountry.append("")
listaLocalitateRap.append("")
# print(listaClientSales)

# print(Sales_EFACTURA)
# abc = Sales_EFACTURA.groupby('Inv. No').size()



# --------------------------------------------------------------CLIENTI--------------------------------------------------------------------

Clients = pd.read_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/Baza de date vanzari/Clients.xlsx")

listheader=["Name 1", "Contents 1"]

m = [set(listheader).issubset(i) for i in Clients.values.tolist()] #gasire rand pe care se afla headerul
# TB=TB.iloc[:, 2:15]
index=Clients[m].index.values #salvare index header
# new_header=TB.iloc[int(index[0])]
# Clients.columns=newheader
new_header=Clients.iloc[int(index[0])]
Clients.columns=new_header
Clients.drop(Clients.index[:int(index[0]+1)], inplace=True)
Clients.columns.values[5] = "Contents vat"
# Clients = Clients[~Clients["Name 3"] == "PO Box cty"]
Clients = Clients[~Clients["Name 3"].str.contains("PO Box cty", na=False)]
# Clients = Clients[~Clients["Name 1"] == "Country"]
Clients = Clients[~Clients["Name 1"].str.contains("Country", na=False)]
Clients2=Clients.copy()
# Clients=Clients[newheader]
# Clients.dropna(axis=1, how='all', inplace=True)
Clients_procesat=pd.DataFrame()
# Clients=Clients.dropna(subset=["Name 1"])
Clientstemp=pd.DataFrame()
Clientstempcity=pd.DataFrame()
Clientstempvat=pd.DataFrame()
ClientstempStreet = pd.DataFrame()
Clientstemp["cust#"]=Clients.loc[Clients['Name 1']=="Customer",'Contents 1']
Clientstempcity["City"]=Clients2.loc[Clients2['Name 1']=="City",'Contents 1']
ClientstempStreet["Street"]=Clients2.loc[Clients2['Name 1']=="Street",'Contents 1']
Clientstempvat["RegNo"]=Clients2.loc[Clients2['Name 2']=="VAT Reg.No",'Contents 2']
# print(Clientstempvat["RegNo"])
Clientstemp = Clientstemp.reset_index(drop = True)
Clientstemp["cust#"]=Clientstemp.dropna(subset=["cust#"])
Clientstemp["cust#"]=Clientstemp.drop_duplicates(subset=['cust#'])
Clientstemp["cust#"]=Clientstemp.dropna(subset=["cust#"])
listaClients=list(Clientstemp["cust#"])
listaCity=list(Clientstempcity["City"])
listaStreet=list(ClientstempStreet["Street"])
# print(len(listaCity), "-----------")
listaVat=list(Clientstempvat["RegNo"])
# print("ListaVat:",listaVat)
listaCityNou=[]
listaClNou=[]

# listavat=[]
for i in range(0, len(listaClients)):
	# print(str(listaClients[i]))
	if str(listaClients[i])!="nan":
		listaClNou.append(listaClients[i])

Clients_procesat["Name"]=Clients.loc[Clients['Name 1']=="Name",'Contents 1']
Clients_procesat["Country"]=Clients.loc[Clients['Name 3']=="Country",'Contents 3']
Clients_procesat = Clients_procesat.reset_index(drop = True)  
Clients_procesat["CUST#"]=listaClNou
Clients_procesat["RegNo"]=listaVat
Clients_procesat["Country"]=Clients_procesat["Country"].fillna(Clients_procesat["RegNo"].str[:2])
Clients_procesat.loc[Clients_procesat["RegNo"].astype(str)=="nan", "RegNo"]=Clients_procesat["Country"].astype(str)+"901512"
Clients_procesat["City"]=listaCity
Clients_procesat["Street"]=listaStreet
# print(Clients_procesat)
Clients_procesat.to_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/outs/client.xlsx")

SalesInv=""
listaInvoiceNo=list(set(listaClientSales))
# print(listaInvoiceNo)

dictClientName=Clients_procesat.set_index('CUST#').to_dict()['Name']
dictClientCountry=Clients_procesat.set_index('CUST#').to_dict()['Country']
dictClientCity=Clients_procesat.set_index('CUST#').to_dict()['City']
dictClientRegNo=Clients_procesat.set_index('CUST#').to_dict()['RegNo']
dictClientStreet=Clients_procesat.set_index('CUST#').to_dict()['Street']

# print(dictClientCity)
# Sales_EFACTURA["Name"] = Sales_EFACTURA["ClientName"].map(dictClientName)
Sales_EFACTURA["Country"] = Sales_EFACTURA["ClientNo"].map(dictClientCountry)
Sales_EFACTURA["City"] = Sales_EFACTURA["ClientNo"].map(dictClientCity)
Sales_EFACTURA["RegNo"] = Sales_EFACTURA["ClientNo"].map(dictClientRegNo)
Sales_EFACTURA["Street"] = Sales_EFACTURA["ClientNo"].map(dictClientStreet)
# print(Sales_EFACTURA["ClientNo"])

# print(Sales_EFACTURA)

JurnalVz=pd.read_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/Baza de date vanzari/Jurnal_Vanzari.xlsx")
# Vanzari=pd.read_excel("D:/Projects/19. Ferro Romania/2. Fisiere input/"+str(periodStart)+"/Vanzari.xlsx", sheet_name="vanzari")

listheader=["Data document",	"Numar document",	"Nume client",	"Cod de inregistrare fiscala"]
m = [set(listheader).issubset(i) for i in JurnalVz.values.tolist()] 


index=JurnalVz[m].index.values #salvare index header
new_header=JurnalVz.iloc[int(index[0])]
JurnalVz.columns=new_header
JurnalVz.drop(JurnalVz.index[:int(index[0]+1)], inplace=True)
JurnalVz=JurnalVz.dropna(subset=["Data document"])
JurnalVz=JurnalVz.dropna(subset=["Numar document"])
JurnalVz=JurnalVz.dropna(subset=[" Nr. crt."])
JurnalVz_proc=pd.DataFrame()
JurnalVz_proc["ClientName"]=JurnalVz["Nume client"]
JurnalVz_proc["RegNo"]=JurnalVz["Cod de inregistrare fiscala"]
JurnalVz_proc["City"]="Unknown"
JurnalVz_proc["Cantitate"]=1
listaHeaderTaxCode=[" Nr. crt.",	"Data document","Livrarari intracomunitare de bunuri triunghiulare scutite",	"Numar document",	"Nume client",	"Cod de inregistrare fiscala",	"Total factura (inclusiv TVA)","Livrari de bunuri intern 19% - Baza A1", "Livrari de bunuri si servicii pentru care locul livrari/prestarii este in afara Romaniei C0  ","Livrarari intracomunitare de bunuri B0","Alte livrari si prestari servicii scutite cu drept de deducere A6", "Achizitii de bunuri cu taxare inversa din AIC 9 - Baza"]
for i in JurnalVz.columns:
	if i not in listaHeaderTaxCode and str(i) !="nan":

		JurnalVz=JurnalVz.drop(str(i), axis=1)
	if str(i)=="nan":
		JurnalVz=JurnalVz[JurnalVz.columns.dropna()]


JurnalVz.loc[JurnalVz["Livrari de bunuri intern 19% - Baza A1"]!=0, "TaxCode"]=310309
try:
	JurnalVz.loc[JurnalVz["Livrarari intracomunitare de bunuri triunghiulare scutite"]!=0, "TaxCode"]=310301
except:
	pass

try:
	JurnalVz.loc[JurnalVz["Prestari de servicii intracomunitare"]!=0, "TaxCode"]=310306
except:
	pass


JurnalVz.loc[JurnalVz["Livrari de bunuri si servicii pentru care locul livrari/prestarii este in afara Romaniei C0  "]!=0, "TaxCode"]=310313
JurnalVz.loc[JurnalVz["Livrarari intracomunitare de bunuri B0"]!=0, "TaxCode"]=310301
JurnalVz.loc[JurnalVz["Alte livrari si prestari servicii scutite cu drept de deducere A6"]!=0, "TaxCode"]=310312
JurnalVz.loc[JurnalVz["Achizitii de bunuri cu taxare inversa din AIC 9 - Baza"]!=0, "TaxCode"]=310310

JurnalVz.loc[JurnalVz["Livrari de bunuri intern 19% - Baza A1"]!=0, "FinalAmount"]=JurnalVz["Livrari de bunuri intern 19% - Baza A1"]
try:
	JurnalVz.loc[JurnalVz["Livrarari intracomunitare de bunuri triunghiulare scutite"]!=0, "FinalAmount"]=JurnalVz["Livrarari intracomunitare de bunuri triunghiulare scutite"]
except:
	pass
try:
	JurnalVz.loc[JurnalVz["Prestari de servicii intracomunitare"]!=0, "FinalAmount"]=JurnalVz["Prestari de servicii intracomunitare"]
except:
	pass

JurnalVz.loc[JurnalVz["Livrari de bunuri si servicii pentru care locul livrari/prestarii este in afara Romaniei C0  "]!=0, "FinalAmount"]=JurnalVz["Livrari de bunuri si servicii pentru care locul livrari/prestarii este in afara Romaniei C0  "]
JurnalVz.loc[JurnalVz["Livrarari intracomunitare de bunuri B0"]!=0, "FinalAmount"]=JurnalVz["Livrarari intracomunitare de bunuri B0"]
JurnalVz.loc[JurnalVz["Alte livrari si prestari servicii scutite cu drept de deducere A6"]!=0, "FinalAmount"]=JurnalVz["Alte livrari si prestari servicii scutite cu drept de deducere A6"]
JurnalVz.loc[JurnalVz["Achizitii de bunuri cu taxare inversa din AIC 9 - Baza"]!=0, "FinalAmount"]=JurnalVz["Achizitii de bunuri cu taxare inversa din AIC 9 - Baza"]


JurnalVz["Cantitate"]=1
dictTaxCodeVzCotaTVA={310301:0,	310309:19.00,	310310:9.00 ,310306:0,	310312:0,	310313:0}
dictTaxCodeVzIDTVA={310301:"E",	310309:"S",	310310:"S",310306:"E",	310312:"E",	310313:"E"}
dictUnitateMasura={"KG":"KGM"}
dictTaxCodeVzDescriere={310301:"Livrări intracomunitare de bunuri, scutite",310306:"Prestări de servicii intracomunitare care beneficiază de scutire in statul membru in care taxa este datorată",	310309:"Livrări de bunuri şi prestări de servicii taxabile cu cota 19%",	310310:"Livrări de bunuri şi prestări de servicii taxabile cu cota 9%",	310312:"Livrari de bunuri si prestari de servicii supuse masurilor de simplificare (taxare inversa)",	310313:"Livrari de bunuri scutite cu drept de deducere cf Art. 294 alin (1) lit a) si b) din Codul Fiscal  (Exporturi)"}
dictTaxCodeVzTaxType={310301:300,	310309:300,	310310:300,310306:300,	310312:300,	310313:300, None:0}
JurnalVz["Cota TVA"]=JurnalVz["TaxCode"].map(dictTaxCodeVzCotaTVA)
JurnalVz["TaxType"]=JurnalVz["TaxCode"].map(dictTaxCodeVzTaxType)
JurnalVz["TaxAmount"]=JurnalVz["FinalAmount"]*JurnalVz["Cota TVA"]

#-------------aducere taxcode din jurnal in raport de sales----------------------------

listaTaxCodeJurnal=list(JurnalVz["TaxCode"])
listaFactJurnal=list(JurnalVz["Numar document"])


listaFacturaRaportVanzari=list(Sales_EFACTURA["Inv. No"])
taxcoderaportsales=[]

for i in range(0, len(listaFacturaRaportVanzari)):
	ok=0
	for j in range(0, len(listaFactJurnal)):
		if ok==0:
			# print(str(listaFacturaRaportVanzari[i]).replace(".0",""),str(listaFactJurnal[j]).replace(".0","") )
			if str(listaFacturaRaportVanzari[i]).replace(".0","")==str(listaFactJurnal[j]).replace(".0",""):
				taxcoderaportsales.append(listaTaxCodeJurnal[j])
				ok=1
	if ok==0:
		taxcoderaportsales.append(None)	


Sales_EFACTURA["TaxCode"]=taxcoderaportsales
# print(Sales_EFACTURA)
Sales_EFACTURA["Cota"] = Sales_EFACTURA["TaxCode"].map(dictTaxCodeVzCotaTVA)
Sales_EFACTURA["ID TVA"] = Sales_EFACTURA["TaxCode"].map(dictTaxCodeVzIDTVA)
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
# print(Sales_EFACTURA)
Sales_EFACTURA.to_excel("C:/Dezvoltare/E-Factura/2023/Baze de vanzari/outs/Output.xlsx", index=False)

issue_date = pd.to_datetime(Sales_EFACTURA["Inv. Date"]).dt.strftime('%Y-%m-%d').iloc[0]

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
                <cbc:CountrySubentity>RO-B</cbc:CountrySubentity>
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
    print(subtotalTva)
    # <cbc:ID>{row["ID TVA"]}</cbc:ID>
    for index, row in subtotalTva.iterrows():
        taxamount=subtotalTva["Valoare linia TVA"].sum()
        baza = subtotalBaza["Amount in local ccy"].sum()
        TaxTotal = f'''
        <cac:TaxTotal>
            <cbc:TaxAmount currencyID="RON">{str(round(float(str(taxamount)),2))}</cbc:TaxAmount>
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
                    <cbc:PriceAmount currencyID="RON">{str(round(float(str(row["Amount in doc ccy"])),2))}</cbc:PriceAmount>
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
    with open(f"C:/Dezvoltare/E-Factura/2023/Baze de vanzari/outs/SalesInvoice_{str(listaNumarFact[i]).replace('.0', '')}.xml", "w", encoding="utf-8") as f:
        f.write(eFacturaXML)
