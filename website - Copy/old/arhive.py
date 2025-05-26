import os
import shutil
import zipfile


directory_path = 'C:\\Dezvoltare\\E-Factura\\2023\\eFactura\\Ferro\\eFacturaFerro\\output zip api'
# directory_path = "/home/efactura/efactura_ferro/outputZipAPI"

output_directory = 'C:\\Dezvoltare\\E-Factura\\2023\\eFactura\\Ferro\\eFacturaFerro\\output conversie'
def make_archive(source, destination):
    base = os.path.basename(destination)
    name = base.split('.')[0]
    format = base.split('.')[1]
    archive_from = os.path.dirname(source)
    archive_to = os.path.basename(source.strip(os.sep))
    shutil.make_archive(name, format, archive_from, archive_to)
    shutil.move('%s.%s'%(name,format), destination)   
    
                    


pdf_directory = 'C:\\Dezvoltare\\E-Factura\\2023\\eFactura\\Ferro\\eFacturaFerro\\output conversie PDF'
# pdf_directory = '/home/efactura/efactura_ferro/outputConversiePDF'
# zip_file_path = '/home/efactura/efactura_ferro/outputArhiveConversiePDF/rezultat'+str(current_datetime)+'.zip'
# zip_file_path = '/home/efactura/efactura_ferro/outputArhiveConversiePDF/rezultatArhiveConversie.zip'
zip_file_path = 'C:\\Dezvoltare\\E-Factura\\2023\\eFactura\\Ferro\\eFacturaFerro\\output arhive conversie PDF\\rezultat.zip'
make_archive(directory_path, os.path.join(pdf_directory, 'arhiveFacturiANAF.zip'))  

with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
    for pdf_file in os.listdir(pdf_directory):
        pdf_file_path = os.path.join(pdf_directory, pdf_file)
        zip_file.write(pdf_file_path, os.path.basename(pdf_file))