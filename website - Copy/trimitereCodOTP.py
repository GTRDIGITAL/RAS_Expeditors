import ssl
import datetime
import base64
import smtplib
from flask import session
from .sendMails import *

def trimitereOTPMail(code, destinatari):
    message_text = f"Hi,\n\nThe authentication code is {code}.\n\nThank you,\nGTRDigital"
    
    data = datetime.datetime.now()

    # Adăugați 2 ore
    data_modificata = data + datetime.timedelta(hours=2)

    subj = "Two Factor Authentication Code" 
    atasament=""
    cc_recipients=""
    send_email_via_graph_api(subj, destinatari,message_text ,atasament,cc_recipients)