import base64
from .sendMails import send_email_via_graph_api
import datetime

def trimitereMail():
    message_text = "Hello,\n\nConnection test.\n\nThank you,\nGTRDigital"
    date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    subj = "Test connect" + str(date)
    mailTo = "cristian.iordache@ro.gt.com"
    attachment_path = ''
    with open(attachment_path, "rb") as attachment:
        attachment_data = attachment.read()
        attachment_encoded = base64.b64encode(attachment_data).decode()
    send_email_via_graph_api(subj, mailTo, message_text, attachment_path)

def trimitereOTPMail(code, email):
    # Implementare funcție pentru trimiterea unui OTP prin email
    pass
