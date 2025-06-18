import ssl
import datetime
import base64
import smtplib
from flask import session
from .sendMails import *

def trimitereOTPMail(code, destinatari):
    message_text = f"""
<!DOCTYPE html>
<html lang="ro">
<head>
  <meta charset="UTF-8" />
  <title>Cod de Autentificare</title>
</head>
<body style="margin:0; padding:0; background:#fff; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color:#f2f2f2;">
  <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%" height="100%">
    <tr>
      <td align="center" valign="middle" style="background:#fff;">
        <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="440" style="background:#120000; border-radius: 18px; padding: 48px; box-shadow: 0 0 40px #d92127cc; border: 4px solid #d92127;">
          <tr>
            <td align="center" style="padding-bottom: 36px;">
              <h1 style="color:#d92127; font-weight: 900; font-size: 38px; margin:0; letter-spacing: 1.2px; text-transform: uppercase;">
                Bună ziua,
              </h1>
            </td>
          </tr>
          <tr>
            <td align="center" style="color:#f2f2f2; font-size: 22px; padding-bottom: 28px; letter-spacing: 0.6px; font-weight: 600;">
              <span style="font-size:32px; color:#d92127; vertical-align:middle;">&#128274;</span>
              Codul de autentificare este:
            </td>
          </tr>
          <tr>
            <td align="center" style="padding: 30px 0;">
              <table role="presentation" border="0" cellpadding="24" cellspacing="0" style="background: #d92127; border-radius: 20px; box-shadow: 0 0 35px #ff2a2acc;">
                <tr>
                  <td align="center" style="color:#100000; font-family: 'Courier New', Courier, monospace; font-size: 54px; font-weight: 900; letter-spacing: 12px; border: 5px solid #100000;">
                    {code}
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td align="center" style="color:#e8e8e8; font-size: 19px; padding-top: 14px; line-height: 1.7; max-width: 380px; font-weight: 600;">
              Vă rugăm să introduceți acest cod în platformă pentru a continua autentificarea.
            </td>
          </tr>
          <tr>
            <td align="center" style="padding-top: 60px; font-size: 15px; font-style: italic; color: #aa0000;">
              <strong style="color:#d92127; display:block; margin-bottom: 10px; font-weight: 700; font-size: 17px; letter-spacing: 1px;">
                Cu stimă,<br />
                Echipa Grant Thornton Romania
              </strong>
              <span style="color:#770000;">Acesta este un mesaj generat automat, vă rugăm să nu răspundeți la acest e-mail.</span>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


    
    data = datetime.datetime.now()

    # Adăugați 2 ore
    data_modificata = data + datetime.timedelta(hours=2)

    subj = "Two Factor Authentication Code" 
    atasament=""
    cc_recipients=""
    send_email_via_graph_api(subj, destinatari,message_text ,atasament,cc_recipients)
    
    
def trimitereFilesMail(destinatari, atasament):
    data = datetime.datetime.now()

    data_modificata = data + datetime.timedelta(hours=2)
    data_modificata = data_modificata.strftime("%d %B %Y, ora %H:%M")
    message_text = f"""
<!DOCTYPE html>
<html lang="ro">
<head>
  <meta charset="UTF-8" />
  <title>Fișiere Extrase</title>
  <style>
    body {{
      margin: 0; 
      padding: 0;
      height: 100vh;
      background: #fff;
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      color: #111;
      display: flex;
      justify-content: center;
      align-items: center;
    }}
    .container {{
      max-width: 460px;
      background: #100000;
      border-radius: 14px;
      padding: 32px;
      box-shadow: 0 0 28px #d9212766;
      border: 3px solid #d92127;
      text-align: center;
    }}
    h1 {{
      color: #d92127;
      font-weight: 800;
      font-size: 32px;
      margin: 0 0 24px 0;
      letter-spacing: 1px;
      text-transform: uppercase;
    }}
    .content {{
      color: #ddd;
      font-size: 16px;
      line-height: 1.5;
      max-width: 380px;
      font-weight: 600;
      margin: 0 auto 40px auto;
      white-space: pre-line;
    }}
    .highlight {{
      color: #d92127;
      font-weight: 700;
    }}
    .footer {{
      font-size: 14px;
      font-style: italic;
      color: #aa0000;
    }}
    .footer strong {{
      color: #d92127;
      display: block;
      margin-bottom: 8px;
      font-weight: 700;
      font-size: 15px;
      letter-spacing: 0.8px;
    }}
    .footer span {{
      color: #770000;
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Fișiere Extrase</h1>
    <p class="content">
      Atașat acestui email se găsesc fișierele extrase din platformă la data de
      <span class="highlight">{data_modificata}</span>.
      
    </p>
    <div class="footer">
      <strong>Cu stimă,<br />Echipa Grant Thornton Romania</strong>
      <span>Acesta este un mesaj generat automat, vă rugăm să nu răspundeți la acest e-mail.</span>
    </div>
  </div>
</body>
</html>
"""

    
    # data = datetime.datetime.now()

    # Adăugați 2 ore
    # data_modificata = data + datetime.timedelta(hours=2)

    subj = f"RAS Documents | "+data_modificata
    mailTo = destinatari
    atasament=atasament
    cc_recipients = ""
    send_email_via_graph_api(subj, destinatari,message_text ,atasament,cc_recipients)