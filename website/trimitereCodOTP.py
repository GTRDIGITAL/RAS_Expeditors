import ssl
import datetime
import base64
import smtplib
from flask import session
from .sendMails import *
import os

def trimitereOTPMail(code, destinatari):
    message_text = f"""
<!DOCTYPE html>
<html lang="ro">
<head>
  <meta charset="UTF-8" />
  <title>Cod de Autentificare</title>
</head>
<body style="margin:0; padding:0; background-color:#f8f8f8; font-family:Segoe UI, Tahoma, sans-serif; color:#111111;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="padding: 40px 0;">
    <tr>
      <td align="center">
        <!-- Conținut principal -->
        <table width="500" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff; border:1px solid #111111; border-radius:12px 12px 0 0; padding:32px; text-align:center;">
          <tr>
            <td>
              <h1 style="color:#d92127; font-size:26px; font-weight:700; margin:0 0 24px;">
                Bună ziua,
              </h1>
              <p style="font-size:18px; color:#333333; font-weight:600; margin:0 0 16px;">
                🔐 Codul dumneavoastră de autentificare este:
              </p>

              <!-- Codul -->
              <table cellpadding="12" cellspacing="0" border="0" align="center" style="margin: 20px auto; background:#ffffff; border:1px solid #d92127; border-radius:10px;">
                <tr>
                  <td align="center" style="color:#d92127; font-family:'Courier New', Courier, monospace; font-size:38px; font-weight:900; letter-spacing:8px;">
                    {code}
                  </td>
                </tr>
              </table>

              <p style="font-size:16px; color:#444444; margin:0 0 20px; font-weight:500; line-height:1.6;">
                Vă rugăm să introduceți acest cod în platformă pentru a continua autentificarea.
              </p>
            </td>
          </tr>
        </table>

        <!-- Footer negru -->
        <table width="500" cellpadding="0" cellspacing="0" border="0" style="background-color:#111111; border-radius:0 0 12px 12px; padding:24px; text-align:left;">
          <tr>
            <td style="padding:24px;">
              <div style="font-size:14px; color:#ffffff; font-style:italic;">
                <strong style="display:block; margin-bottom:6px; color:#d92127;">
                  Cu stimă,<br />
                  Echipa Grant Thornton Romania
                </strong>
                <span style="color:#cccccc;">
                  Acesta este un mesaj generat automat, vă rugăm să nu răspundeți la acest e-mail.
                </span>
              </div>
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
    
    
def trimitereFilesMail(name,destinatari, atasament):
    data = datetime.datetime.now()

    data_modificata = data + datetime.timedelta(hours=2)
    nume_fisier = os.path.basename(atasament)
    data_modificata = data_modificata.strftime("%d %B %Y, ora %H:%M")
    message_text = f"""
<!DOCTYPE html>
<html lang="ro">
  <head>
    <meta charset="UTF-8" />
    <title>Fișiere Extrase</title>
  </head>
  <body style="margin:0; padding:0; background-color:#f8f8f8; font-family:Segoe UI, Tahoma, sans-serif; color:#111111;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="padding: 40px 0;">
      <tr>
        <td align="center">
          <!-- Conținut principal -->
          <table width="500" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff; border:1px solid #111111; border-radius:12px 12px 0 0; padding:32px; text-align:left;">
            <tr>
              <td>
                <h1 style="color:#d92127; font-size:26px; font-weight:700; margin:0 0 20px; text-align:center;">
                  Fișiere Extrase
                </h1>

                <p style="font-size:16px; color:#111111; line-height:1.6; margin:0 0 24px;">
                  Bună ziua, {name}!,
                </p>

                <p style="font-size:16px; color:#333333; line-height:1.6; margin:0 0 24px;">
                  Atașat acestui email se găsește fișierul <strong style="color:#d92127;">{str(nume_fisier)}</strong>, extras din platformă la data de
                  <span style="color:#d92127; font-weight:700;">{data_modificata}</span>.
                </p>
              </td>
            </tr>
          </table>

          <!-- Footer -->
          <table width="500" cellpadding="0" cellspacing="0" border="0" style="background-color:#111111; border-radius:0 0 12px 12px; padding:24px; text-align:left;">
            <tr>
              <td style="padding:24px;">
                <div style="font-size:14px; color:#ffffff; font-style:italic;">
                  <strong style="display:block; margin-bottom:6px; color:#d92127;">
                    Cu stimă,<br />
                    Echipa Grant Thornton Romania
                  </strong>
                  <span style="color:#cccccc;">
                    Acesta este un mesaj generat automat, vă rugăm să nu răspundeți la acest e-mail.
                  </span>
                </div>
              </td>
            </tr>
          </table>

        </td>
      </tr>
    </table>
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
    print(atasament)
    send_email_via_graph_api(subj, destinatari,message_text ,atasament,cc_recipients)