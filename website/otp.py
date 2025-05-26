from flask import session
import pyotp
from .mail import trimitereOTPMail

def generate_new_code():
    email = session.get('email')  # Obține adresa de email din sesiune
    if email:
        key = pyotp.random_base32()  # Generează o cheie secretă random
        totp = pyotp.TOTP(key)  # Creează un obiect TOTP (Time-based One-Time Password)
        new_code = totp.now()  # Obține un nou cod OTP
        session['cod'] = new_code  # Salvează codul OTP în sesiune
        trimitereOTPMail(new_code, email)  # Trimite OTP-ul prin email
        return new_code
    return 'Adresa de email nu este prezentă în sesiune.'
