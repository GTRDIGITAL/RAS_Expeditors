from .models import Users
from . import db
import mysql.connector



def get_all_users():
    return Users.query.all()

def get_user_from_db(email_or_id):
    return Users.query.filter_by(username=email_or_id).first() if isinstance(email_or_id, str) else Users.query.get(email_or_id)

def update_user_in_db(user_id, username, role):
    user = Users.query.get(user_id)
    if user:
        user.username = username
        user.role = role
        db.session.commit()
        
