from flask import Blueprint, render_template, redirect, request, session, flash, url_for, jsonify
from flask_login import login_user, login_required, logout_user, current_user
from . import db
from .utils import extract_name_from_email
from .otp import generate_new_code
from .mail import trimitereMail, trimitereOTPMail
from .database import get_all_users, get_user_from_db, update_user_in_db
from .decorators import admin_required
from .models import Users


views = Blueprint('views', __name__)

@views.errorhandler(403)
def forbidden_error(error):
    code = session.get('verified_code')
    cod = session.get('cod')
    
    if code == cod:
        email = session.get('email')
        return render_template("403.html", email=email), 403
    else:
        return render_template('auth.html')

@views.route('/main', methods=['GET', 'POST'])
@login_required
def main():
    email = session.get('email')
    first_name = extract_name_from_email(email)
    
    user = get_user_from_db(email)
    is_admin = user and user.role == 'admin'

    code = session.get('verified_code')
    cod = session.get('cod')
    
    if code == cod:
        return render_template('main.html', email=user.username, user_name=first_name, is_admin=is_admin)
    else:
        return render_template('auth.html')


@views.route('/verify', methods=['GET', 'POST'])
@login_required
def verify():
    email = session.get('email')
    code = None
    if request.method == 'POST':
        user_code = request.form['code']
        cod = session.get('cod')
        if user_code == cod:
            user = get_user_from_db(email)
            login_user(user)
            code = user_code
            session['verified_code'] = code
            return redirect(url_for('views.main', email=email))
        else:
            flash('Cod incorect. Încearcă din nou.')
    return render_template('verify.html')


@views.route('/fail', methods=['GET', 'POST'])
@login_required
def fail():
    email = session.get('email')
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        return render_template('fail.html')
    else:
        return render_template('auth.html')


@views.route('/view-users')
@login_required
@admin_required
def users():
    email = session.get('email')
    first_name = extract_name_from_email(email)
    
    cod = session.get('cod')
    code = session.get('verified_code')
    
    user = get_user_from_db(email)
    users_list = get_all_users()
    
    if code == cod:
        return render_template('view_users.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id)
    else:
        return render_template('auth.html')


@views.route('/edit_user/<int:user_id>')
@login_required
@admin_required
def edit_user(user_id):
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        user = get_user_from_db(user_id)
        return render_template('edit_user.html', user=user)
    else:
        return render_template('auth.html')


@views.route("/update_user/<int:user_id>", methods=["POST"])
@login_required
@admin_required
def update_user(user_id):
    cod = session.get('cod')
    code = session.get('verified_code')
    if code == cod:
        username = request.form.get("username")
        role = request.form.get("role")
        if username and role:
            update_user_in_db(user_id, username, role)
        return redirect(url_for('views.users'))
    else:
        return render_template('auth.html')


@views.route('/delete-user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    user = Users.query.get(user_id)
    if not user:
        return jsonify({"error": "Userul nu a fost gasit."}), 404

    try:
        db.session.delete(user)
        db.session.commit()
        return jsonify({"success": True, "message": "Userul a fost sters cu succes."}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Eroare stergere user: {str(e)}"}), 500


@views.route('/load_transform')
# @login_required
# @admin_required
def load_transform():
    email = session.get('email')
    first_name = extract_name_from_email(email)
    
    cod = session.get('cod')
    code = session.get('verified_code')
    
    user = get_user_from_db(email)
    users_list = get_all_users()
    
    if code == cod:
        return render_template('load_transform.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id)
    else:
        return render_template('auth.html')

@views.route('/generate_reports')
# @login_required
# @admin_required
def generate_reports():
    email = session.get('email')
    first_name = extract_name_from_email(email)
    
    cod = session.get('cod')
    code = session.get('verified_code')
    
    user = get_user_from_db(email)
    users_list = get_all_users()
    
    if code == cod:
        return render_template('vizualizare_rapoarte.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id)
    else:
        return render_template('auth.html')