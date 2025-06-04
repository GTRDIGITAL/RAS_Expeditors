from flask import Blueprint, render_template, redirect, request, session, flash, url_for, jsonify
from flask_login import login_user, login_required, logout_user, current_user
from . import db
from .utils import extract_name_from_email
from .otp import generate_new_code
from .mail import trimitereMail, trimitereOTPMail
from .database import get_all_users, get_user_from_db, update_user_in_db
from .decorators import admin_required
from .models import Users
import pandas as pd
import os
from .insert_GL import import_into_db
from celery import Celery  # Import Celery
from celery.result import AsyncResult
import logging
import redis  # Import the redis library

# filepath: c:\Dezvoltare\RAS\RAS Expeditors\website\views.py
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND")
celery = Celery('website', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)

celery.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='Europe/Bucharest',
    enable_utc=False,
    broker_pool_limit=None,
    broker_connection_timeout=30,
    broker_heartbeat=None,
    broker_use_ssl=True,
    broker_transport_options = {'ssl_cert_reqs': 'required'}, # Ensure SSL certificate verification
    redis_socket_timeout=60  # Increase Redis connection timeout
)

views = Blueprint('views', __name__)

UPLOAD_FOLDER = 'C:\\Dezvoltare\\RAS\\RAS Expeditors\\uploads'  # Define the upload folder
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery task
@celery.task(bind=True, autoretry_for=(redis.exceptions.ConnectionError, redis.exceptions.TimeoutError), retry_kwargs={'max_retries': 5}, retry_backoff=True, retry_jitter=True)
def async_import_task(self, filepath):
    """
    Asynchronous task to import data into the database and update progress.
    """
    logger.info(f"Starting async_import_task with filepath: {filepath}")
    try:
        # Read the Excel file to get the total number of rows
        df = pd.read_excel(filepath)
        total_rows = len(df)
        logger.info(f"Total rows in the file: {total_rows}")

        # Call your import_into_db function
        import_into_db(filepath)

        # Update task progress to 100% after successful import
        self.update_state(state='SUCCESS', meta={'current': total_rows, 'total': total_rows, 'progress': 100})
        logger.info("Task completed successfully!")
        return {'message': 'File imported successfully!', 'progress': 100}

    except Exception as e:
        logger.error(f"Task failed with error: {e}", exc_info=True)
        self.update_state(state='FAILURE', meta={'exc_type': type(e).__name__, 'exc_message': str(e)})
        raise

# @views.route('/mapare')
# def porneste_maparea():
#     mapareSQL_task.delay()  # ✅ pornește task-ul în fundal
#     flash("🔄 Mapare pornită în fundal!")
#     return redirect(url_for('views.home'))

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

UPLOAD_FOLDER = 'C:\\Dezvoltare\\RAS\\RAS Expeditors\\uploads'  # Define the upload folder
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@views.route('/load_transform', methods=['GET', 'POST'])
# @login_required
# @admin_required
def load_transform():
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()

    if request.method == 'POST':
        if 'file' not in request.files:
            print("No file part in the request")  # Debugging
            return jsonify({'message': 'No file part'}), 400

        file = request.files['file']

        if file.filename == '':
            print("No file selected")  # Debugging
            return jsonify({'message': 'No selected file'}), 400

        if file:
            try:
                filename = file.filename
                filepath = os.path.join(UPLOAD_FOLDER, filename)  # Use os.path.join
                file.save(filepath)

                print(f"File saved to: {filepath}")  # Debugging
                return jsonify({'message': 'File upload successful.'}), 200 # No task started here

            except Exception as e:
                print(f"Error processing file: {e}")  # Detailed error logging
                return jsonify({'message': str(e)}), 500

    if code == cod:
        return render_template('load_transform.html', email=user.username, user_name=first_name, users=users_list, user_id=user.id)
    else:
        return render_template('auth.html')


@views.route('/task_status/<task_id>')
def task_status(task_id):
    task_result = AsyncResult(task_id, app=celery)
    result = {
        'state': task_result.state,
        'result': task_result.result,
        'progress': 0
    }
    if task_result.state == 'PROGRESS' and task_result.info:
        result['progress'] = task_result.info.get('progress', 0)
    return jsonify(result)


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


@views.route('/upload_into_database', methods=['POST'])
def upload_into_database():
    print("suntem pe upload")
    email = session.get('email')
    first_name = extract_name_from_email(email)

    cod = session.get('cod')
    code = session.get('verified_code')

    user = get_user_from_db(email)
    users_list = get_all_users()

    if request.method == 'POST':
        # Get the list of files in the upload folder
        files = os.listdir(UPLOAD_FOLDER)
        print("avem fisiere aici")

        # Check if there are any files in the directory
        if not files:
            return jsonify({'message': 'No files found in upload directory'}), 400

        # Assuming you want to process the latest uploaded file
        # You might want to add more sophisticated logic to determine the correct file
        latest_file = max([os.path.join(UPLOAD_FOLDER, f) for f in files], key=os.path.getctime)

        # Add Redis connection check here
        try:
            r = redis.Redis(
                host='RedisBoxGT.redis.cache.windows.net',  # Replace with your Redis host
                port=6380,  # Use the same port as redis_health_check.py
                password=REDIS_PASSWORD,
                ssl=True,  # Enable SSL
                ssl_certfile=None,
                ssl_keyfile=None,
                ssl_cert_reqs='required'  # Verify the server's certificate
            )
            r.ping()
            print("Successfully connected to Redis!") # Print success message
            logger.info("Successfully connected to Redis!") # Log success message
        except redis.exceptions.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            return jsonify({'message': f'Failed to connect to Redis: {str(e)}'}), 500
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return jsonify({'message': f'An unexpected error occurred: {str(e)}'}), 500

        try:
            print("introducem in redis")
            task = async_import_task.delay(latest_file)
            print("a introdus in redis")
            return jsonify({'message': 'File import started in the background.', 'task_id': task.id}), 202
        except Exception as e:
            return jsonify({'message': f'Error importing file: {str(e)}'}), 500

    return jsonify({'message': 'Invalid request method'}), 400