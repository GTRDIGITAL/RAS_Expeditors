from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from celery import Celery
import ssl
import os
from dotenv import load_dotenv
from .config import Config

load_dotenv()

# print("=== Environment Variables ===")
# print(f"CELERY_BROKER_URL: {os.getenv('CELERY_BROKER_URL')}")
# print(f"CELERY_RESULT_BACKEND: {os.getenv('CELERY_RESULT_BACKEND')}")
# print(f"REDIS_PASSWORD: {os.getenv('REDIS_PASSWORD')}")
# print("==========================")

celerey_Result_Backend = os.getenv('CELERY_RESULT_BACKEND')
celery_Broker_URL = os.getenv('CELERY_BROKER_URL')

db = SQLAlchemy()

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    ssl_opts = {'ssl_cert_reqs': ssl.CERT_NONE}
    celery.conf.broker_use_ssl = ssl_opts
    celery.conf.redis_backend_use_ssl = ssl_opts

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery
def create_app():
    app = Flask(__name__, template_folder="templates")
    app.config.from_object(Config)
    app.config['SECRET_KEY'] = 'secret'
    
    # Add these explicit Celery configs
    app.config['CELERY_BROKER_URL'] = os.getenv('CELERY_BROKER_URL')
    app.config['CELERY_RESULT_BACKEND'] = os.getenv('CELERY_RESULT_BACKEND')

    db.init_app(app)
    celery = make_celery(app)
    
    from .views import views
    from .auth import auth
    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(auth, url_prefix='/')
    app.celery = celery

    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    # Create UPLOAD_FOLDER if it doesn't exist
    upload_folder = Config.UPLOAD_FOLDER
    if not os.path.exists(upload_folder):
        os.makedirs(upload_folder)

    @login_manager.user_loader
    def load_user(id):
        from .models import Users
        return Users.query.get(int(id))

    return app