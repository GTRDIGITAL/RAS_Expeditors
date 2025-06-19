from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from celery import Celery
import ssl
import os
from dotenv import load_dotenv

load_dotenv()

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
    app.config.from_object('website.config.Config')

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

    @login_manager.user_loader
    def load_user(id):
        from .models import Users
        return Users.query.get(int(id))

    return app