from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
import json
import os

db = SQLAlchemy()

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.json')
config = citeste_configurare(config_path)
mysql_config = config['mysql']

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'secret'
    app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}/{mysql_config['database']}"
    app.config['CELERY_BROKER_URL'] = config.get('redis_url')
    app.config['CELERY_RESULT_BACKEND'] = config.get('redis_url')

    db.init_app(app)

    from .views import views
    from .auth import auth
    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(auth, url_prefix='/')

    from .models import Users

    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(id):
        return Users.query.get(int(id))

    return app
