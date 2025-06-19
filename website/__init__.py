from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from celery import Celery
import os

db = SQLAlchemy()
celery = Celery()

def create_app():
    app = Flask(__name__)
    
    # Load config
    app.config.from_object('config.Config')
    
    # Initialize extensions
    db.init_app(app)
    
    # Initialize Celery
    celery.conf.update(app.config['CELERY_CONFIG'])
    
    # Register blueprints
    from .views import views
    from .auth import auth
    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(auth, url_prefix='/')
    
    # Setup login manager
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)
    
    @login_manager.user_loader
    def load_user(id):
        from .models import Users
        return Users.query.get(int(id))
    
    return app