from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from os import path
from flask_login import LoginManager
import json

def citeste_configurare(file_path):
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

config = citeste_configurare('config.json')
mysql_config = config['mysql']


db = SQLAlchemy()

# DB_NAME = "efactura.db"

# creare app
def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'secret'
    # app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://userAdmin:some_pass@192.168.1.222/efacturaferro'
    app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql://{config['mysql']['user']}:{config['mysql']['password']}@{config['mysql']['host']}/{config['mysql']['database']}"
    
    
    db.init_app(app)
    
    
    
    # importam rutele de aici
    from .views import views
    from .auth import auth
    
    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(auth, url_prefix='/')
    
    from .models import Users
    
    # create_database(app)
    
    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)
    
    @login_manager.user_loader
    def load_user(id):
        return Users.query.get(int(id))
    
    return app

# def create_database(app):
#     with app.app_context():
#         if not path.exists('website/efactura'):
#             try:
#                 db.create_all()
#                 print('Created DB!')
#             except Exception as e:
#                 print(f"Error creating database: {str(e)}")