from celery import Celery
import os
from dotenv import load_dotenv
from flask import Flask

load_dotenv()

def create_celery(app=None):
    app = app or Flask(__name__)
    app.config.update(
        broker_url=os.getenv('CELERY_BROKER_URL'),
        result_backend=os.getenv('CELERY_RESULT_BACKEND')
    )
    celery = Celery(app.import_name, broker=app.config['broker_url'], backend=app.config['result_backend'])
    celery.conf.update(app.config)
    celery.config_from_object("website.config.Config")

    return celery

celery = create_celery()

@celery.task(bind=True)
def async_import_task(self, filepath):
    try:
        from .procedurasql import procedura_mapare
        procedura_mapare()
        return {'status': 'success'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}