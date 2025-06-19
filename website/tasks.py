from flask import Flask 
from celery import Celery
import ssl
import os
from dotenv import load_dotenv

load_dotenv()

def create_celery():
    app = Flask(__name__)
    app.config.update(
        CELERY_BROKER_URL=os.getenv('CELERY_BROKER_URL'),
        CELERY_RESULT_BACKEND=os.getenv('CELERY_RESULT_BACKEND')  # Fix key name
    )
    
    celery = Celery(
        'tasks',
        broker=app.config['CELERY_BROKER_URL'],
        backend=app.config['CELERY_RESULT_BACKEND']
    )

    celery.conf.broker_use_ssl = {'ssl_cert_reqs': ssl.CERT_NONE}
    celery.conf.redis_backend_use_ssl = {'ssl_cert_reqs': ssl.CERT_NONE}

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