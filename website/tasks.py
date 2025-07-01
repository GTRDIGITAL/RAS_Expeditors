from flask import Flask 
from celery import Celery
import ssl
import os
from dotenv import load_dotenv
import mysql.connector as mysql
from celery.exceptions import SoftTimeLimitExceeded
from .config import mysql_config

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
def import_gl_task(self, filepath):
    try:
        from .insert_GL import import_into_db
        result = import_into_db(filepath)
        # Update status în DB la final
        from .tasks import update_task_status
        update_task_status(self.request.id, 'SUCCESS', result.get('message', ''))
        return result
    except Exception as e:
        from .tasks import update_task_status
        update_task_status(self.request.id, 'ERROR', str(e))
        print(f"Task error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }


def update_task_status(task_id, status, message):
    connection = mysql.connect(**mysql_config)
    cursor = connection.cursor()
    cursor.execute(
        "UPDATE celery_tasks SET end_time=NOW(), status=%s, message=%s WHERE task_id=%s",
        (status, message, task_id)
    )
    connection.commit()
    cursor.close()
    connection.close()

@celery.task(bind=True)
def mapare_gl_task(self):
    from .tasks import update_task_status  # dacă nu e deja importat
    try:
        # Update status la start
        update_task_status(self.request.id, 'STARTED', 'Task mapare GL pornit...')
        from .procedurasql import procedura_mapare
        result = procedura_mapare()
        
        update_task_status(self.request.id, 'SUCCESS', 'Mapare GL finalizată cu succes.')
        from .procedurasql import generare_sold_clienti
        result2= generare_sold_clienti()
        return {
            'status': 'success',
            'message': 'Mapare GL finalizată cu succes.'
        }
        
    except SoftTimeLimitExceeded:
        update_task_status(self.request.id, 'REVOKED', 'Task oprit de utilizator (timeout)')
        return {
            'status': 'revoked',
            'message': 'Task oprit de utilizator (timeout)'
        }
    except Exception as e:
        update_task_status(self.request.id, 'ERROR', str(e))
        print(f"Task error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }