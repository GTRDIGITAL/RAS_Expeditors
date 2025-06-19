from . import celery
from .procedurasql import procedura_mapare

@celery.task(bind=True)
def async_import_task(self, filepath):
    try:
        procedura_mapare()
        return {'status': 'success'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}