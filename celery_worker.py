from website import create_app
from celery import Celery

app = create_app()
celery = Celery(
    app.import_name,
    broker=app.config['CELERY_BROKER_URL'],
    backend=app.config['CELERY_RESULT_BACKEND']
)

celery.conf.update(app.config)

class ContextTask(celery.Task):
    def __call__(self, *args, **kwargs):
        with app.app_context():
            return self.run(*args, **kwargs)

celery.Task = ContextTask

from .website.procedurasql import procedura_mapare

@celery.task()

def mapareSQL_task():
    procedura_mapare()
