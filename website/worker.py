import os
from . import celery  # Import the celery instance from your Flask app

if __name__ == '__main__':
    celery.worker_main(argv=['worker', '-l', 'info'])