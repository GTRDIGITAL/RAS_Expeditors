from celery_app import celery

@celery.task
def run_mapping_task(gl_id):
    from website.mapping import map_general_ledger
    return map_general_ledger(gl_id)
