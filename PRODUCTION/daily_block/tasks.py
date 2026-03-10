# PRODUCTION/daily_block/tasks.py

from celery import shared_task
from django.core.management import call_command
import io
from contextlib import redirect_stdout

@shared_task(bind=True)
def sync_erp_task(self):
    """
    Celery task to run the sync_erp_block management command and capture its output.
    """
    try:
        # Use an in-memory text stream to capture the standard output of the command
        f = io.StringIO()
        with redirect_stdout(f):
            call_command('sync_erp_block')
        
        output = f.getvalue()
        
        # Log the output for debugging purposes
        print(output)

        # The task is successful, return a dictionary with the status and result
        return {'status': 'SUCCESS', 'result': output}
    except Exception as e:
        # If any exception occurs during the command, the task fails
        self.update_state(state='FAILURE', meta={'exc_type': type(e).__name__, 'exc_message': str(e)})
        print(f"ERP Sync Task Failed: {e}")
        return {'status': 'FAILURE', 'result': str(e)}