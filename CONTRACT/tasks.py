# CONTRACT/tasks.py

from celery import shared_task
from django.db import connection, transaction
from django.utils import timezone
import logging
from CONTRACT.hr_contract_etl import run_hr_contract_etl

logger = logging.getLogger('custom_logger')


def _run_update(days_back: int) -> int:
    """
    Update ONLY the last assignment row per employee per day.
    'Last' = greatest assigned_date (fallback to highest id).
    """
    sql = """
    WITH last_rows AS (
        SELECT
            a.id,
            ROW_NUMBER() OVER (
                PARTITION BY a.employee_id, a.punch_date
                ORDER BY COALESCE(a.assigned_date, '1900-01-01') DESC, a.id DESC
            ) AS rn
        FROM contract_employee_assignment AS a
        WHERE a.punch_date >= DATEADD(DAY, -%s, CAST(GETDATE() AS DATE))
    )
    UPDATE a
       SET a.punch_out = h.out_time
    FROM contract_employee_assignment AS a
    INNER JOIN last_rows AS lr
            ON lr.id = a.id
           AND lr.rn = 1                           -- only the last row for that day/employee
    INNER JOIN hr_contract AS h
            ON h.employee_id = a.employee_id
           AND h.work_date   = a.punch_date
    WHERE h.out_time IS NOT NULL
      AND (a.punch_out IS NULL OR a.punch_out <> h.out_time);
    """
    from django.db import connection, transaction
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(sql, [days_back])   # use %s placeholder
            return cur.rowcount or 0


@shared_task(name="CONTRACT.tasks.sync_punch_out_from_hr_contract_task",bind=True,max_retries=3,default_retry_delay=7,)
def sync_punch_out_from_hr_contract_task(self, days_back: int = 7) -> dict:
    """
    Celery task: copy hr_contract.out_time -> contract_employee_assignment.punch_out
    for the last `days_back` days. Idempotent (updates only when changed).
    """
    started = timezone.now()
    try:
        updated = _run_update(days_back)
        msg = f"[contract] punch_out sync OK — updated={updated}, lookback={days_back}d"
        logger.info(msg)
        return {
            "ok": True,
            "updated": updated,
            "days_back": days_back,
            "started": started.isoformat(),
            "finished": timezone.now().isoformat(),
        }
    except Exception as exc:
        logger.exception("punch_out sync failed")
        raise self.retry(exc=exc)



# This is the new Celery Task that wraps your script
@shared_task(name="CONTRACT.tasks.run_hr_contract_etl_task")
def run_hr_contract_etl_task():
    """
    Celery task to execute the HR Contract ETL process.
    """
    try:
        logger.info("Starting HR Contract ETL task...")
        run_hr_contract_etl()
        logger.info("Successfully completed HR Contract ETL task.")
    except Exception as e:
        # Log any errors that occur during the ETL process
        logger.error(f"An error occurred during the HR Contract ETL task: {e}", exc_info=True)
        # Re-raise the exception to mark the task as FAILED in Celery monitoring tools
        raise