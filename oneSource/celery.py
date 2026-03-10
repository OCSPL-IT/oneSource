import os
from celery import Celery
from django.conf import settings
from kombu import Exchange, Queue

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "oneSource.settings")

app = Celery("oneSource")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

app.autodiscover_tasks(lambda: list(settings.INSTALLED_APPS) + ["PRODUCTION.daily_block"])

# load schedule from schedule_task.py
from .schedule_task import BEAT_SCHEDULE, TIMEZONE as BEAT_TZ
app.conf.beat_schedule = BEAT_SCHEDULE
app.conf.timezone = BEAT_TZ
app.conf.enable_utc = False
app.conf.beat_max_loop_interval = 30  # reduces idle sleep

app.conf.task_default_queue = "celery"
app.conf.task_default_exchange = "celery"
app.conf.task_default_routing_key = "celery"
app.conf.task_queues = (
    Queue("celery", Exchange("celery"), routing_key="celery"),
)


# 🔹 NEW: nightly purge task (clear all waiting Celery tasks)
# @app.task(name="system.purge_all_celery_queues")
# def purge_all_celery_queues():
#     """
#     Discard all *waiting* tasks from all queues.
#     Running tasks are NOT killed.
#     """
#     deleted = app.control.purge()   # same as: `celery purge`
#     return deleted