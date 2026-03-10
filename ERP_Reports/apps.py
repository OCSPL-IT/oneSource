# ERP_Reports/apps.py
from django.apps import AppConfig

class ERPReportsConfig(AppConfig):
    name = "ERP_Reports"
    verbose_name = "ERP Reports"

    _started = False  # guard against double-start on autoreload

    def ready(self):
        import os, atexit, threading
        from zoneinfo import ZoneInfo
        from urllib.parse import quote_plus

        from django.conf import settings
        from django.db.models.signals import post_save, post_delete

        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

        from ERP_Reports.models import CogsReportSchedule
        from ERP_Reports.scheduler_tasks import run_cogs_job  # top-level callable (picklable)

        # Start only once under runserver autoreloader
        if os.environ.get("RUN_MAIN") != "true" and settings.DEBUG:
            return
        if ERPReportsConfig._started:
            return
        ERPReportsConfig._started = True

        # ---- Build SQLAlchemy URL from Django DATABASES['default'] ----
        def sa_url_from_django(db_alias: str = "default") -> str:
            db = settings.DATABASES[db_alias]
            eng = db.get("ENGINE", "")
            if "mssql" not in eng:
                raise RuntimeError(f"{db_alias} is not an MSSQL database.")
            opts = db.get("OPTIONS", {})
            driver = opts.get("driver", "ODBC Driver 17 for SQL Server").replace(" ", "+")
            host = db.get("HOST") or "localhost"
            port = db.get("PORT") or "1433"
            name = db["NAME"]
            user = db.get("USER") or ""
            pwd = db.get("PASSWORD") or ""
            extra = opts.get("extra_params", "TrustServerCertificate=yes;")
            # Convert ;key=value; to &key=value for URL query
            extra_qs = "&".join(
                kv for kv in (p.strip().strip(";") for p in extra.split(";")) if kv
            )
            return (
                f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(pwd)}@{host}:{port}/{quote_plus(name)}"
                f"?driver={driver}&{extra_qs}"
            )

        # Use the default DB for the jobstore (change to 'production_scheduler' if you prefer)
        jobstore_url = sa_url_from_django("default")

        self.scheduler = BackgroundScheduler(
            timezone=settings.TIME_ZONE,
            jobstores={"default": SQLAlchemyJobStore(url=jobstore_url)},
        )

        def _add_job_for(sch: CogsReportSchedule):
            tz = ZoneInfo(sch.timezone or settings.TIME_ZONE)
            if sch.schedule_type == "DAILY":
                trigger = CronTrigger(hour=sch.hour, minute=sch.minute, timezone=tz)
            elif sch.schedule_type == "WEEKLY":
                trigger = CronTrigger(day_of_week=sch.day_of_week, hour=sch.hour, minute=sch.minute, timezone=tz)
            else:  # CRON
                trigger = CronTrigger(
                    month=sch.cron_month, day=sch.cron_day, day_of_week=sch.cron_dow,
                    hour=sch.hour, minute=sch.minute, timezone=tz
                )

            self.scheduler.add_job(
                run_cogs_job,
                trigger=trigger,
                id=f"cogs_{sch.pk}",
                replace_existing=True,
                max_instances=1,
                kwargs={"pk": sch.pk},
                misfire_grace_time=60 * 30,
                jobstore="default",
            )

        def _load_all_jobs():
            for job in list(self.scheduler.get_jobs()):
                if job.id.startswith("cogs_"):
                    self.scheduler.remove_job(job.id)
            for sch in CogsReportSchedule.objects.filter(is_enabled=True):
                _add_job_for(sch)

        # Defer initial DB load slightly to avoid init warnings
        threading.Timer(1.0, _load_all_jobs).start()

        # Keep in sync with Admin changes
        def _refresh(*args, **kwargs):
            _load_all_jobs()

        post_save.connect(_refresh, sender=CogsReportSchedule)
        post_delete.connect(_refresh, sender=CogsReportSchedule)

        self.scheduler.start()
        atexit.register(lambda: self.scheduler.shutdown(wait=False))
