from django.utils import timezone

def run_cogs_job(pk: int):
    """
    Top-level callable for APScheduler (picklable).
    Lazy-imports to avoid circular imports at startup.
    """
    from ERP_Reports.models import CogsReportSchedule
    from ERP_Reports.jobs import send_cogs_report_job

    sch = CogsReportSchedule.objects.filter(pk=pk, is_enabled=True).first()
    if not sch:
        return
    send_cogs_report_job(sch)
    CogsReportSchedule.objects.filter(pk=pk).update(last_run_at=timezone.now())
