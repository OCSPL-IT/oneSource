from django.conf import settings
from django.db import models

class CogsReportSchedule(models.Model):
    FREQ_CHOICES = [
        ("CRON", "CRON (advanced)"),
        ("DAILY", "Daily"),
        ("WEEKLY", "Weekly"),
    ]
    RANGE_CHOICES = [
        ("yesterday", "Yesterday"),
        ("last7", "Last 7 days"),
        ("mtd", "Month-to-date"),
        ("custom", "Custom (use from/to)"),
    ]

    name = models.CharField(max_length=120, unique=True)
    is_enabled = models.BooleanField(default=True)

    # Who receives
    to_emails = models.TextField(
        help_text="Comma separated emails, e.g. a@b.com, c@d.com"
    )
    subject = models.CharField(max_length=200, default="COGS Report")
    greet   = models.CharField(max_length=80, default="Team")

    # Filters (same as your view)
    company_id   = models.IntegerField(default=27)
    year_id      = models.IntegerField(default=7)
    cust_code    = models.IntegerField(default=0)
    item_id      = models.IntegerField(default=0)
    txn_name      = models.CharField(max_length=200, blank=True, default="")
    customer_name = models.CharField(max_length=200, blank=True, default="")
    item_name     = models.CharField(max_length=200, blank=True, default="")

    # Date range
    date_range = models.CharField(max_length=20, choices=RANGE_CHOICES, default="mtd")
    from_date  = models.DateField(null=True, blank=True)
    to_date    = models.DateField(null=True, blank=True)

    # Scheduling
    schedule_type = models.CharField(max_length=10, choices=FREQ_CHOICES, default="DAILY")
    # DAILY / WEEKLY
    hour   = models.CharField(max_length=20, default="10")   # "0-23" or "*/2"
    minute = models.CharField(max_length=20, default="30")   # "0-59" or "*/5"
    day_of_week = models.CharField(
        max_length=20, default="mon-fri",
        help_text="Only for WEEKLY (e.g., 'mon-fri' or 'sun'). Ignored for DAILY."
    )

    # CRON (advanced)
    cron_month = models.CharField(max_length=20, default="*", help_text="1-12 or *")
    cron_day   = models.CharField(max_length=20, default="*", help_text="1-31 or *")
    cron_dow   = models.CharField(max_length=20, default="*", help_text="mon-sun or *")

    timezone = models.CharField(max_length=64, default=getattr(settings, "TIME_ZONE", "UTC"))

    # Audit
    last_run_at = models.DateTimeField(null=True, blank=True, editable=False)
    created_at  = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name
