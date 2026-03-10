# maintenance/models.py
from django.conf import settings
from django.db import models
from django.utils import timezone

class MaintenanceSchedule(models.Model):
    STATUS_SCHEDULED = "SCHEDULED"
    STATUS_DONE = "DONE"
    STATUS_POSTPONED = "POSTPONED"

    STATUS_CHOICES = [
        (STATUS_SCHEDULED, "Scheduled"),
        (STATUS_DONE, "Done"),
        (STATUS_POSTPONED, "Postponed/Rescheduled"),
    ]

    equipment_id   = models.CharField(max_length=50, db_index=True)
    location       = models.CharField(max_length=100, blank=True)
    scheduled_date = models.DateField(db_index=True)

    # user updates
    status         = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_SCHEDULED, db_index=True)
    completed_at   = models.DateTimeField(null=True, blank=True)
    completed_by   = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True, on_delete=models.SET_NULL, related_name="maintenance_completed"
    )

    # downtime & notes
    downtime_minutes = models.PositiveIntegerField(null=True, blank=True, help_text="Total downtime in minutes (if any)")
    downtime_reason  = models.TextField(blank=True)
    notes            = models.TextField(blank=True)

    # reschedule
    rescheduled_to  = models.DateField(null=True, blank=True, help_text="New date if postponed")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "maintenance_schedule"
        unique_together = (("equipment_id", "scheduled_date"),)
        indexes = [
            models.Index(fields=["status", "scheduled_date"]),
        ]

    def __str__(self):
        return f"{self.equipment_id} on {self.scheduled_date} ({self.status})"

    @property
    def effective_date(self):
        """Used for 'due' calculations (reschedule overrides original)."""
        return self.rescheduled_to or self.scheduled_date

    @property
    def is_due_today(self):
        return self.effective_date == timezone.localdate()

    @property
    def is_due_tomorrow(self):
        return self.effective_date == (timezone.localdate() + timezone.timedelta(days=1))
