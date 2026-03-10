# main/models.py

from django.conf import settings
from django.db import models
from django.utils import timezone

User = settings.AUTH_USER_MODEL

class UserStatus(models.Model):
    """
    Tracks last_seen for each user; "online" is computed as last_seen within ONLINE_WINDOW minutes.
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="status")
    last_seen = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "auth_user_status"

    def __str__(self):
        return f"{getattr(self.user, 'username', self.user_id)} status"

class LoginActivity(models.Model):
    """
    One row per login. logout_time is filled when user logs out.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="login_activities",null=True,  # Allow this field to be NULL in the database
        blank=True)
    session_key = models.CharField(max_length=40, db_index=True, blank=True, null=True)
    login_time = models.DateTimeField(default=timezone.now, db_index=True)
    logout_time = models.DateTimeField(blank=True, null=True, db_index=True)
    ip_address = models.GenericIPAddressField(blank=True, null=True)
    user_agent = models.TextField(blank=True, null=True)
    login_failed = models.BooleanField(default=False)

    class Meta:
        db_table = "auth_login_activity"
        ordering = ("-login_time",)

    def __str__(self):
        state = "ACTIVE" if self.logout_time is None and not self.login_failed else "ENDED/FAILED"
        return f"{getattr(self.user, 'username', self.user_id)} @ {self.login_time:%Y-%m-%d %H:%M} [{state}]"

    @property
    def duration_seconds(self):
        end = self.logout_time or timezone.now()
        return int((end - self.login_time).total_seconds())


########################  Audit Log  in DB  ################################################

# main/models.py
from django.conf import settings
from django.db import models

class AuditLog(models.Model):
    ACTION_CHOICES = [
        ("VIEW", "View"),
        ("CREATE", "Create"),
        ("UPDATE", "Update"),
        ("DELETE", "Delete"),
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="audit_logs",
    )
    action = models.CharField(max_length=10, choices=ACTION_CHOICES)

    app_label = models.CharField(max_length=50)
    model_name = models.CharField(max_length=50)
    object_id = models.CharField(max_length=64, blank=True)

    path = models.CharField(max_length=255, blank=True)
    method = models.CharField(max_length=10, blank=True)

    extra = models.JSONField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "audit_log"
        ordering = ["-created_at"]

    def __str__(self):
        u = self.user.username if self.user else "anonymous"
        return f"{self.action} {self.app_label}.{self.model_name}({self.object_id}) by {u}"
