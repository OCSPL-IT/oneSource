# main/signals.py
from django.conf import settings
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver

from .models import AuditLog
from .middleware import get_current_user, get_current_request


# Optional: restrict to only some apps
AUDIT_APPS = getattr(settings, "AUDIT_APPS", None)  # None => allow all (except excludes)

# ✅ Always ignore these apps (main + sessions + others)
AUDIT_EXCLUDE_APPS = set(
    getattr(
        settings,
        "AUDIT_EXCLUDE_APPS",
        ("main", "sessions", "receivable", "CONTRACT"),
    )
)

# ✅ Always ignore these specific model names (lowercase)
AUDIT_EXCLUDE_MODELS = set(
    getattr(
        settings,
        "AUDIT_EXCLUDE_MODELS",
        (
            "bmrissue",
            "localequipmentmaster",
            "localitemmaster",
            "localbomdetail",
            "attendance",
            'dailyattendance',
            "overtimereport",
            "dailycheckin",
            "shortleave",
            "late_early_go",
            "on_duty_request",
            "canteenheadcount",
            "attendanceregulation",
            "logentry",
            "blockitemmaster",
            "dailycheckbominput",
            "issuelinecache",
            "capexgrnline",
            "materialissueline",
            "erpbomdetail",
            "erpbomrow",
            "productionbominputline",
        ),
    )
)


def _should_audit(instance) -> bool:
    """
    Decide whether this model should be audited.
    - Never audit AuditLog itself
    - Never audit excluded app labels
    - Never audit excluded model names
    - If AUDIT_APPS is set, only allow those apps
    """
    if isinstance(instance, AuditLog):
        return False

    app = getattr(instance._meta, "app_label", None)
    model_name = getattr(instance._meta, "model_name", "").lower()

    # ❌ skip excluded apps
    if app in AUDIT_EXCLUDE_APPS:
        return False

    # ❌ skip excluded models (your list above)
    if model_name in AUDIT_EXCLUDE_MODELS:
        return False

    # If no explicit allowed-app list, audit everything else
    if AUDIT_APPS is None:
        return True

    # Otherwise, only audit if the app is whitelisted
    return app in AUDIT_APPS


def _create_log(instance, action: str):
    if not _should_audit(instance):
        return

    user = get_current_user()
    request = get_current_request()

    AuditLog.objects.create(
        user=user if user and user.is_authenticated else None,
        action=action,
        app_label=instance._meta.app_label,
        model_name=instance._meta.model_name,
        object_id=str(getattr(instance, "pk", "")),
        path=getattr(request, "path", "") if request else "",
        method=getattr(request, "method", "") if request else "",
        extra=None,
    )


@receiver(post_save)
def audit_create_update(sender, instance, created, **kwargs):
    if not _should_audit(instance):
        return
    action = "CREATE" if created else "UPDATE"
    _create_log(instance, action)


@receiver(post_delete)
def audit_delete(sender, instance, **kwargs):
    if not _should_audit(instance):
        return
    _create_log(instance, "DELETE")
