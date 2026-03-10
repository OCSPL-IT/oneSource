# main/utils.py
from __future__ import annotations

from django.db import transaction

from .middleware import get_current_user, get_current_request
from .tasks import create_audit_log_task


def log_model_view(obj, *, extra=None):
    request = get_current_request()
    user = get_current_user()

    user_id = user.pk if user and getattr(user, "is_authenticated", False) else None

    payload = dict(
        user_id=user_id,
        action="VIEW",
        app_label=obj._meta.app_label,
        model_name=obj._meta.model_name,
        object_id=str(getattr(obj, "pk", "")),
        path=getattr(request, "path", "") if request else "",
        method=getattr(request, "method", "") if request else "",
        extra=extra or {},
    )

    transaction.on_commit(lambda: create_audit_log_task.delay(**payload))
