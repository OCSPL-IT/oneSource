# # main/tasks.py
# from __future__ import annotations

# from celery import shared_task
# from django.contrib.auth import get_user_model
# from django.db import transaction

# from .models import AuditLog

# User = get_user_model()


# @shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 5})
# def create_audit_log_task(
#     self,
#     *,
#     user_id: int | None,
#     action: str,
#     app_label: str,
#     model_name: str,
#     object_id: str,
#     path: str = "",
#     method: str = "",
#     extra: dict | None = None,
# ):
#     """
#     Create AuditLog row asynchronously.
#     Keep payload primitive (ids/strings) to avoid pickling issues.
#     """
#     user = None
#     if user_id:
#         user = User.objects.filter(pk=user_id).only("id").first()

#     # Ensure it's committed first (important when called inside save())
#     with transaction.atomic():
#         AuditLog.objects.create(
#             user=user,
#             action=action,
#             app_label=app_label,
#             model_name=model_name,
#             object_id=object_id,
#             path=path or "",
#             method=method or "",
#             extra=extra,
#         )
