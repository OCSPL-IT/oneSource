import logging
from celery import shared_task
from django.conf import settings
from django.core.mail import EmailMessage
from django.db.models import Count
from django.utils import timezone
from email.utils import formataddr

from .models import PC_SampleRequest

logger = logging.getLogger("custom_logger")


def _name(obj, fallback=""):
    if not obj:
        return fallback
    return (getattr(obj, "subcategory", None) or getattr(obj, "name", None) or str(obj)).strip()


def _fmt_dt(d):
    return d.strftime("%Y-%m-%d") if d else "N/A"


def build_pc_nmp_pending_summary_body() -> str:
    pending_qs = PC_SampleRequest.objects.filter(approval_by_nmp="Pending")

    counts = (
        pending_qs.values("executive_name__subcategory")
        .annotate(cnt=Count("id"))
        .order_by("-cnt", "executive_name__subcategory")
    )

    lines = [
        "Dear User,",
        "",
        "Pending approvals per executive:",
        "",
    ]

    if counts:
        for row in counts:
            exec_name = (row.get("executive_name__subcategory") or "Unassigned").strip()
            lines.append(f"{exec_name}: {row['cnt']}")
    else:
        lines.append("No pending approvals.")

    lines += ["", "Recent pending items (top 10):"]

    recent = (
        pending_qs.select_related("customer_name", "product_name", "executive_name")
        .order_by("-inquiry_date", "-sample_dispatch_date", "-id")[:10]
    )

    if recent:
        for r in recent:
            cust = _name(r.customer_name, "Unknown Customer")
            prod = _name(r.product_name, "Unknown Product")
            lines.append(f"- {cust} | {prod} | Inquiry: {_fmt_dt(r.inquiry_date)} | Dispatch: {_fmt_dt(r.sample_dispatch_date)}")
    else:
        lines.append("- None")

    lines += ["", "Regards,", "oneSource"]
    return "\n".join(lines)


@shared_task(bind=True, max_retries=2, default_retry_delay=60)
def send_pc_nmp_pending_summary_email(self):
    try:
        recipients = getattr(settings, "PC_NMP_SUMMARY_EMAILS", [])
        if not recipients:
            logger.warning("PC_NMP_SUMMARY_EMAILS is empty. Skipping NMP summary email.")
            return {"sent": False, "reason": "no_recipients"}

        body = build_pc_nmp_pending_summary_body()
        today = timezone.localdate()
        subject = f"PC Sample Request – Pending NMP approvals summary ({today:%d-%b-%Y})"

        from_email = formataddr(("oneSource", getattr(settings, "DEFAULT_FROM_EMAIL", "workflow@ocspl.com")))

        msg = EmailMessage(
            subject=subject,
            body=body,
            from_email=from_email,
            to=recipients,
        )
        msg.send(fail_silently=False)

        logger.info("Sent PC NMP pending summary email to: %s", recipients)
        return {"sent": True, "to": recipients}

    except Exception as exc:
        logger.exception("Failed to send PC NMP pending summary email: %s", exc)
        raise self.retry(exc=exc)
