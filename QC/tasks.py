from datetime import timedelta
import os
import logging
from celery import shared_task
from django.conf import settings
from django.core.mail import send_mail
from django.utils import timezone

from .models import QCCalibrationSchedule


logger = logging.getLogger("custom_logger")

REMINDER_WINDOW_DAYS = 5  # today .. today+4  (3–4 days before + on reminder date)


def _get_recipients():
    raw = os.getenv("QC_CALIBRATION_REMINDER_TO", "")
    recipients = [e.strip() for e in raw.split(",") if e.strip()]
    logger.info(
        "[QC_CALIBRATION][_get_recipients] loaded recipients_count=%s from=env",
        len(recipients),
    )
    return recipients


@shared_task(bind=True)
def send_qc_calibration_reminders(self):
    """
    Runs once a day (12:00 PM).
    Sends email if reminder_date is between today and today+4 days.
    """
    task_id = getattr(self.request, "id", None)
    today = timezone.localdate()
    window_end = today + timedelta(days=REMINDER_WINDOW_DAYS)

    logger.info(
        "[QC_CALIBRATION][ReminderTask] START task_id=%s today=%s window_end=%s window_days=%s",
        task_id, today, window_end, REMINDER_WINDOW_DAYS,
    )

    qs = (
        QCCalibrationSchedule.objects
        .select_related("instrument")
        .filter(reminder_date__gte=today, reminder_date__lte=window_end)
    )

    total = qs.count()
    logger.info(
        "[QC_CALIBRATION][ReminderTask] schedules_in_window=%s (today=%s..%s)",
        total, today, window_end,
    )

    if total == 0:
        logger.info("[QC_CALIBRATION][ReminderTask] END task_id=%s result=no_schedules", task_id)
        return "No schedules in reminder window"

    base_recipients = _get_recipients()
    if not base_recipients:
        logger.warning(
            "[QC_CALIBRATION][ReminderTask] NO_RECIPIENTS task_id=%s setting/env QC_CALIBRATION_REMINDER_TO empty",
            task_id,
        )
        return "No QC_CALIBRATION_REMINDER_TO configured"

    sent = 0
    failed = 0

    for sch in qs:
        try:
            inst = getattr(sch.instrument, "instument_id", None) if sch.instrument else None
            recipients = list(base_recipients)

            subject = (
                f"QC Calibration Reminder: {inst} "
                f"(Due {sch.calibration_due_date:%d-%m-%Y})"
            )

            # Plain-text fallback (for clients that don't render HTML)
            text_body = (
                f"Dear Team,\n\n"
                f"This is a reminder that calibration for instrument "
                f"{inst} "
                f"is scheduled.\n\n"
                f"Calibration Date : {sch.calibration_date:%d-%m-%Y}\n"
                f"Due Date         : {sch.calibration_due_date:%d-%m-%Y}\n"
                f"Reminder Date    : {sch.reminder_date:%d-%m-%Y}\n"
                f"Remarks          : {sch.remarks or '-'}\n\n"
                f"Regards,\n"
                f"oneSource QC System"
            )

            # HTML body with highlighted Reminder Date
            html_body = f"""
            <p>Dear Team,</p>
            <p>This is a reminder that calibration for instrument
            <strong>{inst}</strong> is scheduled.</p>

            <table style="border-collapse:collapse; margin-top:8px;">
              <tr>
                <td style="padding:2px 8px 2px 0;">Calibration Date :</td>
                <td style="padding:2px 0;">{sch.calibration_date:%d-%m-%Y}</td>
              </tr>
              <tr>
                <td style="padding:2px 8px 2px 0;">Due Date :</td>
                <td style="padding:2px 0;">{sch.calibration_due_date:%d-%m-%Y}</td>
              </tr>
              <tr>
                <td style="padding:2px 8px 2px 0;">Reminder Date :</td>
                <td style="padding:2px 0;">
                  <span style="background-color:#fff3b0; font-weight:bold; padding:2px 4px; border-radius:3px;">
                    {sch.reminder_date:%d-%m-%Y}
                  </span>
                </td>
              </tr>
              <tr>
                <td style="padding:2px 8px 2px 0;">Remarks :</td>
                <td style="padding:2px 0;">{sch.remarks or '-'}</td>
              </tr>
            </table>

            <p style="margin-top:12px;">
              Regards,<br/>
              <strong>oneSource QC System</strong>
            </p>
            """

            logger.info(
                "[QC_CALIBRATION][ReminderTask] sending schedule_id=%s instrument=%s reminder_date=%s due_date=%s recipients=%s",
                sch.pk,
                inst,
                sch.reminder_date,
                sch.calibration_due_date,
                ",".join(recipients),
            )

            send_mail(
                subject,
                text_body,
                settings.DEFAULT_FROM_EMAIL,
                recipients,
                fail_silently=False,
                html_message=html_body,
            )

            sent += 1
            logger.info(
                "[QC_CALIBRATION][ReminderTask] SENT schedule_id=%s instrument=%s",
                sch.pk, inst,
            )

        except Exception:
            failed += 1
            logger.exception(
                "[QC_CALIBRATION][ReminderTask] FAILED schedule_id=%s instrument=%s task_id=%s",
                getattr(sch, "pk", None),
                getattr(getattr(sch, "instrument", None), "instument_id", None),
                task_id,
            )

    logger.info(
        "[QC_CALIBRATION][ReminderTask] END task_id=%s sent=%s failed=%s total=%s",
        task_id, sent, failed, total,
    )

    return f"Sent {sent} QC calibration reminder email(s) | failed={failed}"
