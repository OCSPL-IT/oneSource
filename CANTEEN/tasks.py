from __future__ import absolute_import, unicode_literals
from celery import shared_task
from django.db import transaction
from datetime import datetime, time, timedelta
from django.core.mail import send_mail
import pytz
import os
# Assuming LOCAL_TIMEZONE is defined in your settings
from django.conf import settings
import logging

# Your models
from .models import *



logger = logging.getLogger("custom_logger")

# A function to determine the shift (assuming it's defined elsewhere, e.g., in utils.py)
def determine_shift(punch_time):
    """
    Determines the shift based on the punch time.
    """
    for shift in Shift.objects.all():
        if shift.start_time <= punch_time.time() <= shift.end_time:
            return shift
    return None


@shared_task
def sync_attendance_from_device_task():
    """
    Fetch attendance records from multiple ZKTeco devices and update the DB.
    Returns a compact summary dict (no verbose logs).
    """
    summary = {
        "totals": {
            "saved": 0,
            "duplicates_exact": 0,
            "already_punched_same_shift": 0,
            "skipped_no_shift": 0,
            "created_employees": 0,
            "devices_ok": 0,
            "devices_failed": 0,
        },
        "devices": {},   # { ip: {saved, duplicates_exact, already_punched_same_shift, skipped_no_shift, created_employees, error} }
    }

    try:
        from zk import ZK  # type: ignore
    except ImportError:
        # Whole feature unavailable
        summary["totals"]["devices_failed"] = 0
        summary["error"] = "python-zk not installed."
        return summary

    ip_addresses = ["192.168.0.30", "192.168.0.14", "192.168.0.12", "192.168.0.13", "192.168.0.24"]
    port = 4370
    LOCAL_TIMEZONE = pytz.timezone(settings.TIME_ZONE)


    for ip_address in ip_addresses:
        dev = {
            "saved": 0,
            "duplicates_exact": 0,
            "already_punched_same_shift": 0,
            "skipped_no_shift": 0,
            "created_employees": 0,
            "error": None,
        }
        summary["devices"][ip_address] = dev

        conn = None
        try:
            zk = ZK(ip_address, port=port, timeout=5, password=0, force_udp=False, ommit_ping=False)
            conn = zk.connect()

            # best-effort disable (don't fail if it errors)
            try:
                conn.disable_device()
            except Exception:
                pass

            attendances = conn.get_attendance()

            with transaction.atomic():
                for record in attendances:
                    emp_id = record.user_id
                    punch_time = record.timestamp

                    # Localize to IST if naive
                    if punch_time.tzinfo is None:
                        punch_time_ist = LOCAL_TIMEZONE.localize(punch_time)
                    else:
                        punch_time_ist = punch_time.astimezone(LOCAL_TIMEZONE)

                    punch_time_utc = punch_time_ist.astimezone(pytz.utc)

                    # Determine shift in IST
                    shift = determine_shift(punch_time_ist)
                    if not shift:
                        dev["skipped_no_shift"] += 1
                        continue

                    # Employee
                    employee = Employee.objects.filter(id=emp_id).first()
                    if not employee:
                        department = Department.objects.first() or Department.objects.create(name="Default Department")
                        employee = Employee.objects.create(
                            id=emp_id,
                            name=f"Employee {emp_id}",
                            employee_type="Company",
                            department=department,
                        )
                        dev["created_employees"] += 1

                    # Exact duplicate at same UTC timestamp
                    if Attendance.objects.filter(employee=employee, punched_at=punch_time_utc).exists():
                        dev["duplicates_exact"] += 1
                        continue

                    # Already punched this shift on same local day
                    punch_date = punch_time_ist.date()
                    start_local = datetime.combine(punch_date, time.min)
                    end_local = start_local + timedelta(days=1)
                    start_utc = LOCAL_TIMEZONE.localize(start_local).astimezone(pytz.utc)
                    end_utc = LOCAL_TIMEZONE.localize(end_local).astimezone(pytz.utc)

                    if Attendance.objects.filter(
                        employee=employee, shift=shift, punched_at__gte=start_utc, punched_at__lt=end_utc
                    ).exists():
                        dev["already_punched_same_shift"] += 1
                        continue

                    # Save (UTC)
                    Attendance.objects.create(
                        employee=employee,
                        punched_at=punch_time_utc,
                        meal_type="Meal",
                        shift=shift,
                    )
                    dev["saved"] += 1

            summary["totals"]["devices_ok"] += 1

        except Exception as e:
            dev["error"] = str(e)
            summary["totals"]["devices_failed"] += 1

        finally:
            if conn:
                # swallow cleanup errors (devices can drop TCP; library may raise)
                try:
                    conn.enable_device()
                except Exception:
                    pass
                try:
                    conn.disconnect()
                except Exception:
                    pass

        # roll dev counts into grand totals
        for k in ("saved", "duplicates_exact", "already_punched_same_shift", "skipped_no_shift", "created_employees"):
            summary["totals"][k] += dev[k]

    return summary




# --------------------------------------------------------------------------------------------


######          Canteen Head Count  Machine  #########


@shared_task
def Canteen_Head_Count_sync():
    """
    Fetch attendance records from multiple ZKTeco devices and update the DB.

    Rules:
      - No shift logic.
      - For each employee:
          * skip exact duplicate (same punched_at UTC)
          * skip if already punched once on that local day
    """
    summary = {
        "totals": {
            "saved": 0,
            "duplicates_exact": 0,
            "already_punched_today": 0,
            "created_employees": 0,
            "devices_ok": 0,
            "devices_failed": 0,
        },
        "devices": {},  # { ip: {...} }
    }

    task_id = getattr(Canteen_Head_Count_sync.request, "id", None)
    logger.info("Canteen HeadCount Sync START | task_id=%s", task_id)

    try:
        from zk import ZK  # type: ignore
    except ImportError:
        summary["error"] = "python-zk not installed."
        logger.error("Canteen HeadCount Sync FAILED | task_id=%s | reason=python-zk not installed", task_id)
        return summary

    ip_addresses = ["192.168.0.72","192.168.0.25"]
    port = 4370
    LOCAL_TIMEZONE = pytz.timezone(settings.TIME_ZONE)

    logger.info(
        "Canteen HeadCount Sync CONFIG | task_id=%s | devices=%s | tz=%s",
        task_id, ip_addresses, settings.TIME_ZONE
    )

    for ip_address in ip_addresses:
        dev = {
            "saved": 0,
            "duplicates_exact": 0,
            "already_punched_today": 0,
            "created_employees": 0,
            "error": None,
        }
        summary["devices"][ip_address] = dev

        conn = None
        attendances_count = None

        logger.info("Device Sync START | task_id=%s | ip=%s | port=%s", task_id, ip_address, port)

        try:
            zk = ZK(ip_address, port=port, timeout=5, password=0, force_udp=False, ommit_ping=False)
            conn = zk.connect()
            logger.info("Device Connected | task_id=%s | ip=%s", task_id, ip_address)

            try:
                conn.disable_device()
                logger.info("Device Disabled | task_id=%s | ip=%s", task_id, ip_address)
            except Exception as e:
                logger.warning("Disable Device Failed | task_id=%s | ip=%s | err=%s", task_id, ip_address, str(e))

            attendances = conn.get_attendance() or []
            attendances_count = len(attendances)
            logger.info(
                "Attendance Fetched | task_id=%s | ip=%s | records=%s",
                task_id, ip_address, attendances_count
            )

            with transaction.atomic():
                for record in attendances:
                    emp_id = record.user_id
                    punch_time = record.timestamp  # naive or tz-aware

                    # Localize to IST
                    if punch_time.tzinfo is None:
                        punch_time_ist = LOCAL_TIMEZONE.localize(punch_time)
                    else:
                        punch_time_ist = punch_time.astimezone(LOCAL_TIMEZONE)

                    punch_time_utc = punch_time_ist.astimezone(pytz.utc)

                    # Ensure employee exists
                    employee = Employee.objects.filter(id=emp_id).first()
                    if not employee:
                        department = (
                            Department.objects.first()
                            or Department.objects.create(name="Default Department")
                        )
                        employee = Employee.objects.create(
                            id=emp_id,
                            name=f"Employee {emp_id}",
                            employee_type="Company",
                            department=department,
                        )
                        dev["created_employees"] += 1
                        logger.info(
                            "Employee Created | task_id=%s | ip=%s | emp_id=%s",
                            task_id, ip_address, emp_id
                        )

                    # A) skip exact duplicates (same UTC timestamp)
                    if CanteenHeadCount.objects.filter(
                        employee=employee,
                        punched_at=punch_time_utc,
                    ).exists():
                        dev["duplicates_exact"] += 1
                        continue

                    # B) skip if already punched for that local date
                    punch_date = punch_time_ist.date()
                    start_local = datetime.combine(punch_date, time.min)
                    end_local = start_local + timedelta(days=1)

                    start_utc = LOCAL_TIMEZONE.localize(start_local).astimezone(pytz.utc)
                    end_utc = LOCAL_TIMEZONE.localize(end_local).astimezone(pytz.utc)

                    if CanteenHeadCount.objects.filter(
                        employee=employee,
                        punched_at__gte=start_utc,
                        punched_at__lt=end_utc,
                    ).exists():
                        dev["already_punched_today"] += 1
                        continue

                    # Save new record
                    CanteenHeadCount.objects.create(
                        employee=employee,
                        punched_at=punch_time_utc,
                    )
                    dev["saved"] += 1

            summary["totals"]["devices_ok"] += 1
            logger.info(
                "Device Sync OK | task_id=%s | ip=%s | fetched=%s | saved=%s | dup_exact=%s | already_today=%s | created_emp=%s",
                task_id, ip_address, attendances_count,
                dev["saved"], dev["duplicates_exact"], dev["already_punched_today"], dev["created_employees"]
            )

        except Exception as e:
            dev["error"] = str(e)
            summary["totals"]["devices_failed"] += 1
            logger.exception("Device Sync FAILED | task_id=%s | ip=%s | err=%s", task_id, ip_address, str(e))

        finally:
            if conn:
                try:
                    conn.enable_device()
                    logger.info("Device Enabled | task_id=%s | ip=%s", task_id, ip_address)
                except Exception as e:
                    logger.warning("Enable Device Failed | task_id=%s | ip=%s | err=%s", task_id, ip_address, str(e))

                try:
                    conn.disconnect()
                    logger.info("Device Disconnected | task_id=%s | ip=%s", task_id, ip_address)
                except Exception as e:
                    logger.warning("Disconnect Failed | task_id=%s | ip=%s | err=%s", task_id, ip_address, str(e))

        for k in ("saved", "duplicates_exact", "already_punched_today", "created_employees"):
            summary["totals"][k] += dev[k]

    logger.info(
        "Canteen HeadCount Sync END | task_id=%s | totals=%s",
        task_id, summary["totals"]
    )
    return summary





LOCAL_TZ = pytz.timezone(settings.TIME_ZONE)


def _get_local_day_range(target_date):
    """
    Given a date (naive), return (start_utc, end_utc) for that local day.
    """
    start_local = datetime.combine(target_date, dtime.min)
    end_local = start_local + timedelta(days=1)

    start_utc = LOCAL_TZ.localize(start_local).astimezone(pytz.utc)
    end_utc = LOCAL_TZ.localize(end_local).astimezone(pytz.utc)
    return start_utc, end_utc


def _meal_headcount_summary(meal_type: str, target_date=None) -> dict:
    """
    Returns counts for a given local date and meal_type:
      - total
      - company+trainee
      - casual
    """
    if target_date is None:
        target_date = timezone.localdate()

    start_utc, end_utc = _get_local_day_range(target_date)

    qs = CanteenHeadCount.objects.filter(
        punched_at__gte=start_utc,
        punched_at__lt=end_utc,
        meal_type=meal_type,
    ).select_related("employee")

    total = qs.count()
    company_trainee = qs.filter(employee__employee_type__in=["Company", "Trainee"]).count()
    casual = qs.filter(employee__employee_type="Casual").count()

    return {
        "date": target_date,
        "meal_type": meal_type,
        "total": total,
        "company_trainee": company_trainee,
        "casual": casual,
    }


@shared_task
def send_canteen_meal_summary_email(meal_type: str, target_date_iso: str | None = None):
    """
    Celery task to send canteen headcount summary email.

    :param meal_type: "Lunch" or "Dinner"
    :param target_date_iso: optional "YYYY-MM-DD" (local) string; default = today
    """
    task_id = getattr(send_canteen_meal_summary_email.request, "id", None)
    logger.info(
        "send_canteen_meal_summary_email START | task_id=%s | meal_type=%s | date=%s",
        task_id, meal_type, target_date_iso,
    )

    # ----- date -----
    if target_date_iso:
        target_date = datetime.strptime(target_date_iso, "%Y-%m-%d").date()
    else:
        target_date = timezone.localdate()

    summary = _meal_headcount_summary(meal_type, target_date)
    date_str = summary["date"].strftime("%d-%m-%Y")

    # ----- recipients from .env -----
    raw_recipients = os.getenv("CANTEEN_HEADCOUNT_RECIPIENTS", "")
    recipients = [e.strip() for e in raw_recipients.split(",") if e.strip()]
    if not recipients:
        logger.warning(
            "send_canteen_meal_summary_email: No recipients configured in CANTEEN_HEADCOUNT_RECIPIENTS"
        )
        return

    subject = f"[Canteen] {summary['meal_type']} Headcount – {date_str}"

    # Plain-text fallback (for clients that don't render HTML)
    text_body = (
        f"Canteen headcount summary for {date_str} ({summary['meal_type']}):\n\n"
        f"  • Total {summary['meal_type']} count : {summary['total']}\n"
        f"  • Company + Trainee count           : {summary['company_trainee']}\n"
        f"  • Casual count                      : {summary['casual']}\n\n"
        f"This email was generated automatically by oneSource."
    )

    # HTML body with larger & bold counts
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; font-size:14px; color:#111;">
        <p>Canteen headcount summary for <strong>{date_str}</strong> ({summary['meal_type']}):</p>
        <ul style="list-style:none; padding-left:0; margin-top:10px;">
          <li style="margin-bottom:4px;">
            • <span style="font-weight:600;">Total {summary['meal_type']} count :</span>
            <span style="font-size:18px; font-weight:700;"> {summary['total']}</span>
          </li>
          <li style="margin-bottom:4px;">
            • <span style="font-weight:600;">Company + Trainee count :</span>
            <span style="font-size:16px; font-weight:700;"> {summary['company_trainee']}</span>
          </li>
          <li style="margin-bottom:4px;">
            • <span style="font-weight:600;">Casual count :</span>
            <span style="font-size:16px; font-weight:700;"> {summary['casual']}</span>
          </li>
        </ul>
        <p style="margin-top:16px; color:#555; font-size:12px;">
          This email was generated automatically by oneSource.
        </p>
      </body>
    </html>
    """

    from_email = os.getenv("DEFAULT_FROM_EMAIL")

    try:
        send_mail(
            subject,
            text_body,
            from_email,
            recipients,
            fail_silently=False,
            html_message=html_body,  # 👈 this enables the styling
        )
        logger.info(
            "send_canteen_meal_summary_email SENT | task_id=%s | meal_type=%s | date=%s | to=%s",
            task_id, meal_type, target_date, recipients,
        )
    except Exception as e:
        logger.exception(
            "send_canteen_meal_summary_email FAILED | task_id=%s | err=%s",
            task_id, str(e),
        )


