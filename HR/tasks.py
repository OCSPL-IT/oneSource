import os,re
from pathlib import Path
import pandas as pd
from HR.models import *
from datetime import date, timedelta, time
from imapclient import IMAPClient
import pyzmail
import imaplib
import email
import requests
from urllib.parse import urlparse
from datetime import datetime, timedelta
from celery import shared_task
import logging
from celery.utils.log import get_task_logger
from email.utils import parsedate_to_datetime 
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(os.path.join(BASE_DIR, '.env'))

tasklog = get_task_logger(__name__)          # goes to celery_worker.log (per your LOGGING)
logger = logging.getLogger("custom_logger")
 

ATT_DIR = os.path.join(os.path.dirname(__file__), "attendance_uploads")

DOWNLOADABLE_MIME_PREFIXES = (
    "application/vnd.openxmlformats-officedocument",  # xlsx, docx, pptx
    "application/vnd.ms-excel",
    "application/octet-stream",
    "text/csv",
    "application/pdf",
)

def _imap_date(d):
    # IMAP wants: 27-Oct-2025
    return d.strftime("%d-%b-%Y")


def _filename_from_headers(resp, default_name):
    cd = resp.headers.get("Content-Disposition", "")
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, flags=re.I)
    return (m.group(1) if m else default_name).strip()


def _hrone_login(session: requests.Session, download_url: str) -> bool:
    """
    Perform a simple HROne web login in the given session.

    Uses HRONE_LOGIN_URL from .env (must be set).
    """
    login_url = os.getenv("HRONE_LOGIN_URL")
    username  = os.getenv("HRONE_LOGIN_USERNAME")
    password  = os.getenv("HRONE_LOGIN_PASSWORD")

    if not (login_url and username and password):
        tasklog.error(
            "HROne login config missing (login_url=%s, username_set=%s, password_set=%s)",
            login_url,
            bool(username),
            bool(password),
        )
        return False

    tasklog.info("Attempting HROne login at %s", login_url)

    try:
        # 1) GET login page (cookies + any hidden fields)
        r = session.get(login_url, timeout=30)
        r.raise_for_status()
        html = r.text

        # Try to capture anti-forgery token if any (optional)
        token_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            html,
            flags=re.IGNORECASE,
        )

        # ⚠️ UPDATE FIELD NAMES AFTER CHECKING IN DEVTOOLS ⚠️
        payload = {
            "Email": username,   # e.g. "email", "username", "UserName", etc.
            "Password": password,
        }
        if token_match:
            payload["__RequestVerificationToken"] = token_match.group(1)

        # 2) POST the credentials back to the same URL (or to a real auth endpoint)
        r2 = session.post(
            login_url,
            data=payload,
            timeout=30,
            allow_redirects=True,
        )

        tasklog.info(
            "HROne login POST -> status=%s final_url=%s",
            r2.status_code,
            r2.url,
        )

        if r2.status_code not in (200, 302):
            tasklog.error("HROne login failed: status=%s url=%s", r2.status_code, r2.url)
            return False

        # crude check: if we still see login form, warn
        if "sign in to continue" in r2.text.lower() or "password" in r2.text.lower():
            tasklog.warning(
                "HROne login response still looks like login page; "
                "form field names / endpoint may be wrong."
            )

        return True

    except requests.RequestException as e:
        tasklog.error("HROne login exception: %s", e)
        return False



def _download_with_optional_login(url, folder=ATT_DIR):
    """
    Try to download URL as a file.

    - First attempt anonymous GET
    - If response is HTML from an HROne domain, log in once and retry
    - Returns absolute file path on success, or None.
    """
    session = requests.Session()
    common_headers = {"User-Agent": "Mozilla/5.0"}
    session.headers.update(common_headers)

    def _try_once(allow_login: bool) -> str | None:
        try:
            with session.get(
                url,
                stream=True,
                allow_redirects=True,
                timeout=30,
            ) as r:
                final_url = r.url
                ctype = (r.headers.get("Content-Type") or "").lower()
                tasklog.info(
                    "HTTP GET %s -> %s (ctype=%s)",
                    url,
                    final_url,
                    ctype,
                )

                # If we already have a file-like content-type -> save it
                if any(ctype.startswith(p) for p in DOWNLOADABLE_MIME_PREFIXES):
                    os.makedirs(folder, exist_ok=True)
                    name_guess = os.path.basename(urlparse(final_url).path) or "download.bin"
                    fname = _filename_from_headers(r, name_guess)
                    path = os.path.join(folder, fname)

                    with open(path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    abs_path = os.path.abspath(path)
                    tasklog.info("Downloaded: %s", abs_path)
                    return abs_path

                # Not a direct file; if HROne HTML and login allowed, try login+retry
                netloc = urlparse(final_url).netloc.lower()
                if (
                    allow_login
                    and "hrone" in netloc  # hronecloud, gateway.app.hrone.cloud, etc.
                    and "text/html" in ctype
                ):
                    tasklog.warning(
                        "Got HTML from HROne for %s, attempting login then retry...",
                        final_url,
                    )
                    if _hrone_login(session):
                        # Retry once without another login attempt
                        return _try_once(allow_login=False)
                    else:
                        tasklog.error("Skipping download: HROne login failed.")
                        return None

                # Otherwise: treat as non-downloadable
                tasklog.warning(
                    "Not a direct file (content-type: %s); likely login or non-file URL. Final URL: %s",
                    ctype,
                    final_url,
                )
                return None

        except requests.RequestException as e:
            tasklog.error("Download error for %s: %s", url, e)
            return None
    return _try_once(allow_login=True)



def fetch_and_download_attachment_report():
    """
    Gmail IMAP:
    - UNSEEN mails from HROne for the last 2 days
    - Save attachments if present
    - Otherwise extract ALL http(s) links from HTML/TEXT
      and download any that look like real files (xlsx/csv/etc.)
    - Mark mail as SEEN only after at least one successful save
    """
    IMAP_URL  = os.getenv("HRO_IMAP_HOST", "imap.gmail.com")
    USERNAME  = os.getenv("HRO_IMAP_USERNAME")
    PASSWORD  = os.getenv("HRO_IMAP_PASSWORD")
    FROM_ADDR = os.getenv("HRO_IMAP_FROM", "do-not-reply@hronecloud.com")
    SUBJECT   = "Notification for report"

    today      = datetime.now().date()
    yesterday  = today - timedelta(days=1)
    tomorrow   = today + timedelta(days=1)

    with IMAPClient(IMAP_URL) as client:
        client.login(USERNAME, PASSWORD)
        client.select_folder("INBOX", readonly=False)

        criteria = [
            "UNSEEN",
            "FROM", FROM_ADDR,
            "SUBJECT", SUBJECT,
            "SINCE",  _imap_date(yesterday),
            "BEFORE", _imap_date(tomorrow),
        ]
        tasklog.info("Search criteria: %s", criteria)
        uids = client.search(criteria, charset="UTF-8")
        tasklog.info("Matched UIDs: %s", uids)
        if not uids:
            tasklog.info("No unread mails in last 2 days.")
            return

        fetch_map = client.fetch(uids, ["RFC822"])

        for uid, data in fetch_map.items():
            tasklog.info("--- Processing UID %s ---", uid)
            msg = pyzmail.PyzMessage.factory(data[b"RFC822"])
            tasklog.info("From: %s", msg.get_address("from"))
            tasklog.info("Subject: %s", msg.get_subject())

            processed_ok = False

            # A) Save attachments if present (overwrite allowed)
            for part in msg.mailparts:
                disp = (part.disposition or "").lower()
                fname = part.filename or ""
                if fname or disp == "attachment":
                    payload = part.get_payload()
                    os.makedirs(ATT_DIR, exist_ok=True)
                    if not fname:
                        fname = f"attachment_{uid}.{(part.subtype or 'bin')}"
                    path = os.path.join(ATT_DIR, fname)  # overwrite if exists
                    with open(path, "wb") as f:
                        f.write(payload)
                    tasklog.info("Saved attachment: %s", os.path.abspath(path))
                    processed_ok = True

            # B) If no attachments, try to download from links in the body
            if not processed_ok:
                html_chunks = []
                # Collect ALL text/html and text/plain parts
                for part in msg.mailparts:
                    ctype = (part.type or "").lower()
                    if ctype in ("text/html", "text/plain"):
                        payload = part.get_payload()
                        if isinstance(payload, (bytes, bytearray)):
                            charset = part.charset or "utf-8"
                            try:
                                txt = payload.decode(charset, "ignore")
                            except Exception:
                                txt = payload.decode("utf-8", "ignore")
                        else:
                            txt = str(payload)
                        html_chunks.append(txt)

                html = "\n".join([b for b in html_chunks if isinstance(b, str)])

                if not html.strip():
                    tasklog.info("No body text for UID %s", uid)
                else:
                    # All href="https://..." links
                    href_links = re.findall(
                        r'href=[\'"](https?://[^\'"]+)[\'"]',
                        html,
                        flags=re.IGNORECASE,
                    )
                    # Also catch bare URLs not in href
                    bare_links = re.findall(
                        r'(https?://[^\s<>"\']+)',
                        html,
                        flags=re.IGNORECASE,
                    )

                    # Deduplicate while preserving order
                    all_links = []
                    seen = set()
                    for u in href_links + bare_links:
                        if u not in seen:
                            seen.add(u)
                            all_links.append(u)

                    if not all_links:
                        tasklog.info("No links detected in body for UID %s", uid)
                    else:
                        tasklog.info("All candidate links for UID %s:", uid)
                        for u in all_links:
                            tasklog.info(" - %s", u)

                        # Prefer the most likely report link (hrone*, 'download', xlsx/csv)
                        def sort_key(u: str):
                            ul = u.lower()
                            return (
                                ("hrone" not in ul and "hronecloud" not in ul),
                                ("download" not in ul),
                                not any(ext in ul for ext in (".xlsx", ".xls", ".csv")),
                            )

                        all_links.sort(key=sort_key)

                        for u in all_links:
                            tasklog.info("Trying download from: %s", u)
                            saved = _download_with_optional_login(u, ATT_DIR)
                            if saved:
                                processed_ok = True
                                # you can break after first success per mail,
                                # or remove break if you want ALL downloadable links
                                break

                        if not processed_ok:
                            tasklog.warning(
                                "Links found for UID %s, but none returned a direct file "
                                "(likely login or HTML redirect required).",
                                uid,
                            )

            # C) Mark mail as SEEN only if something was actually saved
            if processed_ok:
                client.add_flags(uid, [b"\\Seen"])
                tasklog.info("Marked UID %s as SEEN.", uid)
            else:
                tasklog.info("Left UID %s UNSEEN (nothing saved).", uid)

def upload_daily_attendance():
    """
    Imports Daily_Attendance.xlsx into the DailyAttendance model.
    Fixes:
      • Preserve leading zeros in Employee Code.
      • Lookup ONLY by (employee_code, attendance_date) so later imports update same row.
      • Deduplicate existing duplicates (keeps oldest pk).
    """
    # ---- helper: normalize employee code (keep leading zeros) ----
    def _norm_emp_code(val, width: int = 5) -> str | None:
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except Exception:
            pass

        s = str(val).strip()
        if not s:
            return None

        # Excel often gives "12345.0" for numeric cells
        if s.endswith(".0"):
            core = s[:-2]
            if core.isdigit():
                s = core

        # If purely digits, zero-fill to a fixed width
        if s.isdigit():
            return s.zfill(width)

        return s  # return as-is if alphanumeric (e.g., EMP001A)

    tasklog.info("Starting daily attendance upload...")
    file_path = os.path.join(ATT_DIR, "Daily_Attendance.xlsx")

    if not os.path.exists(file_path):
        tasklog.error("File not found: %s", file_path)
        return

    try:
        # Read employee code as string to retain zeros from the source file
        df = pd.read_excel(file_path, dtype={"Employee Code": "string"})
    except Exception as e:
        tasklog.exception("Failed to read or process Excel file %s: %s", file_path, e)
        return

    cutoff_date = date.today() - timedelta(days=100)

    created_count = 0
    updated_count = 0
    skipped_count = 0
    dedup_deleted = 0

    for _, row in df.iterrows():
        try:
            emp_code = _norm_emp_code(row.get("Employee Code"))

            if not emp_code:
                tasklog.warning("Skipped row with empty Employee Code.")
                skipped_count += 1
                continue

            attendance_date_raw = pd.to_datetime(row.get("Attendance date"), errors="coerce")
            if pd.isna(attendance_date_raw):
                tasklog.warning("Skipped Emp %s: missing or invalid attendance date.", emp_code)
                skipped_count += 1
                continue

            attendance_date = attendance_date_raw.date()
            if attendance_date < cutoff_date:
                skipped_count += 1
                continue

            defaults = {
                "full_name": row.get("Full name"),
                "employment_status": row.get("Employment status"),
                "company": row.get("Company"),
                "business_unit": row.get("Business Unit"),
                "department": row.get("Department"),
                "sub_department": row.get("Sub department"),
                "designation": row.get("Designation"),
                "branch": row.get("Branch"),
                "sub_branch": row.get("Sub branch"),
                "punch_in_punch_out_time": row.get("Punch/clocking time"),
                "shift_code": row.get("Shift code"),
                "shift_timing": row.get("Shift timings"),
                "Late_or_early": row.get("Late or early"),
                "working_hours": row.get("Working hour"),
                "total_office_hours": row.get("Total office hours"),
                "source": row.get("Source"),
                "date_of_joining": (
                    pd.to_datetime(row.get("Date of joining"), errors="coerce").date()
                    if pd.notna(row.get("Date of joining"))
                    else None
                ),
                "employment_type": row.get("Employment type"),
                "grade": row.get("Grade"),
                "lattitude_longitude": row.get("Lat long"),
                "level": row.get("Level"),
                "location": row.get("Location"),
                "mobile": row.get("Mobile number"),
                "region": row.get("Region"),
                "reporting_manager": row.get("Reporting manager"),
                "work_email": row.get("Work email"),
                # IMPORTANT: status should update the SAME record (not be part of the lookup)
                "status_in_out": row.get("Status"),
            }

            # Lookup ONLY by (employee_code, attendance_date)
            qs = DailyAttendance.objects.filter(
                employee_code=emp_code,
                attendance_date=attendance_date,
            ).order_by("pk")

            if not qs.exists():
                DailyAttendance.objects.create(
                    employee_code=emp_code,
                    attendance_date=attendance_date,
                    **defaults,
                )
                created_count += 1
            else:
                obj = qs.first()
                for k, v in defaults.items():
                    setattr(obj, k, v)
                obj.save(update_fields=list(defaults.keys()))
                updated_count += 1

                # Deduplicate if there are multiple rows already
                extras = qs[1:]
                if extras:
                    dedup_deleted += extras.count()
                    extras.delete()

        except Exception as e:
            tasklog.exception("Failed to process row for Emp %s: %s", row.get("Employee Code"), e)

    tasklog.info(
        "Daily attendance import finished. Created: %d, Updated: %d, Skipped: %d, Dedup-removed: %d.",
        created_count,
        updated_count,
        skipped_count,
        dedup_deleted,
    )



def upload_attendance_regulation():
    """
    Imports AR_Request.xlsx into AttendanceRegulation.
    - Reads from HR/attendance_uploads/AR_Request.xlsx
    - Parses dd/mm/YYYY dates (dayfirst=True)
    - Skips rows older than 50 days
    - Logs progress and errors via the task logger.
    """
    file_path = os.path.join(ATT_DIR, "AR_Request.xlsx")
    tasklog.info("[AR] Starting attendance regulation upload. Path=%s", file_path)

    if not os.path.exists(file_path):
        tasklog.error("[AR] File not found: %s", file_path)
        return {"status": "missing", "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        tasklog.exception("[AR] Failed to read Excel file: %s", e)
        return {"status": "read_error", "created": 0, "updated": 0, "skipped": 0, "errors": 1}

    cutoff = date.today() - timedelta(days=50)
    created_cnt = updated_cnt = skipped_cnt = error_cnt = 0

    for _, row in df.iterrows():
        try:
            emp_raw = row.get("Employee Code")
            if emp_raw is None or (isinstance(emp_raw, float) and pd.isna(emp_raw)) or str(emp_raw).strip() == "":
                skipped_cnt += 1
                continue

            emp_code = str(emp_raw).strip()

            # dd/mm/YYYY in your admin, so parse with dayfirst=True
            att_dt = pd.to_datetime(row.get("Attendance date"), dayfirst=True, errors="coerce")
            if pd.isna(att_dt):
                skipped_cnt += 1
                continue
            attendance_date = att_dt.date()

            if attendance_date < cutoff:
                skipped_cnt += 1
                continue

            requested_on_dt = pd.to_datetime(row.get("Request on"), dayfirst=True, errors="coerce")
            approved_on_dt  = pd.to_datetime(row.get("Approved on"), dayfirst=True, errors="coerce")
            punch_in_dt     = pd.to_datetime(row.get("Punch in (date)"), dayfirst=True, errors="coerce")
            punch_out_dt    = pd.to_datetime(row.get("Punch out (date)"), dayfirst=True, errors="coerce")

            obj, created = AttendanceRegulation.objects.update_or_create(
                employee_code=emp_code,
                attendance_date=attendance_date,
                requested_on=requested_on_dt.date() if not pd.isna(requested_on_dt) else None,
                defaults={
                    "full_name": row.get("Full name"),
                    "employment_status": row.get("Employment status"),
                    "company": row.get("Company"),
                    "business_unit": row.get("Business Unit"),
                    "department": row.get("Department"),
                    "designation": row.get("Designation"),
                    "branch": row.get("Branch"),
                    "sub_branch": row.get("Sub branch"),
                    "request_type": row.get("Request type"),
                    "attendance_day": row.get("Attendance day"),
                    "reason": row.get("Reason"),
                    "shift_code": row.get("Shift code"),
                    "shift_timings": row.get("Shift timings"),
                    "actual_punch_in_out": row.get("Actual punch in/ out", ""),
                    "punch_in_date": punch_in_dt.date() if not pd.isna(punch_in_dt) else None,
                    "punch_in_time": row.get("Punch in timing", ""),
                    "punch_out_date": punch_out_dt.date() if not pd.isna(punch_out_dt) else None,
                    "punch_out_time": row.get("Punch out timing", ""),
                    "remarks": row.get("Remarks", ""),
                    "request_status": row.get("Request status"),
                    "requested_by": row.get("Request by"),
                    "approved_by": row.get("Approved by", ""),
                    "approved_on": approved_on_dt.date() if not pd.isna(approved_on_dt) else None,
                    "approver_remark": row.get("Approver remark", ""),
                },
            )

            if created:
                created_cnt += 1
            else:
                updated_cnt += 1

        except Exception as e:
            error_cnt += 1
            tasklog.exception("[AR] Failed to process row for Emp=%s: %s", row.get("Employee Code"), e)

    tasklog.info(
        "[AR] Upload complete. Created: %d, Updated: %d, Skipped: %d, Errors: %d",
        created_cnt, updated_cnt, skipped_cnt, error_cnt
    )
    return {
        "status": "ok",
        "created": created_cnt,
        "updated": updated_cnt,
        "skipped": skipped_cnt,
        "errors": error_cnt,
    }





def upload_late_early_go():
    """
    Import Late_and_Early_go.xlsx into Late_Early_Go.
    - Reads from HR/attendance_uploads/Late_and_Early_go.xlsx
    - Parses dates as dd/mm/YYYY (dayfirst=True)
    - Skips rows >50 days old or without Employee Code / valid date
    - Robust to NaN / blanks
    """
    file_path = os.path.join(ATT_DIR, "Late_and_Early_go.xlsx")
    tasklog.info("[LEG] Starting late/early go upload. Path=%s", file_path)

    if not os.path.exists(file_path):
        tasklog.error("[LEG] File not found: %s", file_path)
        return {"status": "missing", "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        tasklog.exception("[LEG] Failed to read Excel file: %s", e)
        return {"status": "read_error", "created": 0, "updated": 0, "skipped": 0, "errors": 1}

    cutoff = date.today() - timedelta(days=50)
    c = u = s = err = 0

    for _, row in df.iterrows():
        try:
            emp_raw = row.get("Employee Code")
            if emp_raw is None or (isinstance(emp_raw, float) and pd.isna(emp_raw)) or str(emp_raw).strip() == "":
                s += 1
                continue
            emp_code = str(emp_raw).strip()

            # dd/mm/YYYY like your admin resource
            dt = pd.to_datetime(row.get("Attendance date"), dayfirst=True, errors="coerce")
            if pd.isna(dt):
                s += 1
                continue
            attendance_date = dt.date()
            if attendance_date < cutoff:
                s += 1
                continue

            # numeric minutes; coerce blanks/NaN to 0
            mins = pd.to_numeric(row.get("Late/early by (min)"), errors="coerce")
            mins = int(mins) if pd.notna(mins) else 0

            obj, created = Late_Early_Go.objects.update_or_create(
                employee_code=emp_code,
                attendance_date=attendance_date,
                defaults={
                    "full_name":          row.get("Full name"),
                    "employment_status":  row.get("Employment status"),
                    "company":            row.get("Company"),
                    "business_unit":      row.get("Business Unit"),
                    "department":         row.get("Department"),
                    "designation":        row.get("Designation"),
                    "branch":             row.get("Branch"),
                    "sub_branch":         row.get("Sub branch"),
                    "late_early":         row.get("Late / early"),
                    "late_early_by_min":  mins,
                    "shift_code":         row.get("Shift code"),
                    "shift_timings":      row.get("Shift timings"),
                },
            )
            if created: c += 1
            else:       u += 1

        except Exception as e:
            err += 1
            tasklog.exception("[LEG] Failed to process row for Emp=%s: %s", row.get("Employee Code"), e)

    tasklog.info("[LEG] Upload complete. Created: %d, Updated: %d, Skipped: %d, Errors: %d", c, u, s, err)
    return {"status": "ok", "created": c, "updated": u, "skipped": s, "errors": err}



def upload_on_duty_request():
    """
    Import HR/attendance_uploads/On_Duty_request.xlsx into On_Duty_Request.

    - Keys: (employee_code, attendance_date)
    - Dates in file are dd/mm/YYYY (same as admin import)
    - 'request_on' and 'approved_on' are saved as **naive** datetimes (no timezone)
    """
    file_path = os.path.join(ATT_DIR, "On_Duty_request.xlsx")
    tasklog.info("[OD] Starting on-duty request upload. path=%s", file_path)

    if not os.path.exists(file_path):
        tasklog.error("[OD] File not found: %s", file_path)
        return {"status": "missing", "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        tasklog.exception("[OD] Failed reading Excel: %s", e)
        return {"status": "read_error", "created": 0, "updated": 0, "skipped": 0, "errors": 1}

    cutoff = date.today() - timedelta(days=50)
    created_cnt = updated_cnt = skipped_cnt = error_cnt = 0

    for _, row in df.iterrows():
        try:
            # --- Employee Code ---
            emp_raw = row.get("Employee Code")
            if emp_raw is None or (isinstance(emp_raw, float) and pd.isna(emp_raw)) or str(emp_raw).strip() == "":
                skipped_cnt += 1
                continue
            emp_code = str(emp_raw).strip()

            # --- Attendance date (dd/mm/YYYY) -> date ---
            att_dt = pd.to_datetime(row.get("Attendance date"), dayfirst=True, errors="coerce")
            if pd.isna(att_dt):
                skipped_cnt += 1
                continue
            attendance_date = att_dt.date()
            if attendance_date < cutoff:
                skipped_cnt += 1
                continue

            # --- Optional naive datetimes for request/approved_on ---
            rq_dt = pd.to_datetime(row.get("Request on"), errors="coerce")
            request_on = None if pd.isna(rq_dt) else rq_dt.to_pydatetime()

            ap_dt = pd.to_datetime(row.get("Approved on"), errors="coerce")
            approved_on = None if pd.isna(ap_dt) else ap_dt.to_pydatetime()

            # --- Optional punch in/out dates (stored as date fields in model) ---
            pin_dt  = pd.to_datetime(row.get("Punch in (date)"),  dayfirst=True, errors="coerce")
            pout_dt = pd.to_datetime(row.get("Punch out (date)"), dayfirst=True, errors="coerce")
            punch_in_date  = None if pd.isna(pin_dt)  else pin_dt.date()
            punch_out_date = None if pd.isna(pout_dt) else pout_dt.date()

            obj, created = On_Duty_Request.objects.update_or_create(
                employee_code=emp_code,
                attendance_date=attendance_date,   # matches your admin import_id_fields
                defaults={
                    "full_name":           row.get("Full name"),
                    "employment_status":   row.get("Employment status"),
                    "company":             row.get("Company"),
                    "business_unit":       row.get("Business Unit"),
                    "department":          row.get("Department"),
                    "designation":         row.get("Designation"),
                    "branch":              row.get("Branch"),
                    "sub_branch":          row.get("Sub branch"),
                    "request_type":        row.get("Request type"),
                    "attendance_day":      row.get("Attendance day"),
                    "on_duty_type":        row.get("On duty type"),
                    "shift_code":          row.get("Shift code"),
                    "shift_timings":       row.get("Shift timings"),
                    "actual_punch_in_out": row.get("Actual punch in/ out"),
                    "punch_in_date":       punch_in_date,
                    "punch_out_date":      punch_out_date,
                    "remarks":             row.get("Remarks"),
                    "request_status":      row.get("Request status"),
                    "request_by":          row.get("Request by"),
                    "request_on":          request_on,     # naive datetime
                    "pending_with":        row.get("Pending with"),
                    "approved_by":         row.get("Approved by"),
                    "approved_on":         approved_on,    # naive datetime
                    "approver_remark":     row.get("Approver remark"),
                    "billable_type":       row.get("Billable type"),
                    "punch_in_timing":     row.get("Punch in timing"),
                    "punch_out_timing":    row.get("Punch out timing"),
                },
            )
            if created: created_cnt += 1
            else:       updated_cnt += 1

        except Exception as e:
            error_cnt += 1
            tasklog.exception("[OD] Failed to process row for Emp=%s: %s", row.get("Employee Code"), e)

    tasklog.info(
        "[OD] Upload complete. Created: %d, Updated: %d, Skipped: %d, Errors: %d",
        created_cnt, updated_cnt, skipped_cnt, error_cnt
    )
    return {"status": "ok", "created": created_cnt, "updated": updated_cnt, "skipped": skipped_cnt, "errors": error_cnt}



def upload_overtime_report():
    """
    Import HR/attendance_uploads/Overtime_Request.xlsx into OvertimeReport.

    - Keys: (employee_code, attendance_date, request_on)  ← matches admin import_id_fields
    - Dates in file are dd/mm/YYYY (dayfirst=True)
    - 'request_on' and 'approved_on' are saved as **naive** datetimes (no timezone)
    - Skips rows older than 50 days or with missing key fields
    """
    file_path = os.path.join(ATT_DIR, "Overtime_Request.xlsx")
    tasklog.info("[OT] Starting overtime report upload. path=%s", file_path)

    if not os.path.exists(file_path):
        tasklog.error("[OT] File not found: %s", file_path)
        return {"status": "missing", "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        tasklog.exception("[OT] Failed to read Excel file: %s", e)
        return {"status": "read_error", "created": 0, "updated": 0, "skipped": 0, "errors": 1}

    cutoff = date.today() - timedelta(days=50)
    c = u = s = err = 0

    for _, row in df.iterrows():
        try:
            # --- Employee Code ---
            emp_raw = row.get("Employee Code")
            if emp_raw is None or (isinstance(emp_raw, float) and pd.isna(emp_raw)) or str(emp_raw).strip() == "":
                s += 1
                continue
            employee_code = str(emp_raw).strip()

            # --- Attendance date (dd/mm/YYYY) -> date ---
            att_dt = pd.to_datetime(row.get("Attendance date"), dayfirst=True, errors="coerce")
            if pd.isna(att_dt):
                s += 1
                continue
            attendance_date = att_dt.date()
            if attendance_date < cutoff:
                s += 1
                continue

            # --- request_on (datetime) — required (part of keys) ---
            req_dt = pd.to_datetime(row.get("Request on"), dayfirst=True, errors="coerce")
            if pd.isna(req_dt):
                # key missing → skip
                s += 1
                continue
            request_on = req_dt.to_pydatetime()  # naive datetime

            # --- approved_on (datetime) — optional ---
            appr_dt = pd.to_datetime(row.get("Approved on"), dayfirst=True, errors="coerce")
            approved_on = None if pd.isna(appr_dt) else appr_dt.to_pydatetime()

            # --- Normalize numerics safely ---
            overtime_minutes = pd.to_numeric(row.get("Overtime hours (in minutes)"), errors="coerce")
            overtime_minutes = int(overtime_minutes) if pd.notna(overtime_minutes) else 0

            # Optional free-text fields (leave as-is / None if missing)
            obj, created = OvertimeReport.objects.update_or_create(
                employee_code=employee_code,
                attendance_date=attendance_date,
                request_on=request_on,   # part of the unique key per your admin Meta
                defaults={
                    "full_name":         row.get("Full name"),
                    "employment_status": row.get("Employment status"),
                    "company":           row.get("Company"),
                    "business_unit":     row.get("Business Unit"),
                    "department":        row.get("Department"),
                    "designation":       row.get("Designation"),
                    "branch":            row.get("Branch"),
                    "sub_branch":        row.get("Sub branch"),
                    "attendance_day":    row.get("Attendance day"),
                    "shift_code":        row.get("Shift code"),
                    "shift":             row.get("Shift"),
                    "shift_timings":     row.get("Shift timings"),
                    "punch_in_time":     row.get("Punch in time"),
                    "punch_out_time":    row.get("Punch out time"),
                    "working_hours":     row.get("Working hours"),
                    "overtime_hours":    row.get("Overtime hours"),
                    "request_status":    row.get("Request status"),
                    "request_by":        row.get("Request by"),
                    "pending_with":      row.get("Pending with"),
                    "approved_by":       row.get("Approved by"),
                    "approved_on":       approved_on,
                    "approver_remark":   row.get("Approver remark"),
                    "overtime_minutes":  overtime_minutes,
                },
            )
            if created: c += 1
            else:       u += 1

        except Exception as e:
            err += 1
            tasklog.exception("[OT] Failed to process row for Emp=%s: %s", row.get("Employee Code"), e)

    tasklog.info("[OT] Upload complete. Created: %d, Updated: %d, Skipped: %d, Errors: %d", c, u, s, err)
    return {"status": "ok", "created": c, "updated": u, "skipped": s, "errors": err}



def upload_short_leave():
    """
    Import HR/attendance_uploads/SH_Request.xlsx into ShortLeave.

    Upsert key: (employee_code, attendance_date)
    - Preserve/left-pad employee_code to width 5 (e.g., '5' -> '00005')
    - Parse dates as dd/mm/YYYY
    - Skip rows older than 50 days or missing keys
    """
    file_path = os.path.join(ATT_DIR, "SH_Request.xlsx")
    tasklog.info("[SL] Starting short leave upload. path=%s", file_path)

    if not os.path.exists(file_path):
        tasklog.error("[SL] File not found: %s", file_path)
        return {"status": "missing", "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    # Normalize Employee Code AT READ TIME so zeros never get lost
    def _conv_emp(v):
        s = "" if v is None else str(v).strip()
        if s in ("", "nan", "NaN", "None", "<NA>"):
            return ""
        # kill Excel tails like '529.0'
        s = re.sub(r"\.0+$", "", s)
        # if pure digits, left-pad to 5
        if s.isdigit():
            s = s.zfill(5)
        return s

    try:
        df = pd.read_excel(
            file_path,
            converters={"Employee Code": _conv_emp},  # <-- critical
        )
    except Exception as e:
        tasklog.exception("[SL] Failed to read Excel file: %s", e)
        return {"status": "read_error", "created": 0, "updated": 0, "skipped": 0, "errors": 1}

    cutoff = date.today() - timedelta(days=50)
    created_cnt = updated_cnt = skipped_cnt = error_cnt = 0

    for _, row in df.iterrows():
        try:
            # --- normalized, padded (e.g., '00005') from converters ---
            employee_code = (row.get("Employee Code") or "").strip()
            if not employee_code:
                skipped_cnt += 1
                continue

            # --- Attendance date (required; dd/mm/YYYY) ---
            att_dt = pd.to_datetime(row.get("Attendance date"), dayfirst=True, errors="coerce")
            if pd.isna(att_dt):
                skipped_cnt += 1
                continue
            attendance_date = att_dt.date()
            if attendance_date < cutoff:
                skipped_cnt += 1
                continue

            # Optional date fields (saved as dates)
            pin_dt  = pd.to_datetime(row.get("Punch in (date)"),  dayfirst=True, errors="coerce")
            pout_dt = pd.to_datetime(row.get("Punch out (date)"), dayfirst=True, errors="coerce")
            req_dt  = pd.to_datetime(row.get("Request on"),       dayfirst=True, errors="coerce")
            apr_dt  = pd.to_datetime(row.get("Approved on"),      dayfirst=True, errors="coerce")

            punch_in_date   = None if pd.isna(pin_dt)  else pin_dt.date()
            punch_out_date  = None if pd.isna(pout_dt) else pout_dt.date()
            request_on      = None if pd.isna(req_dt)  else req_dt.date()
            approved_on     = None if pd.isna(apr_dt) else apr_dt.date()

            obj, created = ShortLeave.objects.update_or_create(
                employee_code=employee_code,              # <-- stays '00005'
                attendance_date=attendance_date,
                defaults={
                    "full_name":           row.get("Full name"),
                    "employment_status":   row.get("Employment status"),
                    "company":             row.get("Company"),
                    "business_unit":       row.get("Business Unit"),
                    "department":          row.get("Department"),
                    "designation":         row.get("Designation"),
                    "branch":              row.get("Branch"),
                    "sub_branch":          row.get("Sub branch"),
                    "request_type":        row.get("Request type"),
                    "attendance_day":      row.get("Attendance day"),
                    "shift_code":          row.get("Shift code"),
                    "shift_timings":       row.get("Shift timings"),
                    "actual_punch_in_out": row.get("Actual punch in/ out"),
                    "punch_in_date":       punch_in_date,
                    "punch_in_timing":     row.get("Punch in timing"),
                    "punch_out_date":      punch_out_date,
                    "punch_out_timing":    row.get("Punch out timing"),
                    "remarks":             row.get("Remarks"),
                    "request_status":      row.get("Request status"),
                    "request_by":          row.get("Request by"),
                    "request_on":          request_on,
                    "pending_with":        row.get("Pending with"),
                    "approved_by":         row.get("Approved by"),
                    "approved_on":         approved_on,
                    "approver_remark":     row.get("Approver remark"),
                },
            )
            if created: created_cnt += 1
            else:       updated_cnt += 1

        except Exception as e:
            error_cnt += 1
            tasklog.exception("[SL] Failed to process row for Emp(raw)=%r: %s", row.get("Employee Code"), e)

    tasklog.info(
        "[SL] Upload complete. Created: %d, Updated: %d, Skipped: %d, Errors: %d",
        created_cnt, updated_cnt, skipped_cnt, error_cnt
    )
    return {"status": "ok", "created": created_cnt, "updated": updated_cnt, "skipped": skipped_cnt, "errors": error_cnt}




def upload_daily_checkin():
    """
    Import HR/attendance_uploads/TimeOffice_Daily_checkIn_report.xlsx into DailyCheckIn.

    Mirrors admin mapping:
      Employee Code -> employee_code (key)
      Attendance date -> attendance_date (key)   [parsed with errors='coerce']
      Full name, Employment status, Company, Business Unit, Department, Designation,
      Branch, Sub branch, Shift, Check in, Punches with source, First Punch,
      Last Punch, Raw Punch
    Skips rows older than 50 days or with missing/invalid keys.
    """
    file_path = os.path.join(ATT_DIR, "TimeOffice_Daily_checkIn_report.xlsx")
    tasklog.info("[DCI] Starting daily check-in upload. path=%s", file_path)

    if not os.path.exists(file_path):
        tasklog.error("[DCI] File not found: %s", file_path)
        return {"status": "missing", "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        tasklog.exception("[DCI] Failed to read Excel file: %s", e)
        return {"status": "read_error", "created": 0, "updated": 0, "skipped": 0, "errors": 1}

    cutoff = date.today() - timedelta(days=50)
    c = u = s = err = 0

    for _, row in df.iterrows():
        try:
            # --- Employee Code ---
            emp_raw = row.get("Employee Code")
            if emp_raw is None or (isinstance(emp_raw, float) and pd.isna(emp_raw)) or str(emp_raw).strip() == "":
                s += 1
                continue
            employee_code = str(emp_raw).strip()

            # --- Attendance date -> date ---
            att_dt = pd.to_datetime(row.get("Attendance date"), errors="coerce")
            if pd.isna(att_dt):
                s += 1
                continue
            attendance_date = att_dt.date()
            if attendance_date < cutoff:
                s += 1
                continue

            # --- Optional fields / parsing ---
            # First/Last Punch may be datetime/time strings → coerce, then take .time()
            fp_dt = pd.to_datetime(row.get("First Punch"), errors="coerce")
            lp_dt = pd.to_datetime(row.get("Last Punch"),  errors="coerce")
            first_punch = None if pd.isna(fp_dt) else fp_dt.to_pydatetime().time()
            last_punch  = None if pd.isna(lp_dt) else lp_dt.to_pydatetime().time()

            obj, created = DailyCheckIn.objects.update_or_create(
                employee_code=employee_code,
                attendance_date=attendance_date,
                defaults={
                    "full_name":         row.get("Full name"),
                    "employment_status": row.get("Employment status"),
                    "company":           row.get("Company"),
                    "business_unit":     row.get("Business Unit"),
                    "department":        row.get("Department"),
                    "designation":       row.get("Designation"),
                    "branch":            row.get("Branch"),
                    "sub_branch":        row.get("Sub branch"),
                    "shift":             row.get("Shift"),
                    "check_in":          row.get("Check in"),
                    "source":            row.get("Punches with source"),
                    "first_punch":       first_punch,
                    "last_punch":        last_punch,
                    "raw_punch":         row.get("Raw Punch"),
                },
            )
            if created: c += 1
            else:       u += 1

        except Exception as e:
            err += 1
            tasklog.exception("[DCI] Failed to process row for Emp=%s: %s", row.get("Employee Code"), e)

    tasklog.info("[DCI] Upload complete. Created: %d, Updated: %d, Skipped: %d, Errors: %d", c, u, s, err)
    return {"status": "ok", "created": c, "updated": u, "skipped": s, "errors": err}



def upload_helpdesk_tickets():
    """
    Import helpdesk tickets from Excel into Helpdesk_Ticket.
    - Upsert key: ticket_id
    - Datetimes parsed naively (no timezone)
    - No extra business rules added
    """
    # Change filename if your export is named differently
    file_path = os.path.join(ATT_DIR, "Ticket_Details.xlsx")
    tasklog.info("[HD] Starting helpdesk tickets upload. path=%s", file_path)

    if not os.path.exists(file_path):
        tasklog.error("[HD] File not found: %s", file_path)
        return {"status": "missing", "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        tasklog.exception("[HD] Failed to read Excel file: %s", e)
        return {"status": "read_error", "created": 0, "updated": 0, "skipped": 0, "errors": 1}

    c = u = s = err = 0

    for _, row in df.iterrows():
        try:
            # Require ticket_id to upsert
            ticket_id_raw = row.get("ticket_id") or row.get("Ticket ID") or row.get("Ticket Id")
            if ticket_id_raw is None or (isinstance(ticket_id_raw, float) and pd.isna(ticket_id_raw)) or str(ticket_id_raw).strip() == "":
                s += 1
                continue
            ticket_id = str(ticket_id_raw).strip()

            # Datetimes (stored as naive)
            rz_dt = pd.to_datetime(row.get("raised_on") or row.get("Raised on"), errors="coerce")
            raised_on = None if pd.isna(rz_dt) else rz_dt.to_pydatetime()

            cl_dt = pd.to_datetime(row.get("closed_on") or row.get("Closed on"), errors="coerce")
            closed_on = None if pd.isna(cl_dt) else cl_dt.to_pydatetime()

            obj, created = Helpdesk_Ticket.objects.update_or_create(
                ticket_id=ticket_id,
                defaults={
                    "employee_code":        (row.get("employee_code")        or row.get("Employee Code")),
                    "full_name":            (row.get("full_name")            or row.get("Full name")),
                    "employment_status":    (row.get("employment_status")    or row.get("Employment status")),
                    "company":              (row.get("company")              or row.get("Company")),
                    "business_unit":        (row.get("business_unit")        or row.get("Business Unit")),
                    "department":           (row.get("department")           or row.get("Department")),
                    "designation":          (row.get("designation")          or row.get("Designation")),
                    "branch":               (row.get("branch")               or row.get("Branch")),
                    "sub_branch":           (row.get("sub_branch")           or row.get("Sub branch")),
                    "ticket_details":       (row.get("ticket_details")       or row.get("Ticket details") or row.get("Details")),
                    "category":             (row.get("category")             or row.get("Category")),
                    "sub_category":         (row.get("sub_category")         or row.get("Sub category")),
                    "priority":             (row.get("priority")             or row.get("Priority")),
                    "status":               (row.get("status")               or row.get("Status")),
                    "raised_on":             raised_on,
                    "assigned_to":          (row.get("assigned_to")          or row.get("Assigned to")),
                    "pending_with":         (row.get("pending_with")         or row.get("Pending with")),
                    "closed_by":            (row.get("closed_by")            or row.get("Closed by")),
                    "closed_on":             closed_on,
                    "is_closed_on_time":    (row.get("is_closed_on_time")    or row.get("Is closed on time")),
                    "feedback_rating":      (row.get("feedback_rating")      or row.get("Feedback rating")),
                    "was_ticket_escalated": (row.get("was_ticket_escalated") or row.get("Was ticket escalated")),
                    "escalated_to":         (row.get("escalated_to")         or row.get("Escalated to")),
                    "rca":                  (row.get("rca")                  or row.get("RCA")),
                    "time_to_close":        (row.get("time_to_close")        or row.get("Time to close")),
                },
            )
            if created: c += 1
            else:       u += 1

        except Exception as e:
            err += 1
            tasklog.exception("[HD] Failed to process row for TicketID=%s: %s", row.get("ticket_id") or row.get("Ticket ID"), e)

    tasklog.info("[HD] Upload complete. Created: %d, Updated: %d, Skipped: %d, Errors: %d", c, u, s, err)
    return {"status": "ok", "created": c, "updated": u, "skipped": s, "errors": err}



# ==============================================================================================


@shared_task(bind=True, name="HR.tasks.fetch_and_download_attachment_report_task")
def fetch_and_download_attachment_report_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        saved_count = fetch_and_download_attachment_report()  # your existing function
        tasklog.info("DONE  %s id=%s saved=%s", self.name, self.request.id, saved_count)
        return saved_count
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise
    
    
@shared_task(bind=True, name="HR.tasks.upload_daily_attendance_task")
def upload_daily_attendance_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        rows = upload_daily_attendance()  # your existing function
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
        return rows
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise
    

@shared_task(bind=True, name="HR.tasks.upload_attendance_regulation_task")
def upload_attendance_regulation_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        upload_attendance_regulation()
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise


@shared_task(bind=True, name="HR.tasks.upload_late_early_go_task")
def upload_late_early_go_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        upload_late_early_go()
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise


@shared_task(bind=True, name="HR.tasks.upload_on_duty_request_task")
def upload_on_duty_request_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        upload_on_duty_request()
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise


@shared_task(bind=True, name="HR.tasks.upload_overtime_report_task")
def upload_overtime_report_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        upload_overtime_report()
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise


@shared_task(bind=True, name="HR.tasks.upload_short_leave_task")
def upload_short_leave_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        upload_short_leave()
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise



@shared_task(bind=True, name="HR.tasks.upload_daily_checkin_task")
def upload_daily_checkin_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        upload_daily_checkin()
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise


@shared_task(bind=True, name="HR.tasks.upload_helpdesk_tickets_task")
def upload_helpdesk_tickets_task(self):
    tasklog.info("START %s id=%s", self.name, self.request.id)
    try:
        upload_helpdesk_tickets()
        tasklog.info("DONE  %s id=%s", self.name, self.request.id)
    except Exception:
        tasklog.exception("FAIL  %s id=%s", self.name, self.request.id)
        raise



@shared_task(name="HR.tasks.debug_celery_ping")
def debug_celery_ping(x, y):
    print("DEBUG_CELERY_PING", x, y)
    return x + y



  
    
# ====================================================================================================


MANUAL_ATT_DIR = Path(__file__).resolve().parent / "manual_upload"


def _norm_emp_code(val, width: int = 5) -> str | None:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    s = str(val).strip()
    if not s:
        return None

    if s.endswith(".0"):
        core = s[:-2]
        if core.isdigit():
            s = core

    if s.isdigit():
        return s.zfill(width)
    return s


def _process_daily_attendance_excel(file_path: str) -> None:
    """
    Core logic that reads the given Excel file and imports/updates
    DailyAttendance rows.
    """
    tasklog.info("Starting daily attendance upload from %s ...", file_path)

    if not os.path.exists(file_path):
        tasklog.error("File not found: %s", file_path)
        return

    try:
        df = pd.read_excel(file_path, dtype={"Employee Code": "string"})
    except Exception as e:
        tasklog.exception("Failed to read or process Excel file %s: %s", file_path, e)
        return

    cutoff_date = date.today() - timedelta(days=100)

    created_count = 0
    updated_count = 0
    skipped_count = 0
    dedup_deleted = 0

    for _, row in df.iterrows():
        try:
            emp_code = _norm_emp_code(row.get("Employee Code"))

            if not emp_code:
                tasklog.warning("Skipped row with empty Employee Code.")
                skipped_count += 1
                continue

            attendance_date_raw = pd.to_datetime(row.get("Attendance date"), errors="coerce")
            if pd.isna(attendance_date_raw):
                tasklog.warning("Skipped Emp %s: missing or invalid attendance date.", emp_code)
                skipped_count += 1
                continue

            attendance_date = attendance_date_raw.date()
            if attendance_date < cutoff_date:
                skipped_count += 1
                continue

            defaults = {
                "full_name": row.get("Full name"),
                "employment_status": row.get("Employment status"),
                "company": row.get("Company"),
                "business_unit": row.get("Business Unit"),
                "department": row.get("Department"),
                "sub_department": row.get("Sub department"),
                "designation": row.get("Designation"),
                "branch": row.get("Branch"),
                "sub_branch": row.get("Sub branch"),
                "punch_in_punch_out_time": row.get("Punch/clocking time"),
                "shift_code": row.get("Shift code"),
                "shift_timing": row.get("Shift timings"),
                "Late_or_early": row.get("Late or early"),
                "working_hours": row.get("Working hour"),
                "total_office_hours": row.get("Total office hours"),
                "source": row.get("Source"),
                "date_of_joining": (
                    pd.to_datetime(row.get("Date of joining"), errors="coerce").date()
                    if pd.notna(row.get("Date of joining"))
                    else None
                ),
                "employment_type": row.get("Employment type"),
                "grade": row.get("Grade"),
                "lattitude_longitude": row.get("Lat long"),
                "level": row.get("Level"),
                "location": row.get("Location"),
                "mobile": row.get("Mobile number"),
                "region": row.get("Region"),
                "reporting_manager": row.get("Reporting manager"),
                "work_email": row.get("Work email"),
                "status_in_out": row.get("Status"),
            }

            qs = DailyAttendance.objects.filter(
                employee_code=emp_code,
                attendance_date=attendance_date,
            ).order_by("pk")

            if not qs.exists():
                DailyAttendance.objects.create(
                    employee_code=emp_code,
                    attendance_date=attendance_date,
                    **defaults,
                )
                created_count += 1
            else:
                obj = qs.first()
                for k, v in defaults.items():
                    setattr(obj, k, v)
                obj.save(update_fields=list(defaults.keys()))
                updated_count += 1

                extras = qs[1:]
                if extras:
                    dedup_deleted += extras.count()
                    extras.delete()

        except Exception as e:
            tasklog.exception(
                "Failed to process row for Emp %s: %s",
                row.get("Employee Code"),
                e,
            )

    tasklog.info(
        "Daily attendance import finished. Created: %d, Updated: %d, Skipped: %d, Dedup-removed: %d.",
        created_count,
        updated_count,
        skipped_count,
        dedup_deleted,
    )


# Keep your old function name for backward compatibility (scheduled jobs, etc.)
def upload_maunal_daily_attendance():
    default_path = os.path.join(MANUAL_ATT_DIR, "Daily_Attendance.xlsx")
    _process_daily_attendance_excel(default_path)


@shared_task
def upload_manual_daily_attendance_task(file_path: str):
    """
    Celery task triggered from the web upload. Processes the uploaded file.
    """
    _process_daily_attendance_excel(file_path)

