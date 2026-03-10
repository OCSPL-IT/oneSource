# oneSource/schedule_task.py
from celery.schedules import crontab
from datetime import timedelta

BEAT_SCHEDULE = {
    # 1) Fetch HROne mail/report
    "hrone-fetch-0805": {
        "task": "HR.tasks.fetch_and_download_attachment_report_task",
        "schedule": crontab(hour=8, minute=5),
    },
    "hrone-fetch-1110": {
        "task": "HR.tasks.fetch_and_download_attachment_report_task",
        "schedule": crontab(hour=11, minute=20),
    },
 

    # 2) Upload Daily Attendance
    "upload-daily-attendance-0805": {
        "task": "HR.tasks.upload_daily_attendance_task",
        "schedule": crontab(hour=8, minute=6),
    },
    "upload-daily-attendance-1110": {
        "task": "HR.tasks.upload_daily_attendance_task",
        "schedule": crontab(hour=11, minute=21),
    },

    # 3) Attendance Regulation
    "ar-upload-0805": {
        "task": "HR.tasks.upload_attendance_regulation_task",
        "schedule": crontab(hour=8, minute=9),
    },
    "ar-upload-1110": {
        "task": "HR.tasks.upload_attendance_regulation_task",
        "schedule": crontab(hour=11, minute=22),
    },

    # 4) Late/Early Go
    "late-early-go-0805": {
        "task": "HR.tasks.upload_late_early_go_task",
        "schedule": crontab(hour=8, minute=8),
    },
    "late-early-go-1110": {
        "task": "HR.tasks.upload_late_early_go_task",
        "schedule": crontab(hour=11, minute=22),
    },

    # 5) On-Duty Request
    "on-duty-upload-0805": {
        "task": "HR.tasks.upload_on_duty_request_task",
        "schedule": crontab(hour=8, minute=8),
    },
    "on-duty-upload-1110": {
        "task": "HR.tasks.upload_on_duty_request_task",
        "schedule": crontab(hour=11, minute=22),
    },

    # 6) Overtime Report
    "overtime-upload-0805": {
        "task": "HR.tasks.upload_overtime_report_task",
        "schedule": crontab(hour=8, minute=8),
    },
    "overtime-upload-1110": {
        "task": "HR.tasks.upload_overtime_report_task",
        "schedule": crontab(hour=11, minute=22),
    },

    # 7) Short Leave
    "short-leave-upload-0805": {
        "task": "HR.tasks.upload_short_leave_task",
        "schedule": crontab(hour=8, minute=5),
    },
    "short-leave-upload-1110": {
        "task": "HR.tasks.upload_short_leave_task",
        "schedule": crontab(hour=11, minute=21),
    },

    # 8) Daily Check-In
    "daily-checkin-upload-0805": {
        "task": "HR.tasks.upload_daily_checkin_task",
        "schedule": crontab(hour=8, minute=7),
    },
    "daily-checkin-upload-1110": {
        "task": "HR.tasks.upload_daily_checkin_task",
        "schedule": crontab(hour=11, minute=23),
    },

    # 9) Helpdesk Tickets
    "helpdesk-upload-0805": {
        "task": "HR.tasks.upload_helpdesk_tickets_task",
        "schedule": crontab(hour=8, minute=10),
    },
    "helpdesk-upload-1110": {
        "task": "HR.tasks.upload_helpdesk_tickets_task",
        "schedule": crontab(hour=11, minute=23),
    },

    "contract-punchout-sync-1310": {
    "task": "CONTRACT.tasks.sync_punch_out_from_hr_contract_task",
    "schedule": crontab(hour=9, minute=15),
    },
    "contract-punchout-sync-1900": {
    "task": "CONTRACT.tasks.sync_punch_out_from_hr_contract_task",
    "schedule": crontab(hour=19, minute=00),
    },
    "contract-punchout-sync-2010": {
        "task": "CONTRACT.tasks.sync_punch_out_from_hr_contract_task",
        "schedule": crontab(hour=23, minute=30),
    },
    # --- START OF NEW SCHEDULES FOR HR CONTRACT ETL ---
    "run-hr-contract-etl-0715": {
        "task": "CONTRACT.tasks.run_hr_contract_etl_task",
        "schedule": crontab(hour=7, minute=15),
    },
    "run-hr-contract-etl-0915": {
        "task": "CONTRACT.tasks.run_hr_contract_etl_task",
        "schedule": crontab(hour=9, minute=15),
    },
    "run-hr-contract-etl-1530": {
        "task": "CONTRACT.tasks.run_hr_contract_etl_task",
        "schedule": crontab(hour=15, minute=30),
    },
    "run-hr-contract-etl-1830": {
        "task": "CONTRACT.tasks.run_hr_contract_etl_task",
        "schedule": crontab(hour=19, minute=30),
    },
    "run-hr-contract-etl-2230": {
        "task": "CONTRACT.tasks.run_hr_contract_etl_task",
        "schedule": crontab(hour=22, minute=30),
    },
    "pc-nmp-pending-summary-0400": {
        # ✅ Update the dotted path below to match where your task is defined
        # Example if task is in PERSONAL_CARE/tasks.py:
        "task": "PERSONAL_CARE.tasks.send_pc_nmp_pending_summary_email",
        "schedule": crontab(hour=16, minute=00),  # runs daily at 04:00PM
    },

     "qc_calibration_reminder_daily": {
        "task": "QC.tasks.send_qc_calibration_reminders",  # import path to the task above
        "schedule": crontab(hour=9, minute=0),  # 9:00 AM every day
    },

      "canteen_headcount_sync_morning": {
        "task": "CANTEEN.tasks.Canteen_Head_Count_sync",
        "schedule": crontab(hour=9, minute=55),   # 09:55 IST
    },
    "canteen_headcount_lunch_email": {
        "task": "CANTEEN.tasks.send_canteen_meal_summary_email",
        "schedule": crontab(hour=10, minute=0),   # 10:00 IST
        "args": ("Lunch",),                       # meal_type
    },

    # --- Evening sync + dinner email ---
    "canteen_headcount_sync_evening": {
        "task": "CANTEEN.tasks.Canteen_Head_Count_sync",
        "schedule": crontab(hour=17, minute=25),  # 17:25 IST
    },
    "canteen_headcount_dinner_email": {
        "task": "CANTEEN.tasks.send_canteen_meal_summary_email",
        "schedule": crontab(hour=17, minute=30),  # 17:30 IST
        "args": ("Dinner",),
    },
}

# Keep Celery on local time
TIMEZONE = "Asia/Kolkata"
