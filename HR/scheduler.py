from apscheduler.schedulers.background import BackgroundScheduler


def start():
    from HR.tasks import upload_daily_attendance,upload_attendance_regulation,upload_daily_checkin,upload_late_early_go
    scheduler = BackgroundScheduler()
    # scheduler.add_job(upload_daily_attendance, trigger='cron', hour=12, minute=0)  # Every minute at 55 minut
    # scheduler.add_job(upload_attendance_regulation, trigger='cron', minute='*/55')  # Every minute at 55 minut
    # scheduler.add_job(upload_daily_checkin, trigger='cron', minute='*/55')  # Every minute at 55 minut
    # scheduler.add_job(upload_late_early_go, trigger='cron', minute='*/55')  # Every minute at 55 minut
    # scheduler.add_job(upload_daily_attendance, trigger='cron', hour=8, minute=0)           # 08:00
    # scheduler.add_job(upload_attendance_regulation, trigger='cron', hour=8, minute=0)       # 08:00

    scheduler.start()
