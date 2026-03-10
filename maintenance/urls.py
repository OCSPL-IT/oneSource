from django.urls import path
from . import views

app_name = "maintenance"  # update namespace to match

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("dashboard/", views.dashboard, name="dashboard2"),
    path("update/<int:pk>/", views.update_schedule, name="update"),
    path("upload/", views.upload_excel, name="upload_excel"),
    path("mark-done/<int:pk>/", views.mark_done, name="mark_done"),  # ← NEW

    # NEW: calendar views
    path("calendar/", views.calendar_month, name="calendar"),
    path("calendar/day/<slug:datestr>/", views.calendar_day, name="calendar_day"),
]
