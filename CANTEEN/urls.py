
from django.urls import path
from . import views

urlpatterns = [
     path('canteen-dashboard/', views.canteen_dashboard, name='canteen_dashboard'),
     # path('attendance/', views.update_attendance, name='update_attendance'),
     # path('device_push/', views.device_push, name='device_push'),
     
      # API endpoint to trigger the Celery sync task
    path('sync/', views.fetch_attendance_from_device, name='fetch_attendance_from_device'),
    
     # API endpoint to get the status of the sync task
    path('sync/status/', views.get_sync_status, name='get_sync_status'),
    
     # API endpoints from your original code
    path('api/update-attendance/', views.update_attendance, name='update_attendance'),
    path('api/device-push/', views.device_push, name='device_push'),
    path('api/dashboard-data/', views.dashboard_api_data, name='dashboard_api_data'),
    
    
     path('canteen-attendance-summary/', views.canteen_attendance_summary_report, name='canteen_attendance_summary_report'),
     path('download_canteen/download/', views.download_canteen_excel, name='download_canteen_excel'),

     path('attendance-list/', views.attendance_list, name='attendance_list'),
     path('attendance/xlsx/', views.attendance_xlsx,name='attendance_xlsx'),
     
     
     path("canteen/headcount/dashboard/",views.ch_count_dashboard, name="ch_count_dashboard",),
     # Trigger device sync (Celery task)
     path("canteen/headcount/fetch-from-device/",views.fetch_canteen_headcount_from_device, name="fetch_canteen_headcount_from_device", ),
     # API: update attendance via POST JSON
     path("canteen/headcount/update/",views.ch_update_attendance,name="ch_update_attendance", ),
     # API: push from punching device
     path("canteen/headcount/device-push/", views.ch_device_push, name="ch_device_push", ),
     # API: dashboard data (used by JS to refresh cards + chart + table)
     path("canteen/headcount/dashboard-api/", views.ch_dashboard_api_data, name="ch_dashboard_api_data", ),
     path("canteen-headcount/export-excel/", views.ch_export_excel, name="ch_export_excel"),




     
     #  Below urls for user add delete to the device 
     path('device/add-user/', views.add_device_user, name='add_device_user'),
     path("device-user/add/", views.add_user_page, name="device_user_add"),
     path('device/export-users/', views.download_device_users_excel, name='export_device_users_excel'),
     path('device/delete-user/', views.delete_device_user, name='delete_device_user'),
     path("device/upload-users-excel/", views.bulk_upload_users_from_excel, name="device_upload_users_excel"),

]