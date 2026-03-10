
from django.urls import path
from HR import views



urlpatterns = [
    path('create_hr/', views.create_hr, name='create_hr'),
    path('view_hr_records/', views.view_hr_records, name='view_hr_records'),
    path('edit_hr/<int:pk>/', views.edit_hr, name='edit_hr'),
    path('delete_hr/<int:pk>/', views.delete_hr, name='delete_hr'),


    path('attendance/summary/', views.attendance_summary, name='attendance_summary'),
    path('attendance/summary/<str:status_code>/', views.attendance_by_status, name='attendance_by_status'),
    path('attendance/download_excel/<path:status_code>/', views.attendance_download_excel, name='attendance_download_excel'),


    path('attendance/regulation/summary/', views.attendance_regulation_summary, name='attendance_regulation_summary'),
    path('attendance/regulation/<str:status>/', views.attendance_regulation_by_status, name='attendance_regulation_by_status'),
    path('attendance-regulation-download/<str:status>/', views.download_attendance_regulation_excel, name='download_attendance_regulation_excel'),


    path('daily-check-in-summary/', views.daily_check_in_summary, name='daily_check_in_summary'),
    path('check-in-status/<str:check_in_status>/', views.check_in_status_detail, name='check_in_status_detail'),
    path('check-in/excel/<str:check_in_status>/',views.check_in_status_excel_download,name='check_in_status_excel_download'),

    path('late-early-go-summary/', views.late_early_go_summary, name='late_early_go_summary'),
    path('late-early-go-detail/<str:late_early>/', views.late_early_go_detail, name='late_early_go_detail'),
    path('late-early-go-download/<str:late_early>/', views.late_early_go_download_excel, name='late_early_go_download_excel'),


    path('on-duty-request-summary/', views.on_duty_request_summary, name='on_duty_request_summary'),
    path('on-duty-request-detail/<str:request_status>/', views.on_duty_request_detail, name='on_duty_request_detail'),
    path('on-duty-request-download/<str:request_status>/', views.on_duty_request_download_excel, name='on_duty_request_download'),


    path('attendance_report/', views.attendance_report, name='attendance_report'),

    path('overtime-report-summary/', views.overtime_report_summary, name='overtime_report_summary'),
    path('overtime-report-detail/<str:request_status>/', views.overtime_report_detail, name='overtime_report_detail'),
    path('overtime-report-excel/<str:request_status>/', views.download_overtime_report_excel, name='download_overtime_report_excel'),

    
    path('short-leave-summary/', views.short_leave_summary, name='short_leave_summary'),
    path('short-leave/detail/<str:status>/', views.short_leave_detail, name='short_leave_detail'),
    path('short-leave-download/<str:status>/', views.download_short_leave_excel, name='download_short_leave_excel'),


    path('helpdesk/summary/', views.helpdesk_ticket_summary, name='helpdesk_ticket_summary'),
    path('helpdesk/detail/<str:status>/', views.helpdesk_ticket_detail, name='helpdesk_ticket_detail'),
    path('helpdesk/download/<str:status>/', views.download_helpdesk_ticket_excel, name='download_helpdesk_ticket_excel'),


    path('mis-report/', views.mis_report, name='hr_mis_report'),
    path('mis/download/', views.download_mis_report_excel, name='download_mis_report_excel'),


    path('data_display/', views.fetch_api_data, name='data_display'),

    path('attendance/pivot-report/', views.attendance_pivot_report, name='attendance_pivot_report'),
    path('pivot-report/pivot-report-excel/', views.attendance_pivot_excel, name='attendance_pivot_excel'),
        # --- Add these new paths for the dynamic filters ---
    path('api/get-departments/', views.get_departments, name='api_get_departments'),
    path('api/get-sub-departments/', views.get_sub_departments, name='api_get_sub_departments'),
    path('api/get-shift-codes/', views.get_shift_codes, name='api_get_shift_codes'),

    # --- These is for employee joining ---
    path("employee-joining/new/", views.employee_joining_create, name="employee_joining_create", ),
    path("employee-joining/list/", views.employee_joining_list,  name="employee_joining_list", ),
    path("employee-joining/<int:pk>/edit/", views.employee_joining_edit, name="employee_joining_edit"),
    path("employee-joining/<int:pk>/detail/",views.employee_joining_detail, name="employee_joining_detail",),
    path("employee-joining/<int:pk>/delete/", views.employee_joining_delete, name="employee_joining_delete"),
    path("employee-joining/export/excel/", views.employee_joining_export_excel, name="employee_joining_export_excel"),
    
    # IT user processing endpoint (sets status -> it_in_progress / pending_approval)
    path("employee-joining/<int:pk>/it-update/", views.employee_joining_it_update, name="employee_joining_it_update", ),
    path("employee-joining/<int:pk>/approve/",  views.employee_joining_approve,   name="employee_joining_approve",  ),



    path("attendance/upload/", views.attendance_manual_upload_excel, name="attendance_upload_excel"),
]
