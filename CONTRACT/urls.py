from django.urls import path
from . import views

urlpatterns = [
    path('contract/employee-report/', views.contract_employee_matched_report, name='contract_employee_matched_report'),
    path('contract_attendance_report/excel/', views.contract_employee_attend_report_excel, name='contract_employee_attend_report_excel'),
    path("sync-hr-contract/", views.sync_hr_contract_view, name="sync_hr_contract"),

    path('contract-employee-names/',views.contract_employee_names,name='contract_employee_names'),


    # ----------Assign  contract employee ---------------------
    path('daily-assignment/', views.daily_assignment_view, name='daily_assignment'),
    path('assignment-dashboard/', views.assignment_dashboard_page, name='assignment_dashboard_page'),
    path('assignment-dashboard/data/', views.assignment_dashboard_data, name='assignment_dashboard_data'),
    path('assignment-dashboard/export/', views.assignment_dashboard_export, name='assignment_dashboard_export'),
    path('assignment-dashboard/update-punch-out/', views.update_punch_out_view, name='update_punch_out'),
    path('assignment-dashboard/re-assign/', views.reassign_employee_view, name='reassign_employee'),
    path("assignment-dashboard/update-shift/", views.update_shift_view, name="update_shift"),

]




