# PERSONAL_CARE/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path("pc-customers/new/", views.pc_customer_create,name="pc_customer_create",),
    path("pc-customers/<int:pk>/edit/", views.pc_customer_create, name="pc_customer_edit"),   ## for edit the record
    path("pc-customers/<int:pk>/delete/", views.pc_customer_delete, name="pc_customer_delete"),  ## delete the record
    path("pc-customers/", views.pc_customer_list, name="pc_customer_list"),
    path("pc-customers/export/", views.pc_customer_export, name="pc_customer_export"),
    path("pc-sample-requests/history/<int:pk>/", views.pc_sample_request_history, name="pc_sample_request_history", ),
    
    path("pc-dashboard/", views.pc_customer_dashboard, name="pc_customer_dashboard"),
    path("pc-dashboard/data/", views.pc_customer_dashboard_data, name="pc_customer_dashboard_data", ),
    path("pc-dashboard/sample-data/", views.pc_sample_dashboard_data, name="pc_sample_dashboard_data"),
    path("pc-followup-dashboard-data/", views.pc_followup_dashboard_data, name="pc_followup_dashboard_data"),
    path("pc-dashboard/missing-customers-data/", views.pc_missing_customers_dashboard_data,name="pc_missing_customers_dashboard_data"),
    path("pc/dashboard/other-customers/data/",views.pc_other_customer_dashboard_data, name="pc_other_customer_dashboard_data", ),
    
    
    path("pc-sample-requests/", views.pc_sample_request_list, name="pc_sample_request_list"),
    path("pc-sample-requests/export/", views.pc_sample_request_export, name="pc_sample_request_export"),
    path("pc-sample-requests/new/", views.pc_sample_request_create, name="pc_sample_request_create"),
    path("pc-sample-requests/<int:pk>/edit/", views.pc_sample_request_create, name="pc_sample_request_edit"),
    path("pc/sample-requests/<int:pk>/delete/",views.pc_sample_request_delete,name="pc_sample_request_delete",),
    path("pc-sample-requests/<int:pk>/update-nmp/",views.pc_sample_request_update_approval,name="pc_sample_request_update_approval",),
    
    
    path('api/customer-contacts/', views.get_PCcustomer_contacts, name='get_PCcustomer_contacts'),



    path("tasks/new/", views.pc_task_create, name="pc_task_create"),
    path("pc-tasks/", views.pc_task_list, name="pc_task_list"),
    path("pc/tasks/<int:pk>/edit/", views.pc_task_edit, name="pc_task_edit"),
    path("pc/tasks/<int:pk>/delete/", views.pc_task_delete, name="pc_task_delete"),
    path("pc/tasks/export/excel/", views.pc_task_export_excel, name="pc_task_export_excel"),
    path("pc-followups/upload/", views.pc_followup_upload, name="pc_followup_upload"),
    path("pc-followups/upload/confirm/", views.pc_followup_upload_confirm, name="pc_followup_upload_confirm"),

    
    path("pc/other-customer/new/", views.pc_other_customer_list, name="pc_other_customer_list"),
    path("pc/other-customer/add/", views.pc_other_customer_create, name="pc_other_customer_create"),
    path("pc/other-customer/<int:pk>/edit/", views.pc_other_customer_create, name="pc_other_customer_edit"),
    path("pc/other-customer/<int:pk>/delete/", views.pc_other_customer_delete, name="pc_other_customer_delete"),
    path("pc/other-customer/export/", views.pc_other_customer_export, name="pc_other_customer_export"),


]
