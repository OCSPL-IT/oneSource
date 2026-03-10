# Place this code in: PRODUCTION/daily_block/urls.py

from django.urls import path
from . import views

# This app_name is very important. It must match the namespace used
# in your template's JavaScript, which is 'daily_checks'.
app_name = 'daily_checks'

urlpatterns = [
    # URL for creating a new daily check entry
    path('entry/new/', views.daily_check_entry, name='entry_new'),

    # URL for viewing an existing entry (used by the form's redirect)
    path('entry/<int:pk>/', views.daily_report_detail, name='report_detail'),    # URL for editing an existing entry
    path('entry/<int:pk>/edit/', views.daily_check_edit, name='entry_edit'),
    path('daily_list/', views.daily_block_list, name='daily_block_list'),
    path('report/<int:pk>/delete/', views.delete_daily_check, name='delete_daily_check'),
    path('daily-block-dashboard/', views.daily_dashboard_page, name='daily_block_dashboard'),
    path("production-dashboard/data/", views.production_dashboard_data, name="production_dashboard_data"),
    path("daily-block/dashboard/export/", views.export_production_data_to_excel, name="export_production_data"),

    # --- AJAX API URLs ---
    # These names must match exactly what's used in your JavaScript.
    path('api/stages/', views.stage_list_api, name='stage_list_api'),
    path('api/stage-detail/', views.stage_detail_api, name='stage_detail_api'),
    path('api/get-equipment/', views.get_equipment_api, name='get_equipment_api'),
    path('api/get-batch-numbers/', views.get_batch_numbers_api, name='get_batch_numbers_api'),
    path('api/get-bom-details/', views.get_bom_details_by_stage_api, name='get_bom_details_api'),
    path('api/get-all-bom-equipment/', views.get_all_bom_equipment_api, name='get_all_bom_equipment_api'),
    
    path('trigger-sync/', views.trigger_erp_sync, name='trigger_erp_sync'),
    # path('task-status/<str:task_id>/', views.get_task_status, name='get_task_status'),
]