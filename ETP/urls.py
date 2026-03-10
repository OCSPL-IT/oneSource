from django.urls import path
from ETP import views

urlpatterns = [
    path('add-effluent/', views.add_effluent_record, name='add_effluent_record'),

    path('get_products/', views.get_products, name='get_products'),
    path('get_stage_names/', views.get_stage_names, name='get_stage_names'),
    path('get_batch_nos/', views.get_batch_nos, name='get_batch_nos'),
    path('get_voucher_details_by_batch/', views.get_voucher_details_by_batch, name='get_voucher_details_by_batch'),
    path('get_effluent_qty_details/', views.get_effluent_qty_details, name='get_effluent_qty_details'),

        
    path('add-effluent/', views.add_effluent_record, name='add_effluent_record'),
    path('view-effluent-records/', views.effluent_records_list, name='view_effluent_records'),
    path('effluent/<int:pk>/edit/', views.edit_effluent_record, name='edit_effluent_record'),
    path('effluent/qty/<int:qty_id>/delete/', views.delete_effluent_qty, name='delete_effluent_qty'),
    path('download-effluent-excel/', views.download_effluent_excel, name='download_effluent_excel'),
    path('effluent/report/', views.effluent_plan_actual_report, name='effluent_plan_actual_report'),

    

    path('add-general-effluent/', views.add_general_effluent, name='add_general_effluent'),
    path('view-general-effluent/', views.view_general_effluent_records, name='view_general_effluent'),
    path('edit-general-effluent/<int:pk>/', views.edit_general_effluent, name='edit_general_effluent'),
    path('general-effluent/delete/<int:pk>/', views.delete_general_effluent, name='delete_general_effluent'),
    path('general-effluent/chart/', views.general_effluent_charts, name='general_effluent_charts'),


    path("effluent/api/received/", views.api_effluent_received, name="api_effluent_received"),
    path("effluent/primary-treatment/add/", views.primary_treat_create, name="pte_create"),
    path("etp/pte/", views.primary_treat_list, name="pte_list"),
    path("etp/pte/<int:pk>/edit/", views.primary_treat_edit, name="pte_edit"),
    path("etp/pte/<int:pk>/delete/", views.primary_treat_delete, name="pte_delete"),
    path("etp/pte/export/", views.primary_treat_excel, name="primary_treat_excel"),  # NEW


        # Dashboard UI (HTML page)
    path("effluent/dashboard/", views.effluent_dashboard, name="effluent_dashboard"),
    path("effluent/dashboard-export/", views.effluent_dashboard_export, name="effluent_dashboard_export"),
    path("effluent/dashboard-data/", views.effluent_dashboard_data, name="effluent_dashboard_data"),
    
    path("effluent-dashboard-data-kl/", views.effluent_dashboard_data_kl, name="effluent_dashboard_data_kl"),
    path("effluent-dashboard-export-kl/", views.effluent_dashboard_export_kl, name="effluent_dashboard_export_kl"),
    
    path("effluent/dashboard-data-hw/", views.effluent_dashboard_data_hw, name="effluent_dashboard_data_hw"),
    path("effluent/export-hw/", views.effluent_dashboard_export_hw, name="effluent_dashboard_export_hw"),
    path("effluent/dashboard/data-tanks/", views.effluent_dashboard_data_tanks, name="effluent_dashboard_data_tanks"),
    path("effluent/dashboard/data/primary/", views.effluent_dashboard_data_primary,name="effluent_dashboard_data_primary"),
    path("effluent/dashboard/data-mee/",views.effluent_dashboard_data_mee,name="effluent_dashboard_data_mee",),
    path("effluent/dashboard/data-atfd/", views.effluent_dashboard_data_atfd, name="effluent_dashboard_data_atfd"),
    path("effluent/dashboard-data-mass-balance/", views.effluent_dashboard_data_mass_balance,name="effluent_dashboard_data_mass_balance"),
    
    
    path("api/transporter_vehicles/", views.api_transporter_vehicles, name="api_transporter_vehicles"),
    path("api/disposal-rates/", views.api_disposal_rates, name="api_disposal_rates"),
    path("hazardous-waste-create/", views.hazardous_waste_create, name="hazardous_waste_create"),    
    path("hazardous-waste/", views.hazardous_waste_list, name="hazardous_waste_list"),
    path("hazardous-waste/export-xlsx/", views.hazardous_waste_export_xlsx, name="hazardous_waste_export_xlsx"),
    path("hazardous-waste/<int:pk>/edit/", views.hazardous_waste_edit, name="hazardous_waste_edit"),
    path("hazardous-waste/<int:pk>/delete/", views.hazardous_waste_delete, name="hazardous_waste_delete"),


    path("etp/opening-balances/", views.opening_balance_form, name="opening_balance_form"),
    path("etp/tank/", views.effluent_tank_report,     name="etp_storage_tank_report"),
]
