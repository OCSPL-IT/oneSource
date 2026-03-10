
from django.urls import path
from HR_BUDGET import views


urlpatterns = [
    path('add-contractor-wages/', views.add_contractor_wages, name='add_contractor_wages'),
    path('contractorwages/', views.contractorwages_list, name='contractorwages_list'),
    path('contractorwages/edit/<int:pk>/', views.edit_contractor_wages, name='edit_contractor_wages'),
    path('contractorwages/delete/<int:pk>/', views.delete_contractor_wages, name='delete_contractor_wages'),

    path('add-security-wages/', views.add_security_wages, name='add_security_wages'),
    path('securitywages/', views.securitywages_list, name='securitywages_list'),
    path('securitywages/<int:pk>/edit/', views.edit_security_wages, name='edit_security_wages'),
    path('securitywages/<int:pk>/delete/', views.delete_security_wages, name='delete_security_wages'),
    
    path('add-hrbudget-welfare/', views.add_hrbudget_welfare, name='add_hrbudget_welfare'),
    path('welfare/', views.hrbudget_welfare_list, name='hrbudget_welfare_list'),
    path('welfare/<int:pk>/edit/', views.edit_hrbudget_welfare, name='edit_hrbudget_welfare'),
    path('welfare/<int:pk>/delete/', views.delete_hrbudget_welfare, name='delete_hrbudget_welfare'),

    path('add-hrbudget-canteen/', views.add_hrbudget_canteen, name='add_hrbudget_canteen'),
    path('canteen/', views.hrbudget_canteen_list, name='hrbudget_canteen_list'),
    path('canteen/<int:pk>/edit/', views.edit_hrbudget_canteen, name='edit_hrbudget_canteen'),
    path('canteen/<int:pk>/delete/', views.delete_hrbudget_canteen, name='delete_hrbudget_canteen'),


    path('add-hrbudget-medical/', views.add_hrbudget_medical, name='add_hrbudget_medical'),
    path('medical-list/', views.hrbudget_medical_list, name='hrbudget_medical_list'),
    path('edit-hrbudget-medical/<int:pk>/', views.edit_hrbudget_medical, name='edit_hrbudget_medical'),
    path('delete-hrbudget-medical/<int:pk>/', views.delete_hrbudget_medical, name='delete_hrbudget_medical'),


    path('add-hrbudget-vehicle/', views.add_hrbudget_vehicle, name='add_hrbudget_vehicle'),
    path('vehicle-list/', views.hrbudget_vehicle_list, name='hrbudget_vehicle_list'),
    path('edit-hrbudget-vehicle/<int:pk>/', views.edit_hrbudget_vehicle, name='edit_hrbudget_vehicle'),
    path('delete-hrbudget-vehicle/<int:pk>/', views.delete_hrbudget_vehicle, name='delete_hrbudget_vehicle'),


    path('add-hrbudget-travelling/', views.add_hrbudget_travelling_lodging, name='add_hrbudget_travelling_lodging'),
    path('travelling-list/', views.hrbudget_travelling_lodging_list, name='hrbudget_travelling_lodging_list'),
    path('edit-hrbudget-travelling/<int:pk>/', views.edit_hrbudget_travelling_lodging, name='edit_hrbudget_travelling_lodging'),
    path('delete-hrbudget-travelling/<int:pk>/', views.delete_hrbudget_travelling_lodging, name='delete_hrbudget_travelling_lodging'),

    path('add-hrbudget-guesthouse/', views.add_hrbudget_guesthouse, name='add_hrbudget_guesthouse'),
    path('guesthouse-list/', views.hrbudget_guesthouse_list, name='hrbudget_guesthouse_list'),
    path('edit-hrbudget-guesthouse/<int:pk>/', views.edit_hrbudget_guesthouse, name='edit_hrbudget_guesthouse'),
    path('delete-hrbudget-guesthouse/<int:pk>/', views.delete_hrbudget_guesthouse, name='delete_hrbudget_guesthouse'),

    path('add-hrbudget-general_admin/', views.add_hrbudget_general_admin, name='add_hrbudget_general_admin'),
    path('general-admin-list/', views.hrbudget_general_admin_list, name='hrbudget_general_admin_list'),
    path('edit-hrbudget-general_admin/<int:pk>/', views.edit_hrbudget_general_admin, name='edit_hrbudget_general_admin'),
    path('delete-hrbudget-general_admin/<int:pk>/', views.delete_hrbudget_general_admin, name='delete_hrbudget_general_admin'),


    path('add-hrbudget-communication/', views.add_hrbudget_communication, name='add_hrbudget_communication'),
    path('edit-hrbudget-communication/<int:pk>/', views.edit_hrbudget_communication, name='edit_hrbudget_communication'),
    path('delete-hrbudget-communication/<int:pk>/', views.delete_hrbudget_communication, name='delete_hrbudget_communication'),
    path('hrbudget-communication-list/', views.hrbudget_communication_list, name='hrbudget_communication_list'),


    path('insurance-mediclaim/add/', views.add_insurance_mediclaim, name='add_insurance_mediclaim'),
    path('insurance-mediclaim/', views.insurance_mediclaim_list, name='insurance_mediclaim_list'),  # Assuming you have list view
    path('insurance-mediclaim/edit/<int:pk>/', views.edit_insurance_mediclaim, name='edit_insurance_mediclaim'),
    path('insurance-mediclaim/delete/<int:pk>/', views.delete_insurance_mediclaim, name='delete_insurance_mediclaim'),


    path('amc/add/', views.add_hrbudget_amc, name='add_hrbudget_amc'),            # Add view you already have
    path('amc/', views.hrbudget_amc_list, name='hrbudget_amc_list'),             # List view (implement separately)
    path('amc/edit/<int:pk>/', views.edit_hrbudget_amc, name='edit_hrbudget_amc'),
    path('amc/delete/<int:pk>/', views.delete_hrbudget_amc, name='delete_hrbudget_amc'),

     path('training/add/', views.add_hrbudget_training, name='add_hrbudget_training'),   # Your existing add view
    path('training/', views.hrbudget_training_list, name='hrbudget_training_list'),     # List view (implement separately)
    path('training/edit/<int:pk>/', views.edit_hrbudget_training, name='edit_hrbudget_training'),
    path('training/delete/<int:pk>/', views.delete_hrbudget_training, name='delete_hrbudget_training'),


    path('legal/', views.hrbudget_legal_list, name='hrbudget_legal_list'),
    path('legal/add/', views.add_hrbudget_legal, name='add_hrbudget_legal'),
    path('legal/edit/<int:pk>/', views.edit_hrbudget_legal, name='edit_hrbudget_legal'),
    path('legal/delete/<int:pk>/', views.delete_hrbudget_legal, name='delete_hrbudget_legal'),


    path('admin-repair-maintenance/', views.admin_repair_list, name='admin_repair_list'),
    path('admin-repair-maintenance/add/', views.add_admin_repair, name='add_admin_repair'),
    path('admin-repair-maintenance/edit/<int:pk>/', views.edit_admin_repair, name='edit_admin_repair'),
    path('admin-repair-maintenance/delete/<int:pk>/', views.delete_admin_repair, name='delete_admin_repair'),

    path('admincapex/', views.admin_capex_list, name='admin_capex_list'),
    path('admincapex/add/', views.add_admin_capex, name='add_admin_capex'),
    path('admincapex/edit/<int:pk>/', views.edit_admin_capex, name='edit_admin_capex'),
    path('admincapex/delete/<int:pk>/', views.delete_admin_capex, name='delete_admin_capex'),

    path('monthly_hr_budget_summary/', views.monthly_hr_budget_summary, name='monthly_hr_budget_summary'),    
    path('download_hr_budget_excel/', views.download_hr_budget_excel, name='download_hr_budget_excel'),

    path('hrbudget/plan/', views.add_hr_budget_plan, name='add_hr_budget_plan'),
    path('hrbudget/plan/<int:pk>/edit/', views.hrbudget_edit_plan, name='hrbudget_edit_plan'),
    path('hrbudget/plan/<int:pk>/delete/', views.hrbudget_delete_plan, name='hrbudget_delete_plan'),
]
