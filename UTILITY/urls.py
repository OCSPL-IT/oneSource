from django.urls import path,include
from UTILITY import views

urlpatterns = [
    # new‐batch
    path("utility/entry/", views.entry_view, name="utility_entry"),
    path("utility/records/", views.utility_readings_report, name="utility_readings_report"),
    path('utility/readings_excel/', views.utility_readings_excel, name='utility_readings_excel'),
    path('utility/edit/date/<str:date_str>/', views.edit_utility_date, name='utility_entry_edit_date'),
    path('utility/delete/<str:date_str>/', views.delete_utility_date, name='utility_entry_delete_date'),
    path('utility/consumption/', views.utility_consumption_report, name='utility_consumption_report'),
    path('utility_consumption_report/export/', views.utility_consumption_excel, name='utility_consumption_excel'),



    path('power-entry/', views.power_entry_view, name='power_entry'),
    path('power-report/', views.power_readings_report, name='power_readings_report'),
    path('power-edit/<str:date_str>/', views.edit_power_date, name='edit_power_date'),
    path('utility/power/delete/<str:date_str>/', views.delete_power_readings_for_date, name='delete_power_readings_for_date'),
    path('power-readings-excel/', views.power_readings_excel, name='power_readings_excel'),
    path('power-consumption/', views.power_consumption_report, name='power_consumption_report'),
    path('power-consumption-excel/', views.power_consumption_excel, name='power_consumption_excel'),

    
]
