from django.urls import path
from . import views

urlpatterns = [
    path('moisture/add/', views.add_r_and_d_moisture, name='add_r_and_d_moisture'),
    path('moisture/<int:pk>/edit/', views.edit_r_and_d_moisture, name='edit_r_and_d_moisture'),
    path('moisture/', views.r_and_d_moisture_list, name='r_and_d_moisture_list'),
    path('moisture/<int:pk>/delete/', views.delete_r_and_d_moisture, name='delete_r_and_d_moisture'),
    path('r_and_d_moisture/download_xlsx/', views.r_and_d_moisture_download_xlsx, name='r_and_d_moisture_download_xlsx'),

    path('kfactor/add/', views.add_kf_factor_entry, name='add_kf_factor_entry'),
    path('kfactor/list/', views.kf_factor_entry_list, name='kf_factor_entry_list'),
    path('kfactor/edit/<int:pk>/', views.kf_factor_entry_edit, name='kf_factor_entry_edit'),
    path('kf-factor-entry/<int:pk>/delete/', views.kf_factor_entry_delete, name='kf_factor_entry_delete'),
    path('kf-factor-entry/download_excel/', views.kf_factor_entry_download_excel, name='kf_factor_entry_download_excel'),


    path('melting_point/add/', views.add_melting_point_record, name='add_melting_point_record'),
    path('melting_point/<int:pk>/edit/', views.edit_melting_point_record, name='edit_melting_point_record'),
    path('melting_point/', views.melting_point_record_list, name='melting_point_record_list'),
    path('melting_point/<int:pk>/delete/', views.delete_melting_point_record, name='delete_melting_point_record'),
    path('melting_point/download_xlsx/', views.melting_point_record_download_excel, name='melting_point_record_download_excel'),


]
