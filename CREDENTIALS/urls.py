from django.urls import path
from . import views

urlpatterns = [
    path('credentials/add/', views.credential_add, name='credential_add'),
    path('credentials/view/', views.credential_list, name='credential_list'),
    path('credentials/edit/<int:pk>/', views.credential_edit, name='credential_edit'),
    path('credentials/delete/<int:pk>/', views.credential_delete, name='credential_delete'),
    
    
    
    
    
    path("extension/", views.extension_list, name="extension_list"),
    path("extension/new/", views.extension_create, name="extension_create"),
    path("extension/<int:pk>/edit/", views.extension_update, name="extension_update"),
    path("extension_find_by_name/", views.extension_find_by_name, name="extension_find_by_name"),
    path("extension/export/", views.extension_export_xlsx, name="extension_export_xlsx"),
]
