from django.urls import path
from . import views

urlpatterns = [
    path("downtime/new/", views.downtime_form, name="add_downtime"),
    path('search_equipment/', views.search_equipment, name='search_equipment'),
    path('search_product/', views.search_product, name='search_product'),
    path('search_batch/', views.search_batch, name='search_batch'),
    path('get_bom_details/', views.get_bom_details, name='get_bom_details'),
    path('view_downtime/', views.view_downtime, name='view_downtime'),
    path('downtime/<int:pk>/', views.view_downtime_detail, name='view_downtime_detail'),
    path('downtime/<int:pk>/edit/', views.edit_downtime, name='edit_downtime'),
    path('downtime/<int:pk>/delete/', views.delete_downtime, name='delete_downtime'),
    path('downtime/export/', views.export_downtime_excel, name='export_downtime_excel'),  
    path("downtime/report/", views.downtime_report, name="downtime_report"),
]
