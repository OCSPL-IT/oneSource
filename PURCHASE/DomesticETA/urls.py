# app_name/urls.py

from django.urls import path
from . import views

app_name = "domestic_eta"   # optional but recommended

urlpatterns = [
    # List page (table / filters)
    path("domestic-eta/",views.domestic_eta_list,  name="domestic_eta_list",  ),
    # Create new record
    path( "domestic-eta/add/",  views.domestic_eta_create, name="domestic_eta_create",  ),
    # Edit existing record
    path("domestic-eta/<int:pk>/edit/", views.domestic_eta_edit,  name="domestic_eta_edit",),
    # (optional) detail view
    path("domestic-eta/<int:pk>/", views.domestic_eta_detail,  name="domestic_eta_detail", ),
    path("domestic-eta/<int:pk>/photo/", views.domestic_eta_photo, name="domestic_eta_photo",),
    path("domestic-eta/<int:pk>/delete/", views.domestic_eta_delete, name="domestic_eta_delete"),
    path("export-excel/",views.domestic_eta_export_excel,name="domestic_eta_export_excel",),

    path("eta-dashboard/", views.eta_dashboard, name="eta_dashboard"),
    path("eta-dashboard/data/", views.eta_dashboard_data, name="eta_dashboard_data"),
    path("dashboard/export/", views.eta_dashboard_export_excel, name="eta_dashboard_export_excel"),

]
