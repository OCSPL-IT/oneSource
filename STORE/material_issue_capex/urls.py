from django.urls import path
from . import views

app_name = "material_issue_capex"

urlpatterns = [
    path("capex-material-dashboard/",views.capex_mi_dashboard,  name="capex_mi_dashboard", ),
    path("capex-material-dashboard/export-excel/", views.capex_mi_export_excel, name="capex_mi_export_excel", ),
    path("capex-material-dashboard/sync-erp/", views.capex_mi_sync_erp, name="capex_mi_sync_erp",),
]
