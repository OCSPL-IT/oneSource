# REPORTS/urls.py
from django.urls import path
from . import views
urlpatterns = [
    path("plan-vs-actual/", views.plan_vs_actual_daily, name="plan_vs_actual_daily"),
    path("plan-batches.xlsx", views.export_plan_batches_excel, name="export_plan_batches_excel"),
    path("reports/export/prod-debug.xlsx", views.export_prod_debug_excel, name="export_prod_debug_excel"),

]