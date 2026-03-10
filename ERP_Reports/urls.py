# erp_reports/urls.py

from django.urls import path
from .views import *

app_name = 'erp_reports'

urlpatterns = [
    path('cogs/', cogs_report, name='cogs_report'),
    path("gst-reco/upload/", gst2b_upload_view, name="gst2b_upload"),
    path('bank-fc-ledger/', bank_fc_ledger_report, name='bank_fc_ledger'),
    path('rgp-issue-grn/', rgp_issue_grn_report, name='rgp_issue_grn_report'),
    path("inventory-ageing/",inventory_ageing_report,name="inventory_ageing_report"),
    path("inventory-ageing/summary-api/",inventory_ageing_summary_api,name="inventory_ageing_summary_api"),
    path("inventory-ageing/export-csv/",inventory_ageing_export_csv,name="inventory_ageing_export_csv"),
    path("inventory-ageing/rebuild-snapshot/",inventory_ageing_rebuild_snapshot,name="inventory_ageing_rebuild_snapshot"),
    path("inventory-ageing/export-xlsx/", inventory_ageing_export_xlsx, name="inventory_ageing_export_xlsx"),
    path("retest-dashboard/", retest_dashboard, name="retest_dashboard"),
]
