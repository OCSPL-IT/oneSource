
from django.contrib import admin
from django.urls import path, include


urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('main.urls')),
    path('', include('HR.urls')),
    path('', include('STORE.urls')),
    path('', include('STORE.material_issue_capex.urls')),
    path('', include('PRODUCTION.urls')),
    path('daily-block/', include('PRODUCTION.daily_block.urls')),
    path('', include('EHS.urls')),
    path('', include('ETP.urls')),
    path('', include('ETP.BIOREACTOR.urls')),
    path('mee/', include('ETP.MEE.urls')),
    path('', include('CANTEEN.urls')),
    path('', include('UTILITY.urls')),
    path("qc/", include(("QC.urls", "qc"), namespace="qc")),
    path('', include('CREDENTIALS.urls')),
    path('', include('CONTRACT.urls')),
    path('', include('HR_BUDGET.urls')),
    path('', include('R_and_D.urls')),
    path('', include('PERSONAL_CARE.urls')),
    path('',include('IMPORT_EXPORT.Export_Commercial_Invoice.urls')),
    path('', include('ERP_Reports.urls')),
    path("maintenance/", include("maintenance.urls")),
    path('', include('REPORTS.urls')),
    path('', include('ACCOUNTS.Receivable.urls')),
    path("accounts/", include("ACCOUNTS.CASHFLOW.urls")),
    path("accounts/budgets/", include("ACCOUNTS.Budget.urls")),
    path('', include('PURCHASE.DomesticETA.urls')),
    path("sales-crm/", include(("SALES_MARKETING.sales_crm.urls", "sales_crm"), namespace="sales_crm")),
    path("select2/", include("django_select2.urls")),
    path("__reload__/", include("django_browser_reload.urls")),
]

