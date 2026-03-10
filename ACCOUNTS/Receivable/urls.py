# ACCOUNTS/urls.py
from django.urls import path

# IMPORTANT:
# If you have moved Receivable views into ACCOUNTS/Receivable/views.py (or a views package),
# import them explicitly from that module instead of ".views".
from ACCOUNTS.Receivable import views as rcv_views
# Receivable views
from ACCOUNTS.Receivable import ajax_views as rcv_ajax_views  # ✅ NEW

app_name = "accounts"

urlpatterns = [
    # Receivables
    path("receivables/dashboard/", rcv_views.receivable_dashboard, name="receivable_dashboard"),
    path("receivables/new/", rcv_views.ReceivableCreateView.as_view(), name="receivable_new"),
    path("receivables/list/", rcv_views.ReceivableListView.as_view(), name="receivable_list"),
    path("receivables/<int:pk>/edit/", rcv_views.ReceivableUpdateView.as_view(), name="receivable_edit"),
    path("receivables/<int:pk>/delete/", rcv_views.ReceivableDeleteView.as_view(), name="receivable_delete"),
    # ✅ Export to Excel (same filters)
    path("receivables/list/excel/", rcv_views.receivable_list_excel, name="receivable_list_excel"),
    path("receivables/dashboard/excel/", rcv_views.receivable_dashboard_excel, name="receivables_dashboard_excel"),

    # ------------------------------------------------------------------
    # Receivables AJAX (Customer → Invoice dependent dropdowns) ✅ NEW
    # ------------------------------------------------------------------
    path("receivables/ajax/customers/", rcv_ajax_views.ajax_customers, name="ajax_customers"),
    path("receivables/ajax/customer-invoices/", rcv_ajax_views.ajax_customer_invoices, name="ajax_customer_invoices"),

    # Snapshot sync
    path("receivables/snapshot/sync/", rcv_views.receivables_snapshot_sync, name="receivables_snapshot_sync"),
    path("receivables/snapshot/sync/start/", rcv_views.receivables_snapshot_sync_start, name="receivables_snapshot_sync_start"),
    path("receivables/snapshot/sync/status/", rcv_views.receivables_snapshot_sync_status, name="receivables_snapshot_sync_status"),

    # Targets (Weekly Payment Target)
    path("receivables/targets/", rcv_views.target_list, name="payment_target_list"),
    path("receivables/targets/new/", rcv_views.target_create, name="payment_target_create"),
    path("receivables/targets/<int:pk>/", rcv_views.target_detail, name="payment_target_detail"),
    path("receivables/targets/<int:pk>/edit/", rcv_views.target_edit, name="payment_target_edit"),
    path("receivables/targets/<int:pk>/delete/", rcv_views.target_delete, name="payment_target_delete"),
    path("receivables/targets/<int:pk>/select-bills/", rcv_views.target_select_bills, name="payment_target_select_bills"),
    path("receivables/targets/<int:pk>/report/", rcv_views.target_report, name="payment_target_report"),
    path("receivables/targets/<int:pk>/excel/", rcv_views.target_detail_excel, name="payment_target_detail_excel"),

    # Reminder Masters: Party Master
    path("receivables/party-master/", rcv_views.party_list, name="party_list"),
    path("receivables/party-master/new/", rcv_views.party_create, name="party_create"),
    path("receivables/party-master/<int:pk>/edit/", rcv_views.party_edit, name="party_edit"),

    # Reminder Masters: Outgoing Email Accounts
    path("receivables/email-accounts/", rcv_views.email_account_list, name="email_account_list"),
    path("receivables/email-accounts/new/", rcv_views.email_account_create, name="email_account_create"),

    path("receivables/outstanding-all/", rcv_views.receivable_outstanding_all_pdc, name="receivable_outstanding_all_pdc"),
]
