# ACCOUNTS/urls.py
from django.urls import path
from . import views

app_name = "ACCOUNTS"

urlpatterns = [
    path(
        "payables-cashflow/",
        views.payables_cashflow_report,
        name="payables_cashflow_report",
    ),
    path("manual-payables/", views.manual_payable_manage_view, name="manual_payable_manage"),
    path("manual-payables/<int:pk>/edit/", views.manual_payable_edit_view, name="manual_payable_edit"),
    path("manual-payables/<int:pk>/delete/", views.manual_payable_delete_view, name="manual_payable_delete"),
    path("cashflow/payables/party-extend/save/", views.payable_party_extend_save, name="payable_party_extend_save"),
    path("payable-extension/", views.payable_party_extension_manage_view, name="payable_party_extension_manage"),
    path("payable-extension/<int:pk>/edit/", views.payable_party_extension_edit_view, name="payable_party_extension_edit"),
    path("payable-extension/<int:pk>/delete/", views.payable_party_extension_delete_view, name="payable_party_extension_delete"),
]
