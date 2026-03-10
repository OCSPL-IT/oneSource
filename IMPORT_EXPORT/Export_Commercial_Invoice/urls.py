# export/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path("invoice/post-shipment/", views.invoice_post_shipment_list_view, name="invoice_post_shipment_list",  ),
    path("invoice/post-shipment/new/",views.invoice_post_shipment_form_view,name="invoice_post_shipment_form",),
    path("invoice/post-shipment/<int:pk>/edit/", views.invoice_post_shipment_edit_view, name="invoice_post_shipment_edit"),
    path("invoice/post-shipment/<int:pk>/",views.invoice_post_shipment_detail_view, name="invoice_post_shipment_detail",),
    path("invoice/post-shipment/<int:pk>/delete/",views.invoice_post_shipment_delete_view,name="invoice_post_shipment_delete"),
    path("invoice/post-shipment/<int:pk>/packing/", views.invoice_post_shipment_packing_list_view, name="invoice_post_shipment_packing_list"),
    path("invoice-post-shipment/<int:pk>/view/", views.invoice_post_shipment_detail_simple, name="invoice_post_shipment_detail_simple"),
    path("invoice-post-shipment/<int:pk>/attachment/", views.invoice_post_shipment_download_attachment, name="invoice_post_shipment_download_attachment"),

]
