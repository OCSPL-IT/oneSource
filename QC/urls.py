# QC/urls.py
from django.urls import path
from . import views
from .views import complaint_report_view
from .views import api_get_product_from_stage
from .views import api_all_batches
# Give the app a namespace
app_name = "qc"

urlpatterns = [
    # ─── PRODUCTS ───────────────────────────────────────────
    path('products/',                         views.product_list,      name='product_list'),
    path('products/<int:pk>/view/',           views.product_detail,    name='product_detail'),
    path('products/new/',                     views.product_create,    name='product_create'),
    path('products/create/fg/',               views.product_create_fg, name='product_create_fg'),
    path('products/<int:pk>/edit/',           views.product_update,    name='product_update'),
    path('products/<int:pk>/delete/',         views.product_delete,    name='product_delete'),
    path('products/import-appearance/',       views.import_appearance_view, name='import_appearance'),
    path('products/product/<int:pk>/spec-upload/', views.import_specs, name='spec_upload'),
    path("products/export-xlsx/", views.product_export_xlsx, name="product_export_xlsx"),

    # ─── API ────────────────────────────────────────────────
    # (No leading 'qc/' here; the project-level include provides the '/qc/' prefix)
    path('api/specs/',                 views.get_specs,                 name='get_specs'),
    path('api/product-data/',          views.get_product_details,       name='get_product_details'),
    path('api/get-spec-groups/',       views.get_spec_groups_for_product, name='api_get_spec_groups'),
    path('api/add-sample-description/',views.add_sample_description,    name='api_add_sample_description'),
    path('api/get-test-parameters/',   views.get_test_parameters_for_group, name='api_get_test_parameters'),
    path('api/get-batch-nos/',         views.api_get_batch_nos,         name='api_get_batch_nos'),

    # ─── QC CORE / DASHBOARD / LISTS ────────────────────────
    path('qc',                   views.qc_list,        name='qc_list'),                 # /qc/
    path('qc/entries/',           views.qc_list,        name='qc_list_entries'),         # /qc/entries/
    path('qc/reports/',           views.qc_list,        name='qc_report'),               # /qc/reports/
    path("qc/export-xlsx/", views.qc_export_excel, name="qc_export_excel"),
     path("qc/drafts/", views.qc_draft_list, name="qc_draft_list"),
     
    # ─── unified dashboard ────────────────────────────────
    #path('dashboard/',         views.dashboard,      name='dashboard'),               # /qc/dashboard/
    path('home/',              views.dashboard,      name='qc_home'),                 # /qc/home/
    path('dashboard/',                views.qc_dashboard,   name='qc_dashboard'),            # /qc/v1/
    path("dashboard/export-excel/",views.qc_dashboard_export_excel, name="qc_dashboard_export_excel",),

    # ─── CRUD Views ────────────────────────────────────────
    path('qc/new/',               views.qc_create,      name='qc_create'),
    path('fgqc/new/',          views.fgqc_create,    name='fgqc_create'),
    path('fgqc/',              views.fgqc_create,    name='fgqc_create_root'),
    path("sfgqc/new/",         views.sfgqc_create,  name="sfgqc_create"),
    path('<int:pk>/',          views.qc_detail,      name='qc_detail'),
    path('<int:pk>/edit/',     views.qc_update,      name='qc_update'),
    path('<int:pk>/delete/',   views.qc_delete,      name='qc_delete'),

    # ─── REOPEN / CANCEL ───────────────────────────────────
    path('<int:pk>/reopen_qc/',   views.qc_reopen_for_qc,   name='qc_reopen_for_qc'),
    path('<int:pk>/reopen_prod/', views.qc_reopen_for_prod, name='qc_reopen_for_prod'),
    path('<int:pk>/cancel/',      views.qc_cancel,          name='qc_cancel'),

    # ─── MASTER LISTS & MIS ────────────────────────────────
    path('equipment-master/',  views.equipment_master,  name='equipment_master'),
    path('item-master/',       views.item_master,       name='item_master'),
    path('mis-report/',        views.mis_report,        name='mis_report'),           # /qc/mis-report/

    # ─── SAMPLE DESCRIPTION OPTIONS ────────────────────────
    path('sample-description-options/', views.sample_description_options_view, name='sample_description_options'),

    # ─── MASTER DATA ───────────────────────────────────────
    path('master-data/qc-test-parameters/', views.qc_test_parameter_view, name='qc_test_parameter_list'),

    # ─── Auto_Number_AR ────────────────────────────────────
    path('generate-ar-no/',    views.generate_ar_no,  name='generate_ar_no'),
    path('sync-erp/start/',  views.sync_erp_start,  name='sync_erp_start'),
    path('sync-erp/status/<uuid:job_id>/', views.sync_erp_status, name='sync_erp_status'),

    # ─── Certificate of Analysis ───────────────────────────
    path('<int:pk>/print-coa/', views.generate_coa,  name='print_coa'),
    path('<int:pk>/coa/',       views.generate_coa,  name='generate_coa'),
    path('coa/',                views.coa_list,      name='coa_list'),

    path('report/product-analysis/', views.qc_product_report, name='qc_product_report'),
    path('report/product/download/', views.download_qc_report_excel, name='download_qc_report_excel'),

     #------------Daily QC--------------------------------------------------------------
    path("daily-report/create/", views.daily_report_create, name="daily_report_create"),
    path("daily-report/new/",    views.daily_report_create, name="daily_report_create_alt"),  # alias
    path("daily-report/<int:pk>/", views.daily_report_detail, name="daily_report_detail"),
    

    # NEW: separate manual PDL entries page
    path("pdl/", views.pdl_entries, name="pdl_entries"),

    # AJAX APIs used by template JS (use trailing slashes; keep a legacy alias)
    path("api/fetch-incoming/",     views.api_fetch_incoming,     name="api_fetch_incoming"),           # canonical
    path("api/incoming/",           views.api_fetch_incoming,     name="api_fetch_incoming_legacy"),    # legacy alias
    path("api/daily/fetch-other/",  views.api_fetch_other_header, name="api_fetch_other_header"),
    path("api/pdl-samples/",        views.api_fetch_pdl_samples,  name="api_fetch_pdl_samples"),        # kept for backward-compat
    path("pdl/list/",               views.pdl_list,               name="pdl_list"),
    path("api/header-metrics/",     views.api_fetch_other_header, name="api_fetch_other_header"),
    path("api/ftr-table/",          views.api_ftr_table,          name="api_ftr_table"),

    # ─────────────────────────────────────────────────────────────
    # Customer Complaint module
    # ─────────────────────────────────────────────────────────────
    path("complaints/",                 views.complaint_list,       name="complaint_list"),
    path("complaints/new/",             views.complaint_create,     name="complaint_create"),
    path("complaints/<int:pk>/edit/",   views.complaint_update,     name="complaint_update"),
    path("complaints/report/",          complaint_report_view,      name="complaint_report"),
    path("complaints/<int:pk>/view/",   views.complaint_detail,     name="complaint_detail"),

    # Analytical Downtime
    path("downtime/",                   views.downtime_list,        name="downtime_list"),
    path("downtime/new/",               views.downtime_create,      name="downtime_create"),
    path("downtime/<int:pk>/edit/",     views.downtime_update,      name="downtime_update"),
    path("downtime/report/",            views.downtime_report,      name="downtime_report"),
    path("downtime/report/export/",     views.downtime_export_xlsx, name="downtime_export"),
    path("api/stage-product/",          api_get_product_from_stage, name="api_get_product_from_stage"),
    path("api/downtime/product-and-batches/", views.api_product_and_batches, name="api_product_and_batches",),
    path("instrument-master/",          views.instrument_master_list, name="instrument_master"),
    path("instrument-master/new/",      views.instrument_master_create, name="instrument_master_new"),

    # Deviation module
    path("deviations/",               views.deviation_list,        name="deviation_list"),
    path("deviations/new/",           views.deviation_create,      name="deviation_create"),
    path("deviations/<int:pk>/edit/", views.deviation_update,      name="deviation_update"),
    path("deviations/report/",        views.deviation_report,      name="deviation_report"),
    path("deviations/report/export/", views.deviation_export_xlsx, name="deviation_export"),
    path("api/alfa-finished/",        views.api_alfa_finished,     name="api_alfa_finished"),
    path("api/batches/",              views.api_batches_for_product, name="api_batches_for_product"),
    path("api/all-batches/",          api_all_batches,              name="api_all_batches"),
    path("deviation/<int:pk>/view/",  views.deviation_detail,       name="deviation_detail"),

    # Analytical Mistake
    path("analytical-mistakes/", views.analytical_mistake_list, name="analytical_mistake_list"),
    path("analytical-mistakes/new/", views.analytical_mistake_create, name="analytical_mistake_create"),
    path("analytical-mistakes/<int:pk>/edit/", views.analytical_mistake_update, name="analytical_mistake_update"),
    path("analytical-mistakes/<int:pk>/view/", views.analytical_mistake_detail, name="analytical_mistake_detail"),
    path("analytical-mistakes/report/",        views.analytical_mistake_report,      name="analytical_mistake_report"),
    path("analytical-mistakes/report/export/", views.analytical_mistake_export_xlsx, name="analytical_mistake_export"),    


    path("calibration/", views.calibration_list, name="calibration_list"),
    path("calibration/add/", views.calibration_create, name="calibration_create"),
    path("calibration/<int:pk>/edit/", views.calibration_edit, name="calibration_edit"),   
    path("calibration/<int:pk>/delete/", views.calibration_delete, name="calibration_delete"), 
    path("calibration/export-excel/",views.calibration_export_excel,name="calibration_export_excel", ),


    path("instruments/", views.instrument_list, name="instrument_list"),
    path("instruments/add/", views.instrument_create, name="instrument_create"),
    path("instruments/<int:pk>/edit/", views.instrument_edit, name="instrument_edit"),
    path("instruments/<int:pk>/delete/", views.instrument_delete, name="instrument_delete"),



    path("fg-product-qc-status/", views.fg_qc_status_list, name="fg_qc_status_list",),
    path("fg-product-qc-status/add/",views.fg_qc_status_create, name="fg_qc_status_create", ),
    path( "fg-product-qc-status/<int:pk>/edit/",views.fg_qc_status_update, name="fg_qc_status_update", ),
    path("fg-product-qc-status/<int:pk>/delete/", views.fg_qc_status_delete, name="fg_qc_status_delete"),
    path("fg-product-qc-status/export/",  views.fg_qc_status_export_excel, name="fg_qc_status_export", ),
    
    
    
    
    path("instrument-occupancy/add/", views.instrument_occupancy_create, name="instrument_occupancy_create",),
    path("instrument-occupancy/edit/", views.instrument_occupancy_edit, name="instrument_occupancy_edit"),
    path( "instrument-occupancy/<int:pk>/delete/", views.instrument_occupancy_delete,name="instrument_occupancy_delete",),
    path("instrument-occupancy/",views.instrument_occupancy_list,name="instrument_occupancy_list",),
    path("instrument-occupancy/export-excel/",  views.instrument_occupancy_export_excel, name="instrument_occupancy_export_excel",),


    path("qc-powerbi-dashboard/", views.qc_powerbi_dashboard, name="qc_powerbi_dashboard"),
]
