
from django.urls import path,include
from STORE import views


urlpatterns = [
    path('search_supplier/', views.search_supplier, name='search_supplier'),
    path('search_item/', views.search_item, name='search_item'),
    path('add_vehicle/', views.add_vehicle, name='add_vehicle'),
    path('vehicle_list/', views.vehicle_list, name='vehicle_list'), 
    path('vehicle/edit/<int:vehicle_id>/', views.edit_vehicle, name='edit_vehicle'),
    path('vehicle/delete/<int:vehicle_id>/', views.delete_vehicle, name='delete_vehicle'),
    path('vehicle/view/<int:vehicle_id>/', views.view_vehicle, name='view_vehicle'),
    path('vehicles/download/', views.vehicle_download_excel, name='vehicle_download_excel'),
    path('vehicle-chart-report/', views.vehicle_chart_report, name='vehicle_chart_report'),
    path('materials/',            views.material_list,   name='material-list'),
    path('materials/new/',        views.material_create, name='material-create'),
    path('materials/<int:pk>/',   views.material_detail, name='material-detail'),
    path('materials/<int:pk>/edit/',   views.material_edit,   name='material-edit'),
    path('materials/<int:pk>/delete/', views.material_delete, name='material-delete'),

       # Store_Rm
    path("racks/",                              views.rack_dashboard,    name="rack_dashboard"),
    path("racks/sync/",                         views.sync_page,         name="rack_sync"),

    # specific FIRST
    path("racks/allocate/by-date/",             views.grn_allocate_date, name="grn_allocate_date"),

    # dynamic after (use slug for safety)
    path("racks/allocate/<slug:erp_line_id>/",  views.grn_allocate,      name="grn_allocate"),
    path("racks/consume/",                      views.rack_consume_group,name="rack_consume_group"),
    path("racks/transfer/",                     views.rack_transfer,     name="rack_transfer"),


    path("racks/issue/apply/<slug:erp_line_id>/", views.issue_apply,     name="issue_apply"),
    path("racks/pallet-options/",               views.pallet_options,    name="pallet_options"),


    path("dispatch-plan-vs-actual/", views.dispatch_plan_vs_actual, name="dispatch_plan_vs_actual"),

    path("inv-ageing-drill/", views.inv_ageing_drill_report, name="inv_ageing_drill_report"),
    path("inv-ageing-drill/sync/", views.inv_ageing_sync, name="inv_ageing_sync"),

    path("reports/pending-grn/", views.pending_grn_report, name="pending_grn_report"),
]
