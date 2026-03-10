# ACCOUNTS/Budget/urls.py
from django.urls import path
from . import views

app_name = "accounts_budget"

urlpatterns = [
    # ─────────────────────────
    # HOME / PLANS  (unchanged)
    # ─────────────────────────
    path("", views.budget_home, name="budget_home"),
    path("plans/new/", views.budget_plan_create, name="budget_plan_create"),

    # Sidebar-friendly category route (current active plan)
    # NOTE: use <slug:> so ENVIRONMENT / QAQC etc are safe, and matches your view signature (category: str)
    path("dept/<slug:category>/", views.budget_category_current, name="budget_category_current"),

    # MAIN EDIT PAGE (keeps existing pattern /accounts/budgets/<plan_id>/<category>/)
    path("<int:plan_id>/<slug:category>/", views.budget_category_edit, name="budget_category_edit"),

    # ─────────────────────────
    # PRODUCTION BUDGET (unchanged)
    # ─────────────────────────
    path("production/", views.production_budget_home, name="production_budget_home"),
    path("production/new/", views.production_budget_create, name="production_budget_create"),
    path("production/<int:plan_id>/new/", views.production_budget_create, name="production_budget_create_plan"),
    path("production/<int:budget_id>/edit/", views.production_budget_edit, name="production_budget_edit"),
    path("production/<int:budget_id>/delete/", views.production_budget_delete, name="production_budget_delete"),

    # ─────────────────────────
    # PRODUCTION BOM MASTER (unchanged)
    # ─────────────────────────
    path("bom/", views.production_bom_list, name="production_bom_list"),
    path("bom/new/", views.production_bom_create, name="production_bom_create"),
    path("bom/<int:bom_id>/", views.production_bom_edit, name="production_bom_edit"),
    path("production-bom/<int:bom_id>/view/", views.production_bom_view, name="production_bom_view"),
    path("bom/<int:bom_id>/json/", views.production_bom_json, name="production_bom_json"),

    # JSON helper (keep, but remove redundant "budgets/" prefix to avoid double nesting)
    path("bom/fg-inputs/", views.production_bom_inputs_for_fg_json, name="production_bom_inputs_for_fg_json"),
    path("production-bom/fg-inputs/", views.production_bom_inputs_for_fg_json, name="production_bom_inputs_for_fg_json"),

    # ─────────────────────────
    # SALES BUDGET (unchanged)
    # ─────────────────────────
    path("sales/", views.sales_budget_home, name="sales_budget_home"),
    path("sales/edit/", views.sales_budget_edit, name="sales_budget_edit"),

    # ─────────────────────────
    # RMC BUDGET (unchanged)
    # ─────────────────────────
    path("rmc/", views.rmc_budget_home, name="rmc_budget_home"),
    path("rmc/new/", views.rmc_budget_create, name="rmc_budget_create"),
    path("rmc/edit/", views.rmc_budget_edit, name="rmc_budget_edit"),
    path("rmc/report.xlsx", views.rmc_budget_report_excel, name="rmc_budget_report_excel"),
    path("rmc/qty-preview/", views.rmc_qty_preview, name="rmc_qty_preview"),

    # ─────────────────────────
    # CAPTIVE CONSUMPTION BUDGET (unchanged)
    # ─────────────────────────
    path("captive/", views.captive_consumption_home, name="captive_consumption_home"),
    path("captive/edit/", views.captive_consumption_edit, name="captive_consumption_edit"),

    # ─────────────────────────
    # APPROVALS (Maker-Checker)  ✅ updated to preserve flow + allow ":" in scope
    # ─────────────────────────
    path("approvals/", views.budget_approvals_inbox, name="budget_approvals_inbox"),

    # IMPORTANT:
    # - model token is like "accounts_budget.BudgetPlan" => contains "."
    # - scope is like "BUDGET:ENVIRONMENT" => contains ":"
    # Use <path:...> for both so reverse() never breaks.
    path("mc/approve/<path:model>/<int:pk>/<path:scope>/", views.mc_approve_view, name="mc_approve"),
    path("mc/reject/<path:model>/<int:pk>/<path:scope>/", views.mc_reject_view, name="mc_reject"),
    path("mc/reopen/<path:model>/<int:pk>/<path:scope>/", views.mc_reopen_view, name="mc_reopen"),

    # ✅ Checker step (same reasoning: scope may contain ":" -> use <path:scope>)
    path("mc/check/<path:model>/<int:pk>/<path:scope>/", views.mc_check_view, name="mc_check_view"),

    # ─────────────────────────
    # LEDGER MASTER (Excel-driven) (unchanged)
    # ─────────────────────────
    path("coa/gl/", views.gl_account_list, name="gl_account_list"),
    path("coa/gl/new/", views.gl_account_create, name="gl_account_create"),
    path("coa/gl/<int:pk>/edit/", views.gl_account_edit, name="gl_account_edit"),

    # ✅ NEW: Delete GL
    path("coa/gl/<int:pk>/delete/", views.gl_account_delete, name="gl_account_delete"),

    # Excel master upload
    path("coa/budget-heads/upload/", views.gl_excel_upload, name="budget_heads_upload"),

    # AJAX endpoints
    path("ajax/budget-departments/", views.ajax_budget_departments, name="ajax_budget_departments"),
    path("ajax/budget-units/", views.ajax_budget_units, name="ajax_budget_units"),
    path("ajax/budget-heads/", views.ajax_budget_heads, name="ajax_budget_heads"),

    # Optional ledger search endpoint
    path("gl/search/", views.gl_search, name="gl_search"),

    path("bom/fg-lookup/", views.production_bom_fg_lookup_json, name="production_bom_fg_lookup_json"),

    path("production-bom/<int:pk>/delete/", views.production_bom_delete, name="production_bom_delete"),
    path("mc/<str:model>/<int:pk>/<str:scope>/check/", views.mc_check, name="mc_check"),

    path("production-bom/material-lookup/",views.production_bom_material_lookup_json,name="production_bom_material_lookup_json"),


    # ─────────────────────────
    # PACKING MATERIAL BUDGET (NEW)
    # ─────────────────────────
    path("packing/", views.packing_budget_home, name="packing_budget_home"),
    path("packing/products/", views.packing_products, name="packing_products"),
    path("packing/inputs/", views.packing_inputs, name="packing_inputs"),

    # ✅ Shortcut URL (Packing Form)
    path("packing/form/", views.packing_inputs, name="packing_form"),

    path("packing/summary/", views.packing_summary, name="packing_summary"),

    # ─────────────────────────
    # PACKING MATERIAL MASTER (NEW)
    # ─────────────────────────
    path("packing/master/", views.packing_material_master_list, name="packing_master_list"),
    path("packing/master/new/", views.packing_material_master_create, name="packing_master_create"),
    path("packing/master/<int:pk>/edit/", views.packing_material_master_edit, name="packing_master_edit"),
    path("packing/master/json/", views.packing_material_master_json, name="packing_master_json"),

]
