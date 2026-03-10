from django.urls import path
from . import views

app_name = "sales_crm"

urlpatterns = [
    path("", views.crm_home, name="home"),
    path("leads/", views.lead_list, name="lead_list"),
    path("leads/new/", views.lead_create, name="lead_create"),
    path("leads/<int:pk>/", views.lead_detail, name="lead_detail"),
    path("leads/<int:pk>/edit/", views.lead_edit, name="lead_edit"),
    path("leads/<int:pk>/followup/", views.lead_followup_add, name="lead_followup_add"),
    path("customer-visit/create/", views.create_customer_visit, name="customer_visit_create"),
    path("visit/add/", views.create_customer_visit, name="customer_visit_add"),
    path("visit/list/", views.customer_visit_list, name="customer_visit_list"),
    path("visit/edit/<int:pk>/", views.update_customer_visit, name="customer_visit_edit"),
    path("visit/delete/<int:pk>/", views.delete_customer_visit, name="customer_visit_delete"),
    path("followup/list/", views.followup_list, name="followup_list"),
    path("followup/add/<int:visit_id>/", views.followup_create, name="followup_add"),
    path("followup/edit/<int:pk>/", views.followup_update, name="followup_edit"),
    path("followup/delete/<int:pk>/", views.followup_delete, name="followup_delete"),
    path("task/list/", views.task_list, name="task_list"),
    path("task/add/<int:visit_id>/", views.task_create, name="task_add"),
    path("task/edit/<int:pk>/", views.task_update, name="task_edit"),
    path("task/delete/<int:pk>/", views.task_delete, name="task_delete"),
    
]