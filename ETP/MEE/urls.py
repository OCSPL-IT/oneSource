from django.urls import path
from . import views

app_name = "mee"

urlpatterns = [
    path("reading/", views.mee_reading_form, name="mee_reading_form"),
    path("reading/list/", views.mee_reading_list, name="mee_reading_list"),
    path("reading/detail/<str:reading_date>/",views.mee_reading_detail,name="mee_reading_detail",),
    path("reading/export/",views.mee_reading_export_xlsx,name="mee_reading_export_xlsx",),
    path("reading/<str:reading_date>/",views.mee_reading_edit,name="mee_reading_edit",),
    path("reading/<str:reading_date>/delete/", views.mee_reading_delete,name="mee_reading_delete",),


         # ---------------- ATFD ----------------
    path("atfd/reading/", views.atfd_entry, name="atfd_reading_form"),
    path("atfd/reading/list/", views.atfd_reading_list, name="atfd_reading_list"),
    path("atfd/reading/detail/<str:reading_date>/", views.atfd_reading_detail, name="atfd_reading_detail"),
    path("atfd/readings/export/", views.atfd_reading_export_xlsx, name="atfd_reading_export_xlsx"),
    path("atfd/reading/<str:reading_date>/", views.atfd_reading_edit, name="atfd_reading_edit"),
    path("atfd/reading/<str:reading_date>/delete/", views.atfd_reading_delete, name="atfd_reading_delete"),
]
