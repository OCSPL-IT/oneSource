# ETP/urls.py
from django.urls import path
from . import views

urlpatterns = [
    # Create new reading
    path("bioreactor-daily-reading/new/",views.bioreactor_daily_reading_form,name="bioreactor_daily_reading_create", ),
    # Edit existing reading
    path("bioreactor-daily-reading/<int:pk>/",views.bioreactor_daily_reading_form, name="bioreactor_daily_reading_edit",),
    path("bioreactor-daily-reading/",views.bioreactor_daily_reading_list, name="bioreactor_daily_reading_list",),
    path("bioreactor-daily-reading/<int:pk>/detail/", views.bioreactor_daily_reading_detail,name="bioreactor_daily_reading_detail",),
    path("bioreactor-daily-reading/<int:pk>/delete/", views.bioreactor_daily_reading_delete, name="bioreactor_daily_reading_delete", ),
    path("bioreactor-daily-reading/excel/",views.bioreactor_daily_reading_excel, name="bioreactor_daily_reading_excel",),

]
