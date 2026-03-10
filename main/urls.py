# main/urls.py

from django.urls import path
from . import views




urlpatterns = [
    # Redirect the root URL to the login page
    path('', views.LoginPage, name='root-login'), 

    # Give each path a unique and matching 'name'
    path('login/', views.LoginPage, name='userlogin'),
    path('logout/', views.User_logout, name='userlogout'),
    path('indexpage/', views.indexpage, name='indexpage'),
    
    # THE FIX IS HERE: The name must be 'signuppage' to match the template.
    path('signup/', views.Signup_Page, name='signuppage'), 

    path('users/online/', views.online_users, name='online_users'),
    path('users/activity/', views.login_activity, name='login_activity'),


    path("audit-log/", views.audit_log_list, name="audit_log_list"),
    path("audit-log/export/", views.audit_log_export_excel, name="audit_log_export_excel"),
]