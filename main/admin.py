# main/admin.py
from django.contrib import admin, messages
from django.contrib.admin.sites import NotRegistered
from django.contrib.auth import get_user_model
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.core.exceptions import PermissionDenied
from django.core.mail import send_mail
from django.shortcuts import redirect
from django.urls import path, reverse
from django.utils.crypto import get_random_string
from django.conf import settings

# --- Admin site titles ---
admin.site.site_header = "oneSource Admin Panel"
admin.site.site_title = "oneSource Admin Portal"
admin.site.index_title = "Welcome to the oneSource Admin Dashboard"

User = get_user_model()


class UserAdmin(BaseUserAdmin):
    """
    Adds two buttons to the user change page:
      • Reset password  -> sets a random temporary password (and tries to email it)
      • Set password…   -> opens Django's built-in password form
    """
    # POINTS TO: templates/main/change_form_with_reset.html
    change_form_template = "main/change_form_with_reset.html"

    def get_urls(self):
        urls = super().get_urls()
        app_label, model_name = self.model._meta.app_label, self.model._meta.model_name
        my_urls = [
            path(
                "<path:object_id>/reset-password/",
                self.admin_site.admin_view(self.reset_password_view),
                name=f"{app_label}_{model_name}_reset_password",
            ),
        ]
        return my_urls + urls

    def reset_password_view(self, request, object_id, *args, **kwargs):
        if not (request.user.is_superuser or request.user.has_perm("auth.change_user")):
            raise PermissionDenied("You do not have permission to reset passwords.")

        obj = self.get_object(request, object_id)
        if obj is None:
            messages.error(request, "User not found.")
            return redirect(
                f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_changelist"
            )

        # Generate & set a 12-char temporary password
        temp_pw = get_random_string(12)
        obj.set_password(temp_pw)
        obj.save()

        emailed = False
        if getattr(obj, "email", None):
            try:
                send_mail(
                    subject="Your oneSource password has been reset",
                    message=(
                        f"Hello {obj.get_full_name() or obj.get_username()},\n\n"
                        f"Your oneSource account password has been reset by an administrator.\n"
                        f"Temporary password: {temp_pw}\n\n"
                        f"Please sign in and change your password immediately."
                    ),
                    from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None),
                    recipient_list=[obj.email],
                    fail_silently=False,
                )
                emailed = True
            except Exception:
                emailed = False

        if emailed:
            messages.success(
                request,
                f"Temporary password set and emailed to {obj.email}. Temp password: {temp_pw}",
            )
        else:
            messages.warning(
                request,
                f"Temporary password set. (Email not sent.) Temp password: {temp_pw}",
            )

        return redirect(
            reverse(
                f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_change",
                args=[obj.pk],
            )
        )

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        extra_context = extra_context or {}
        if object_id:
            app_label, model_name = self.model._meta.app_label, self.model._meta.model_name
            extra_context["reset_password_url"] = reverse(
                f"admin:{app_label}_{model_name}_reset_password", args=[object_id]
            )
            extra_context["password_change_url"] = reverse(
                f"admin:{app_label}_{model_name}_password_change", args=[object_id]
            )
        return super().changeform_view(request, object_id, form_url, extra_context)


# Re-register User with our customized admin
try:
    admin.site.unregister(User)
except NotRegistered:
    pass
admin.site.register(User, UserAdmin)
