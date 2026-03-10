def auth_extras(request):
    """
    Adds user_groups (list[str]), is_superuser (bool), show_admin_panel (bool),
    and can_location_wise_stock (bool) to every template context.

    Safe for anonymous users (returns [] and False).
    """
    user = getattr(request, "user", None)

    groups = []
    is_super = False
    is_staff = False
    is_active = False
    show_admin_panel = False

    if user and user.is_authenticated:
        groups = list(user.groups.values_list("name", flat=True))
        is_super = bool(user.is_superuser)
        is_staff = bool(user.is_staff)
        is_active = bool(user.is_active)
        show_admin_panel = is_super or (is_staff and is_active)

    # ✅ Permission for Location wise Stock menu
    can_location_wise_stock = (
        bool(user and user.is_authenticated and user.has_perm("STORE.view_invageingpreview"))
    )

    return {
        "user_groups": groups,
        "is_superuser": is_super,
        "is_staff": is_staff,                 # optional but useful sometimes
        "show_admin_panel": show_admin_panel, # ✅ use this in templates
        "can_location_wise_stock": can_location_wise_stock,
    }
