# accounts_budget/utils.py
from .models import UserBudgetCategoryAccess

def allowed_budget_categories(user, edit: bool = False):
    """
    Returns set of category values user can view/edit.
    """
    if not user.is_authenticated:
        return set()
    if user.is_superuser:
        return None  # means ALL allowed

    qs = UserBudgetCategoryAccess.objects.filter(user=user, can_view=True)
    if edit:
        qs = qs.filter(can_edit=True)

    return set(qs.values_list("category", flat=True))
