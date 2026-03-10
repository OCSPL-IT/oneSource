# accounts_budget/context_processors.py
from .models import UserBudgetCategoryAccess, BudgetCategory


def budget_access_flags(request):
    if not request.user.is_authenticated:
        return {}

    # ✅ superuser: allow everything
    if request.user.is_superuser:
        return {
            "show_department_forms": True,
            "can_view_budget_production": True,
            "can_view_budget_safety": True,
            "can_view_budget_environment": True,
            "can_view_budget_qaqc": True,
            "can_view_budget_engineering": True,
            "can_view_budget_utility": True,
            "can_view_budget_admin": True,
            "can_view_budget_hr": True,
            "can_view_budget_rd": True,
            "can_view_budget_logistic": True,
            "can_view_budget_fin_accounts": True,
            "can_view_budget_sales": True,
            "can_view_budget_steam": True,
            "can_view_budget_electricity": True,
            "can_view_budget_rm": True,
            "can_view_budget_emp_oc": True,
            "can_view_budget_emp_contract": True,
        }

    # ✅ get all allowed categories for this user (can_view=True)
    allowed = set(
        UserBudgetCategoryAccess.objects.filter(
            user=request.user,
            can_view=True
        ).values_list("category", flat=True)
    )

    # ✅ show heading only if user has any one of these department forms
    forms_cats = {
        BudgetCategory.SAFETY,
        BudgetCategory.ENVIRONMENT,
        BudgetCategory.QAQC,
        BudgetCategory.ENGINEERING,
        BudgetCategory.UTILITY,
        BudgetCategory.ADMIN,
        BudgetCategory.HR,
        BudgetCategory.RD,
        BudgetCategory.LOGISTIC,
        BudgetCategory.FIN_ACCTS,
        BudgetCategory.SALES,
        BudgetCategory.STEAM,
        BudgetCategory.ELECTRICITY,
        BudgetCategory.RM,
        BudgetCategory.EMP_OC,
        BudgetCategory.EMP_CONTRACT,
        # add BudgetCategory.PRODUCTION here only if you want it under Department Forms
    }

    return {
        "show_department_forms": any(c in allowed for c in forms_cats),

        # ✅ keep your original variable
        "can_view_budget_production": (BudgetCategory.PRODUCTION in allowed),

        # ✅ other categories for sidebar checks
        "can_view_budget_safety": (BudgetCategory.SAFETY in allowed),
        "can_view_budget_environment": (BudgetCategory.ENVIRONMENT in allowed),
        "can_view_budget_qaqc": (BudgetCategory.QAQC in allowed),
        "can_view_budget_engineering": (BudgetCategory.ENGINEERING in allowed),
        "can_view_budget_utility": (BudgetCategory.UTILITY in allowed),
        "can_view_budget_admin": (BudgetCategory.ADMIN in allowed),
        "can_view_budget_hr": (BudgetCategory.HR in allowed),
        "can_view_budget_rd": (BudgetCategory.RD in allowed),
        "can_view_budget_logistic": (BudgetCategory.LOGISTIC in allowed),
        "can_view_budget_fin_accounts": (BudgetCategory.FIN_ACCTS in allowed),
        "can_view_budget_sales": (BudgetCategory.SALES in allowed),
        # legacy
        "can_view_budget_steam": (BudgetCategory.STEAM in allowed),
        "can_view_budget_electricity": (BudgetCategory.ELECTRICITY in allowed),
        "can_view_budget_rm": (BudgetCategory.RM in allowed),
        "can_view_budget_emp_oc": (BudgetCategory.EMP_OC in allowed),
        "can_view_budget_emp_contract": (BudgetCategory.EMP_CONTRACT in allowed),
    }
