from django.contrib import admin
from django import forms
from .models import HRBudgetPlan
import datetime

def get_financial_year_choices(num_years=2):
    now = datetime.date.today()
    start_year = now.year if now.month > 3 else now.year - 1
    years = []
    for i in range(num_years):
        y1 = start_year + i
        y2 = str(y1 + 1)[-2:]
        years.append((f"{y1}-{y2}", f"{y1}-{y2}"))
    return years

class HRBudgetPlanAdminForm(forms.ModelForm):
    year = forms.ChoiceField(choices=get_financial_year_choices())

    class Meta:
        model = HRBudgetPlan
        fields = '__all__'

class HRBudgetPlanAdmin(admin.ModelAdmin):
    form = HRBudgetPlanAdminForm
    list_display = ['year', 'category', 'plan_amount']

admin.site.register(HRBudgetPlan, HRBudgetPlanAdmin)
