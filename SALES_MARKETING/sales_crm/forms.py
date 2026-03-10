from django import forms
from .models import SalesLead, LeadFollowUp

class SalesLeadForm(forms.ModelForm):
    class Meta:
        model = SalesLead
        fields = [
            "name", "company", "phone", "email", "city", "state",
            "status", "source", "expected_value", "assigned_to", "remarks",
        ]
        widgets = {
            "remarks": forms.Textarea(attrs={"rows": 2}),
        }

class LeadFollowUpForm(forms.ModelForm):
    class Meta:
        model = LeadFollowUp
        fields = ["note", "next_date"]
        widgets = {
            "note": forms.Textarea(attrs={"rows": 3}),
        }




from django import forms
from .models import CustomerVisit, Customer, Product, Industry, SalesPerson


class CustomerVisitForm(forms.ModelForm):

    new_customer = forms.CharField(required=False)
    new_product = forms.CharField(required=False)
    new_industry = forms.CharField(required=False)
    new_sales_person = forms.CharField(required=False)

    class Meta:
        model = CustomerVisit
        fields = "__all__"
        widgets = {
            "visit_date": forms.DateInput(attrs={"type": "date"}),

            "remark": forms.Textarea(attrs={
                "rows": 1,
                "style": "resize:vertical;"
            }),

            "quantity": forms.NumberInput(attrs={
                "class": "w-full bg-white border-2 border-slate-400 rounded-lg px-4 py-2 text-slate-800 text-sm \
                        focus:border-blue-600 focus:ring-2 focus:ring-blue-200 focus:outline-none shadow-sm",
                "placeholder": "Enter quantity"
            }),
        }

    # ✅ MOVE THIS INSIDE THE CLASS
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Remove required validation from dropdowns
        self.fields['customer'].required = False
        self.fields['product'].required = False
        self.fields['industry'].required = False
        self.fields['sales_person'].required = False

        # Show only existing values used in CustomerVisit
        self.fields['customer'].queryset = Customer.objects.filter(
            customervisit__isnull=False
        ).distinct()

        self.fields['product'].queryset = Product.objects.filter(
            customervisit__isnull=False
        ).distinct()

        self.fields['industry'].queryset = Industry.objects.filter(
            customervisit__isnull=False
        ).distinct()

        self.fields['sales_person'].queryset = SalesPerson.objects.filter(
            customervisit__isnull=False
        ).distinct()

    def clean(self):
        cleaned_data = super().clean()

        customer = cleaned_data.get("customer")
        new_customer = cleaned_data.get("new_customer")

        product = cleaned_data.get("product")
        new_product = cleaned_data.get("new_product")

        industry = cleaned_data.get("industry")
        new_industry = cleaned_data.get("new_industry")

        sales_person = cleaned_data.get("sales_person")
        new_sales_person = cleaned_data.get("new_sales_person")

        if not customer and not new_customer:
            raise forms.ValidationError("Please select or add a Customer.")

        if not product and not new_product:
            raise forms.ValidationError("Please select or add a Product.")

        if not industry and not new_industry:
            raise forms.ValidationError("Please select or add an Industry.")

        if not sales_person and not new_sales_person:
            raise forms.ValidationError("Please select or add a Sales Person.")

        return cleaned_data




from django import forms
from .models import FollowUp


class FollowUpForm(forms.ModelForm):
    class Meta:
        model = FollowUp
        fields = "__all__"   # show all fields

        widgets = {
            # Date Field
            "followup_date": forms.DateInput(attrs={
                "type": "date",
                "class": "w-full bg-white border-2 border-slate-400 rounded-lg px-4 py-2 text-slate-800 text-sm \
                          focus:border-blue-600 focus:ring-2 focus:ring-blue-200 focus:outline-none shadow-sm"
            }),

            # Notes Field (smaller)
            "notes": forms.Textarea(attrs={
                "rows": 3,
                "class": "w-full bg-white border-2 border-slate-400 rounded-lg px-4 py-2 text-slate-800 text-sm \
                          focus:border-blue-600 focus:ring-2 focus:ring-blue-200 focus:outline-none shadow-sm resize-none",
                "placeholder": "Enter follow-up notes..."
            }),

        }

    # Apply Tailwind styling to all other fields automatically
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for field_name, field in self.fields.items():
            if field_name not in self.Meta.widgets:
                field.widget.attrs.update({
                    "class": "w-full bg-white border-2 border-slate-400 rounded-lg px-4 py-2 text-slate-800 text-sm \
                              focus:border-blue-600 focus:ring-2 focus:ring-blue-200 focus:outline-none shadow-sm"
                })




from .models import Task
class TaskForm(forms.ModelForm):
    class Meta:
        model = Task
        fields = [
            "title",
            "description",
            "assigned_to",
            "due_date",
            "priority",
            "status",
        ]
        widgets = {
            "due_date": forms.DateInput(attrs={
                "type": "date"
            }),
            "title": forms.TextInput(attrs={
                "class": "w-full bg-white border-2 border-slate-400 rounded-lg px-4 py-2 text-slate-800 text-sm \
                        focus:border-blue-600 focus:ring-2 focus:ring-blue-200 focus:outline-none \
                        placeholder-gray-400 shadow-sm",
                "placeholder": "Enter task title..."
            }),
            "description": forms.Textarea(attrs={
                "rows": 4,
                "class": "w-full bg-white border-2 border-slate-400 rounded-lg px-4 py-2 text-slate-800 text-sm \
                        focus:border-blue-600 focus:ring-2 focus:ring-blue-200 focus:outline-none \
                        placeholder-gray-400 shadow-sm resize-none",
                "placeholder": "Enter task description..."
            }),
        }