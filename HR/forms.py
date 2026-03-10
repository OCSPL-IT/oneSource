from django import forms
from .models import *

class HRForm(forms.ModelForm):
    class Meta:
        model = HR
        fields = [
            'date',
            'permanent_employees',
            'contract_labour_production',
            'contract_labour_others',
            'total_employee',
            'hrs',
            'total_no_of_hrs'
        ]
        widgets = {
            'date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'permanent_employees': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'contract_labour_production': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'contract_labour_others': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'total_employee': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
            'hrs': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
            'total_no_of_hrs': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
        }
    
    def clean(self):
        cleaned_data = super().clean()
        # Retrieve values and default to 0 if missing
        permanent_employees = cleaned_data.get('permanent_employees') or 0
        contract_labour_production = cleaned_data.get('contract_labour_production') or 0
        contract_labour_others = cleaned_data.get('contract_labour_others') or 0
        
        # Compute total_employee as the sum of employee types
        total_employee = permanent_employees + contract_labour_production + contract_labour_others
        cleaned_data['total_employee'] = total_employee
        
        # Set hrs to 8
        hrs = 8
        cleaned_data['hrs'] = hrs
        
        # Compute total_no_of_hrs as hrs * total_employee
        cleaned_data['total_no_of_hrs'] = hrs * total_employee
        
        return cleaned_data






# ========================================================================================================

# Common Tailwind classes
BASE_INPUT_CLASS = (
    "block w-full rounded-md border border-gray-300 px-3 py-2 text-sm "
    "focus:outline-none focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500"
)

BASE_SELECT_CLASS = (
    "block w-full rounded-md border border-gray-300 px-3 py-2 text-sm bg-white "
    "focus:outline-none focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500"
)

BASE_TEXTAREA_CLASS = (
    "block w-full rounded-md border border-gray-300 px-3 py-2 text-sm "
    "focus:outline-none focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500"
)


# ------------------------------------------------------------------
# 1) HR FORM  – used when HR creates / edits basic employee details
# ------------------------------------------------------------------
class EmployeeJoiningForm(forms.ModelForm):
    class Meta:
        model = EmployeeJoining
        fields = [
            # basic details
            "employee_name",
            "gender",
            "date_of_birth",
            "company",
            "location",
            "department",
            "designation",
            "date_of_joining",
            "date_of_confirmation",
            "employee_id",
            # access / infra (HR can propose requirements)
            "Biomatric_enrollment",
            "attendance_card",
            "smart_office_entry",
            "mobile_phone",
            "sim_card",
            "telephone_extension",
            "computer",
            "erp_login",
            "office_365_id",
            "email_id",
            "sharepoint_site",
            "specific_folder_rights",
            "remark",
        ]

        widgets = {
            # --- Basic details ---
            "employee_name": forms.TextInput(
                attrs={"class": BASE_INPUT_CLASS, "placeholder": "Employee Name"}
            ),
            "gender": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "date_of_birth": forms.DateInput(
                attrs={"type": "date", "class": BASE_INPUT_CLASS}
            ),
            "company": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "location": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "department": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "designation": forms.TextInput(
                attrs={"class": BASE_INPUT_CLASS, "placeholder": "Designation"}
            ),
            "date_of_joining": forms.DateInput(
                attrs={"type": "date", "class": BASE_INPUT_CLASS}
            ),
            "date_of_confirmation": forms.DateInput(
                attrs={"type": "date", "class": BASE_INPUT_CLASS}
            ),
            "employee_id": forms.TextInput(
                attrs={"class": BASE_INPUT_CLASS, "placeholder": "Employee ID"}
            ),

            # --- Access / infra ---
            "Biomatric_enrollment": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "attendance_card": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "smart_office_entry": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "mobile_phone": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "sim_card": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "telephone_extension": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "computer": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "erp_login": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "office_365_id": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "email_id": forms.EmailInput(
                attrs={"class": BASE_INPUT_CLASS, "placeholder": "official.email@ocspl.com"}
            ),
            "sharepoint_site": forms.TextInput(
                attrs={"class": BASE_INPUT_CLASS, "placeholder": "SharePoint Site / URL"}
            ),
            "specific_folder_rights": forms.Textarea(
                attrs={
                    "class": BASE_TEXTAREA_CLASS,
                    "rows": 2,
                    "placeholder": "Mention specific network / server folder rights, if any",
                }
            ),
            "remark": forms.Textarea(
                attrs={"class": BASE_TEXTAREA_CLASS, "rows": 2, "placeholder": "Any Remark"}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["date_of_confirmation"].required = False
        self.fields["sharepoint_site"].required = False
        self.fields["specific_folder_rights"].required = False
        self.fields["remark"].required = False


# ------------------------------------------------------------------
# 2) IT USER FORM – IT fills / adjusts infra details only
# ------------------------------------------------------------------
class EmployeeJoiningITForm(forms.ModelForm):
    class Meta:
        model = EmployeeJoining
        fields = [
            "erp_login",
            "office_365_id",
            "sharepoint_site",
            "specific_folder_rights",
            "remark",
        ]
        widgets = {
            "erp_login": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "office_365_id": forms.Select(attrs={"class": BASE_SELECT_CLASS}),
            "sharepoint_site": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "specific_folder_rights": forms.Textarea(
                attrs={"class": BASE_TEXTAREA_CLASS, "rows": 2}
            ),
            "remark": forms.Textarea(
                attrs={"class": BASE_TEXTAREA_CLASS, "rows": 2}
            ),
        }


# ------------------------------------------------------------------
# 3) APPROVER FORM – Approver only sets approval remark
# ------------------------------------------------------------------
class EmployeeJoiningApprovalForm(forms.ModelForm):
    class Meta:
        model = EmployeeJoining
        fields = ["approval_remark"]
        widgets = {
            "approval_remark": forms.Textarea(
                attrs={
                    "class": BASE_TEXTAREA_CLASS,
                    "rows": 3,
                    "placeholder": "Approval / rejection remark",
                }
            )
        }

