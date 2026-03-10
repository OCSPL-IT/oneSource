from django import forms
from django_select2.forms import Select2Widget
from .models import Downtime

# Static dropdown options for Downtime Category
DOWNTIME_CATEGORY_CHOICES = [
    ('RM/PM', 'RM/PM'),
    ('Process related issues', 'Process related issues'),
    ('pipe line chocking', 'pipe line chocking'),
    ('ETP related issues', 'ETP related issues'),
    ('non availability of people', 'non availability of people'),
    ('Machinery breakdown', 'Machinery breakdown'),
    ('Instrument', 'Instrument'),
    ('Analysis from QC', 'Analysis from QC'),
    ('Utility', 'Utility'),
    ('Electrical', 'Electrical'),
    ('Modifications', 'Modifications'),
    ('Breakdown Maintenance', 'Breakdown Maintenance'),
    ('General Maintenance', 'General Maintenance'),
    ('Modification Maintenance', 'Modification Maintenance'),
    ('Preventive Maintenance', 'Preventive Maintenance'),
    ('Repair Maintenance', 'Repair Maintenance'),
    ('Support Production Maintenance', 'Support Production Maintenance'),
]

# Static dropdown options for Department
DEPARTMENT_CHOICES = [
    ('ACCOUNTS', 'ACCOUNTS'),
    ('BOILER UTILITY', 'BOILER UTILITY'),
    ('ELECTRICAL', 'ELECTRICAL'),
    ('EHS', 'EHS'),
    ('HR ADMIN', 'HR ADMIN'),
    ('INSTRUMENT', 'INSTRUMENT'),
    ('IT', 'IT'),
    ('MAINTENANCE', 'MAINTENANCE'),
    ('OPERATION', 'OPERATION'),
    ('PRODUCTION', 'PRODUCTION'),
    ('QA/QC', 'QA/QC'),
    ('SECURITY', 'SECURITY'),
    ('STORE', 'STORE'),
    ('ETP', 'ETP'),
]

idle_select=[
    ('Yes','Yes'),
    ('No','No')
]

select_block=[
    ('Block-A','Block-A'),
    ('Block-B','Block-B'),
    ('Block-C','Block-C'),
    ('Block-D','Block-D'),
    ('Block-E','Block-E')
]

class DowntimeForm(forms.ModelForm):
    """
    A ModelForm that uses django-select2 for eqpt_id, product_name, batch_no,
    and static <select> dropdowns for downtime_category and downtime_dept.
    """
    eqpt_id = forms.CharField(
        label="Equipment ID",
        widget=Select2Widget(
            attrs={
                'data-url': '/search_equipment/',  # AJAX endpoint
                'data-placeholder': 'Search Equipment ID',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }
        )
    )
    product_name = forms.CharField(
        required=False,  # Allow empty value for Idle = Yes
        label="Product Name",
        widget=Select2Widget(
            attrs={
                'data-url': '/search_product/',
                'data-placeholder': 'Search Product Name',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }
        )
    )
    stage_name = forms.CharField(
        required=False,  # Allow empty value for Idle = Yes
        label="Stage Name",
        widget=Select2Widget(
            attrs={
                'data-url': '/search_product/',
                'data-placeholder': 'Search Stage Name',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }
        )
    )
    batch_no = forms.CharField(
        required=False,  # Allow empty value for Idle = Yes
        label="Batch No",
        widget=Select2Widget(
            attrs={
                'data-url': '/search_batch/',
                'data-placeholder': 'Search Batch No',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }
        )
    )
    downtime_category = forms.ChoiceField(
        label="Downtime Category",
        choices=[('', 'Select Downtime Category')] + DOWNTIME_CATEGORY_CHOICES,
        required=False,
        widget=forms.Select(
            attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}
        )
    )
    downtime_dept = forms.ChoiceField(
        label="Downtime Department",
        choices=[('', 'Select Department')] + DEPARTMENT_CHOICES,
        required=False,
        widget=forms.Select(
            attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}
        )
    )

    idle = forms.ChoiceField(
        label="Idle",
        choices=[('', 'Select Idle')] + idle_select,
        required=True,
        widget=forms.Select(
            attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}
        )
    )
    block = forms.ChoiceField(
        label="Block",
        choices=[('', 'Select Block')] + select_block,
        required=False,
        widget=forms.Select(
            attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}
        )
    )

    class Meta:
        model = Downtime
        fields = [
            'date',
            'idle',
            'eqpt_id',
            'eqpt_name',
            'product_name',
            'stage_name',
            'product_code',
            'batch_no',
            'start_date',
            'end_date',
            'start_time',
            'end_time',
            'total_duration',
            'downtime_dept',
            'downtime_category',
            'block',
            'reason',
            'bom_qty',
            'bct',
            'loss',
        ]
        widgets = {
            'date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
             'eqpt_name': forms.TextInput(attrs={
                 
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'readonly': 'readonly'
            }),
            'product_code': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
            'start_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'start-time w-full p-2 border border-gray-300 rounded-lg'
            }),
            'end_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'end-time  w-full p-2 border border-gray-300 rounded-lg'
            }),
            'total_duration': forms.NumberInput(attrs={
                'class': 'total-duration  w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
            'reason': forms.Textarea(attrs={
                'rows': 3,
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'start_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'start-date  w-full p-2 border border-gray-300 rounded-lg'
            }),
            'end_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'end-date w-full p-2 border border-gray-300 rounded-lg'
            }),
            'bom_qty': forms.NumberInput(attrs={
                'class': 'bomqty-value  w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
            'bct': forms.NumberInput(attrs={
                'class': 'bct-value w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
            'loss': forms.NumberInput(attrs={
                'class': 'loss-value w-full p-2 border border-gray-300 rounded-lg bg-gray-50',
                'readonly': 'readonly'
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['product_code'].required = False
        self.fields['bom_qty'].required = False
        self.fields['bct'].required = False
        self.fields['loss'].required = False
    
    def clean(self):
        cleaned_data = super().clean()
        # print("DEBUG: Entering clean() with cleaned_data =", cleaned_data)

        idle = cleaned_data.get('idle')
        dependent_fields = ['product_name', 'stage_name', 'product_code']

        if idle == "No":
            errors = {}
            for field in dependent_fields:
                value = cleaned_data.get(field)
                if value in [None, '']:
                    errors[field] = "This field is required when Idle is No."
            if errors:
                # print("DEBUG: Errors when Idle=No =>", errors)
                raise forms.ValidationError(errors)

        elif idle == "Yes":
            # If Idle is Yes, clear dependent fields
            cleaned_data['product_name'] = 'NA'
            cleaned_data['stage_name'] = 'NA'
            cleaned_data['product_code'] = 'NA'
            cleaned_data['batch_no'] = 'NA'
            cleaned_data['bom_qty'] = None
            cleaned_data['bct'] = None
            cleaned_data['loss'] = None

            # print("DEBUG: After clearing fields for Idle=Yes =>", cleaned_data)

        return cleaned_data


    