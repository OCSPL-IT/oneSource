from django import forms
from django.utils import timezone
from .models import *
from django.forms import inlineformset_factory




class PhysicalLocationForm(forms.ModelForm):
    class Meta:
        model = Physical_Location
        fields = ['name']
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'w-full border border-gray-300 rounded px-3 py-2 mb-4',
                'placeholder': 'Enter location name'
            }),
        }
        labels = {
            'name': 'Location Name'
        }


PSL_MEMBER_CHOICES = [
    ('Mr.Pravin Patil', 'Mr.Pravin Patil'),
    ('Mr.Bhagavan Bhange', 'Mr.Bhagavan Bhange'),
    ('Mr.Vikas Kale', 'Mr.Vikas Kale'),
    ('Mr.Aanad Kanaka', 'Mr.Aanad Kanaka'),
    ('Mr.Raju Rathod', 'Mr.Raju Rathod'),
    ('Mr.Balasaheb Shendage', 'Mr.Balasaheb Shendage'),
    ('Mr.Suraj Shinde', 'Mr.Suraj Shinde'),
    ('Mr.Dattatraya Chavan', 'Mr.Dattatraya Chavan'),
    ('Mr.Navnath Shep', 'Mr.Navnath Shep'),
    ('Mr.Savala Kumbhar', 'Mr.Savala Kumbhar'),
    ('Mr.Ganesh Tadka', 'Mr.Ganesh Tadka'),
    ('Mr.Sachin Jadhav', 'Mr.Sachin Jadhav'),
    ('Mr.Somnath Asabe', 'Mr.Somnath Asabe'),
    ('Mr.Bhagywan Gaikwad', 'Mr.Bhagywan Gaikwad'),
    ('Mr.Shakir Sayyad', 'Mr.Shakir Sayyad'),
    ('Mr.Vinayak Bagalatti', 'Mr.Vinayak Bagalatti'),
    ('Mr.Siddeshwar More', 'Mr.Siddeshwar More'),
    ('Mr.Yogesh Ailolla', 'Mr.Yogesh Ailolla'),
    ('Mr.Prashant Ghogare', 'Mr.Prashant Ghogare'),
    ('Mr.Piyush Kapase', 'Mr.Piyush Kapase'),
]





DEPARTMENT_CHOICES = [
    ('ACCOUNTS', 'ACCOUNTS'),
    ('BOILER', 'BOILER'),
    ('UTILITY', 'UTILITY'),
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
    ('PROJECT', 'PROJECT'),
    ('RM & ENGINEERING STORE', 'RM & ENGINEERING STORE'),
]

SEVERITY_CHOICES = [
    (1, '1- Negligible (Can lead to First Aid Injury)'),
    (2, '2- Marginal (Can lead to MTC or RWC injury)'),
    (3, '3- Critical (Can lead to LTI Injury)'),
    (4, '4- Catastrophic (Multiple LTI or Fatalities)'),
]

LIKELIHOOD_CHOICES = [
    (1, '1- Unlikely (Very Rare - Once in a Lifetime)'),
    (2, '2- Remote (Once in 10 Years)'),
    (3, '3- Occasional (Once in 2 Years)'),
    (4, '4- Frequent (Once or more in a year)'),
]

LEADING_ABNORMALITY_CHOICES = [
    ('Unsafe Act', 'Unsafe Act'),
    ('Unsafe Condition', 'Unsafe Condition'),
    ('Safety Observation', 'Safety Observation'),
    ('Environment Abnormality', 'Environment Abnormality'),
]

STATUS_CHOICES = [
    ('Open', 'Open'),
    ('Closed', 'Closed'),
    ('Omitted', 'Omitted'),
]

class LeadingRecordForm(forms.ModelForm):
    department = forms.ChoiceField(
        choices=[('', 'Select Department')] + DEPARTMENT_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
    )
    physical_location = forms.ModelChoiceField(
        queryset=Physical_Location.objects.all(),
        empty_label="Select Physical Location",
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
    )
    leading_abnormality = forms.ChoiceField(
        choices=[('', 'Select Leading Abnormality')] + LEADING_ABNORMALITY_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
    )
    severity = forms.ChoiceField(
        choices=[('', 'Select Severity')] + [(str(k), v) for k, v in SEVERITY_CHOICES],
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'id': 'severity',
            'onchange': 'calculateRiskFactor()',
        }),
    )
    likelihood = forms.ChoiceField(
        choices=[('', 'Select Likelihood')] + [(str(k), v) for k, v in LIKELIHOOD_CHOICES],
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'id': 'likelihood',
            'onchange': 'calculateRiskFactor()',
        }),
    )
    risk_factor = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'readonly': 'readonly',
            'id': 'risk_factor',
        }),
    )
    status = forms.ChoiceField(
        choices=[('', 'Select Status')] + STATUS_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
    )
    psl_member_name = forms.ChoiceField(
        choices=[('', 'Select PSL Member')] + PSL_MEMBER_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
        required=False,
    )
    responsible_person = forms.ChoiceField(
        choices=[('', 'Select Responsible Person')] + PSL_MEMBER_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
        required=False,
    )


    class Meta:
        model = LeadingRecords
        fields = [
            'observation_date',
            'department',
            'physical_location',
            'leading_abnormality',
            'initiated_by',
            'severity',
            'likelihood',
            'risk_factor',
            'observation_description',
            'corrective_action',
            'psl_member_name',
            'responsible_person',
            'root_cause',
            'preventive_action',
            'target_date',
            'remark',
            'status',
        ]
        widgets = {
            'observation_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'initiated_by': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'observation_description': forms.Textarea(attrs={
                'rows': 3,
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'corrective_action': forms.Textarea(attrs={
                'rows': 3,
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'root_cause': forms.Textarea(attrs={
                'rows': 3,
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'preventive_action': forms.Textarea(attrs={
                'rows': 3,
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'target_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'remark': forms.Textarea(attrs={
                'rows': 2,
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
        }



# ============================= Below is the Lagging Form related code ================================

# Global choice constants (from your previous definitions)
EMPLOYEE_TYPE_CHOICES = [
    ("Company", "Company"),
    ("Contract", "Contract")
]


HSE_LAG_INDICATOR_CHOICES = [
    ("MiPSI", "MiPSI (Minor Process Safety)"),
    ("MPSI", "MPSI (Major Process Safety)"),
    ("TRFR", "TRFR (Total Recordable Frequency Rate)"),
    ("SI", "SI (Severity Incidents)"),
    ("Near Miss", "Near Miss"),
    ("FAC", "FAC"),
    ("RWC", "RWC"),
    ("MTC", "MTC"),
    ("LTI", "LTI"),
    ("Multiple LTI", "Multiple LTI"),
    ("No Injury", "No Injury"),
    ("Unusual Occurrence", "Unusual Occurrence"),
    ("Fire Incident", "Fire Incident"),
]

TYPE_OF_INJURY_CHOICES = [
    ("Chemical Burn", "Chemical Burn"),
    ("Thermal Burn", "Thermal Burn"),
    ("Cut", "Cut"),
    ("Chemical Splash", "Chemical Splash"),
    ("Blunt/Internal Injury", "Blunt/Internal Injury"),
    ("Sprain", "Sprain"),
    ("Strain", "Strain"),
    ("Chemical Inhale", "Chemical Inhale"),
    ("Dehydration", "Dehydration"),
    ("Tripping", "Tripping"),
    ("Abrasion", "Abrasion"),
    ("Grievous hurt", "Grievous hurt"),
    ("Swelling", "Swelling"),
    ("Scratching", "Scratching"),
    ("Fracture", "Fracture"),
    ("Laceration", "Laceration"),
    ("Lachrymation", "Lachrymation"),
    ("Ergonomic injuries", "Ergonomic injuries"),
    ("No Injury", "No Injury"),
]

INJURED_BODY_PART_CHOICES = [
    ("Head", "Head"),
    ("Face", "Face"),
    ("Eye", "Eye"),
    ("Ear", "Ear"),
    ("Chin", "Chin"),
    ("Nose", "Nose"),
    ("Mouth", "Mouth"),
    ("Hand", "Hand"),
    ("Finger", "Finger"),
    ("Chest", "Chest"),
    ("Neck", "Neck"),
    ("Back", "Back"),
    ("Legs", "Legs"),
    ("Hips", "Hips"),
    ("Thighs", "Thighs"),
    ("Stomach", "Stomach"),
    ("Shoulder", "Shoulder"),
    ("Elbow", "Elbow"),
    ("Knee", "Knee"),
    ("Ill health", "Ill health"),
    ("Trunk", "Trunk"),
    ("Abdominal", "Abdominal"),
    ("Spine", "Spine"),
    ("Ankle", "Ankle"),
]

PSM_FAILURE_CHOICES = [
    ("Process Technology", "Process Technology"),
    ("Operating Process", "Operating Process"),
    ("Safe work Practices", "Safe work Practices"),
    ("Management of Change (Technology)", "Management of Change (Technology)"),
    ("Process Hazards Analysis", "Process Hazards Analysis"),
    ("Quality Assurance & Mechanical Integrity", "Quality Assurance & Mechanical Integrity"),
    ("Pre start up Review (PSSR)", "Pre start up Review (PSSR)"),
    ("Management of Subtle changes", "Management of Subtle changes"),
    ("Training & Performance", "Training & Performance"),
    ("Contractor Safety Performance", "Contractor Safety Performance"),
    ("Incident Investigation", "Incident Investigation"),
    ("Management of Change-Personnel", "Management of Change-Personnel"),
    ("Emergency Response & Planning", "Emergency Response & Planning"),
    ("Auditing", "Auditing"),
]

COMPLIANCE_STATUS_CHOICES = [
    ("Open", "Open"),
    ("Closed", "Closed"),
    ("Overdue", "Overdue")
]


INVESTIGATION_METHOD_CHOICES = [
    ('Fishbone', 'Fishbone'),
    ('Why-Why-Analysis', 'Why-Why-Analysis')
]


# Inline form for CAPA entries
class LaggingCapaEntryInlineForm(forms.ModelForm):
    compliance_status = forms.ChoiceField(
        required=False,
        choices=[('', 'Select Compliance Status')] + COMPLIANCE_STATUS_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    department = forms.ChoiceField(
        required=False,
        choices=[('', 'Select Department')] + DEPARTMENT_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
    )
    capa = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'})
    )
    frp = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'})
    )
    target_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'w-full p-2 border border-gray-300 rounded-lg'})
    )
    class Meta:
        model = LaggingCapaEntry
        fields = '__all__'


LaggingCapaEntryFormSet = inlineformset_factory(
    Lagging_Indicator,
    LaggingCapaEntry,
    form=LaggingCapaEntryInlineForm,
    extra=0,
    can_delete=True,
    fields=('id','capa', 'department', 'frp', 'compliance_status', 'target_date')
)

class LaggingIndicatorForm(forms.ModelForm):
    record_date = forms.DateField(
        widget=forms.DateInput(attrs={
            'type': 'date',
            'readonly': 'readonly',
            'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-100'
        }),
        initial=timezone.now().date()
    )
    incident_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    incident_time = forms.TimeField(
        required=False,
        widget=forms.TimeInput(attrs={
            'type': 'time',
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    date_resume_duty = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    complience_status_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'type': 'date',
            'readonly': 'readonly',
            'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-100'
        })
    )
    employee_type = forms.ChoiceField(
        required=False,
        choices=[('', 'Select Employee Type')] + EMPLOYEE_TYPE_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'id': 'id_employee_type',
        })
    )
    contractor_name = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'id': 'id_contractor_name',
            'placeholder': 'Enter contractor name'
        })
    )
    department = forms.ChoiceField(
        required=False,
        choices=[('', 'Select Department')] + DEPARTMENT_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    physical_location = forms.ModelChoiceField(
        required=False,
        queryset=Physical_Location.objects.all(),
        empty_label="Select Physical Location",
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    hse_lag_indicator = forms.ChoiceField(
        required=False,
        choices=[('', 'Select HSE Lag Indicator')] + HSE_LAG_INDICATOR_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    type_of_injury = forms.ChoiceField(
        required=False,
        choices=[('', 'Select Type Of Injury')] + TYPE_OF_INJURY_CHOICES,   
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    injured_body_part = forms.ChoiceField(
        required=False,
        choices=[('', 'Select Injured Body Part')] + INJURED_BODY_PART_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    name_of_injured_person = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',

        })
    )
    psm_failure = forms.MultipleChoiceField(
    choices=PSM_FAILURE_CHOICES,
    required=False,
    widget=forms.MultipleHiddenInput  # hides Django’s default checkboxes
)

    severity = forms.ChoiceField(
        choices=[('', 'Select Severity')] + [(str(k), v) for k, v in SEVERITY_CHOICES],
        widget=forms.Select(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'})
    )
    
    likelihood = forms.ChoiceField(
        choices=[('', 'Select Likelihood')] + [(str(k), v) for k, v in LIKELIHOOD_CHOICES],
        widget=forms.Select(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'})
    )
    risk_factor = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-100',
            'readonly': 'readonly',
        })
    )
    mandays_lost = forms.IntegerField(
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-100',
            'readonly': 'readonly'
        })
    )
    complience_status = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-100',
            'readonly': 'readonly'
        })
    )
    incident = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 3,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    immediate_action = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 3,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    investigation_method = forms.ChoiceField(
        required=True,
        choices=[('', 'Select Method')] + INVESTIGATION_METHOD_CHOICES,
        initial='Fishbone',
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'id': 'id_investigation_method',
        })
    )
    fact_about_men = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    fact_about_machine = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    fact_about_mother_nature = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    fact_about_material = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    fact_about_measurement = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    fact_about_method = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    fact_about_history = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    why_one = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    why_two = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    why_three = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    why_four = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    why_five = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 2,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    
    direct_root_cause = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 3,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )
    indirect_root_cause = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 3,
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )

    class Meta:
        model = Lagging_Indicator
        fields = [
            'record_date', 'incident_date', 'incident_time', 'employee_type', 'contractor_name', 'department','physical_location',
            'hse_lag_indicator','type_of_injury','injured_body_part','name_of_injured_person','severity',
            'likelihood','risk_factor','incident','immediate_action','investigation_method','fact_about_men', 'fact_about_machine', 
            'fact_about_mother_nature', 'fact_about_measurement', 'fact_about_method', 'fact_about_material', 
            'fact_about_history','why_one', 'why_two', 'why_three', 'why_four', 'why_five',
            'direct_root_cause','indirect_root_cause','psm_failure','date_resume_duty', 
            'mandays_lost','complience_status','complience_status_date',
        ]
        
    def clean_psm_failure(self):
        """Join the multiple selected PSM failures into a comma-separated string."""
        psm_list = self.cleaned_data.get('psm_failure', [])
        return ", ".join(psm_list)
    
    def clean(self):
        cleaned_data = super().clean()
        emp_type = cleaned_data.get('employee_type')
        # if not Contract, clear contractor_name
        if emp_type != 'Contract':
            cleaned_data['contractor_name'] = None

        # your existing investigation-method logic...
        method = cleaned_data.get('investigation_method')
        if method == 'Fishbone':
            for field in ['why_one','why_two','why_three','why_four','why_five']:
                cleaned_data[field] = None
        elif method == 'Why-Why-Analysis':
            for field in ['fact_about_men','fact_about_machine',
                          'fact_about_mother_nature','fact_about_measurement',
                          'fact_about_method','fact_about_material','fact_about_history']:
                cleaned_data[field] = None

        return cleaned_data

    def clean_risk_factor(self):
        value = self.cleaned_data.get("risk_factor")
        # If value is a number, convert to label
        if value and "-" not in value and value.isdigit():
            num = int(value)
            if num <= 2:
                label = "Low"
            elif num <= 6:
                label = "Medium"
            else:
                label = "High"
            return f"{num} - {label}"
        return value

COMPLIANCE_CHOICES = [
    ('', 'Select Compliance Status'),
    ('Completed', 'Completed'),
    ('Pending', 'Pending'),
    ('Omitted', 'Omitted'),
]

RPN_CATEGORY_CHOICES = [
    ('', 'Select RPN Category'),
    ('A', 'A'),
    ('B', 'B'),
]



class PSSRObservationInlineForm(forms.ModelForm):
    observar = forms.CharField(
        required=False,
        label="Observar",
        widget=forms.TextInput({
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'placeholder': 'Enter Observar'
        })
    )
    observation = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'rows': 2,
            'placeholder': 'Enter Observation'
        })
    )

    fpr = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'placeholder': 'Enter FPR'
        })
    )

    target_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
        })
    )

    rpn_category = forms.ChoiceField(
        required=False,
        choices=RPN_CATEGORY_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
        })
    )

    compliance_status = forms.ChoiceField(
        required=True,
        choices=COMPLIANCE_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
        })
    )
    compliance_date = forms.DateField(
        required=False,
        widget=forms.DateInput({
            'type':'date',
            'class':'w-full p-2 border border-gray-300 rounded-lg',
        })
    )
    remark = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'placeholder': 'Enter Remark'
        })
    )

    class Meta:
        model = PSSRObservation
        fields = ['observar','observation', 'fpr', 'target_date', 'rpn_category', 'compliance_status','compliance_date', 'remark',]


PSSRObservationFormSet = inlineformset_factory(
    PSSRJobRecord,
    PSSRObservation,
    form=PSSRObservationInlineForm,
    extra=1,
    can_delete=True
)


class PSSRJobRecordForm(forms.ModelForm):
    date = forms.DateField(
        required=True,
        initial=timezone.now().date(),
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-100',
        })
    )
    moc_no = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'placeholder': 'Enter MOC No'
        })
    )

    job_description = forms.CharField(required=False, widget=forms.Textarea(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'rows': 3,'placeholder': 'Enter Job Description'}))

    class Meta:
        model = PSSRJobRecord
        fields = ['date', 'moc_no', 'job_description']


    



##########   Daily work pemit  ############################

class DailyWorkPermitForm(forms.ModelForm):

    class Meta:
        model = Dailyworkpermit
        fields = '__all__'

        widgets = {

            'site_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'permit_approval_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'reason_for_delay': forms.Textarea(attrs={
                'rows': 1,
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none resize-none'
            }),

            'work_description': forms.Textarea(attrs={
                'rows': 1,
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none resize-none'
            }),

            'work_location': forms.TextInput(attrs={
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'permit_no': forms.NumberInput(attrs={
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'work_type': forms.Select(attrs={
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'work_type_value': forms.TextInput(attrs={
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none',
                'placeholder': 'Enter value'
            }),

            'initiated_by': forms.TextInput(attrs={
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none',
            }),

            'permit_open_closed': forms.Select(attrs={
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'valid_from': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'valid_to': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'Extenction_valid_from': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'Extenction_valid_to': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'permit_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none'
            }),

            'permit_status_reason': forms.Textarea(attrs={
                'rows': 1,
                'class': 'w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-1 focus:ring-blue-400 focus:outline-none resize-none'
            }),
        }

    # ? make permit status compulsory
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['permit_open_closed'].required = True


    # ? validation for permit status
    def clean_permit_open_closed(self):
        status = self.cleaned_data.get('permit_open_closed')

        if not status:
            raise forms.ValidationError("Permit Status is required.")

        return status


    # ? conditional validation
    def clean(self):
        cleaned_data = super().clean()

        status = cleaned_data.get("permit_open_closed")
        reason = cleaned_data.get("permit_status_reason")

        if status == "Open" and not reason:
            self.add_error(
                'permit_status_reason',
                'Reason is required when permit status is Open.'
            )
        return cleaned_data