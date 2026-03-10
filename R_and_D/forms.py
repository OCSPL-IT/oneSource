from django import forms
from .models import R_and_D_Moisture,KFFactorEntry, KFFactorEntryLine,MeltingPointRecord
from django.forms import inlineformset_factory
import re
from django.core.exceptions import ValidationError

MP_PATTERN = r"^\s*\d+(\.\d+)?(\s*-\s*\d+(\.\d+)?)?\s*$"


class RAndDMoistureForm(forms.ModelForm):
    class Meta:
        model = R_and_D_Moisture
        fields = '__all__'
        widgets = {
            'entry_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'entry_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'eln_id': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'product_name': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'batch_no': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'sample_description': forms.Textarea(attrs={    # <-- CHANGE HERE!
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'rows': 2,
                'placeholder': 'Enter sample description...'
            }),
            'unit': forms.Select(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'instrument': forms.Select(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'factor_mg_per_ml': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'sample_weight_gm': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'burette_reading_ml': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'moisture_percent': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'analysed_by': forms.Select(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'completed_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'completed_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Only display 'name' in dropdowns
        for field in ['product_name', 'unit', 'instrument', 'analysed_by']:
            self.fields[field].label_from_instance = lambda obj: obj.name




class KFFactorEntryForm(forms.ModelForm):
    class Meta:
        model = KFFactorEntry
        fields = ['instrument', 'analysed_by']
        widgets = {
            'instrument': forms.Select(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'analysed_by': forms.Select(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Only display 'name' in dropdowns
        for field in ['instrument', 'analysed_by']:
            self.fields[field].label_from_instance = lambda obj: obj.name

class KFFactorEntryLineForm(forms.ModelForm):
    class Meta:
        model = KFFactorEntryLine
        fields = ['sample_weight_mg', 'burette_reading_ml']
        widgets = {
            'sample_weight_mg': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'Sample Weight (mg)'
            }),
            'burette_reading_ml': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'Burette Reading (mL)'
            }),
        }

KFFactorEntryLineFormSet = inlineformset_factory(
    KFFactorEntry, KFFactorEntryLine,
    form=KFFactorEntryLineForm,
    extra=3,  # You can make this dynamic
    can_delete=False
)





class MeltingPointRecordForm(forms.ModelForm):
    class Meta:
        model = MeltingPointRecord
        fields = '__all__'
        widgets = {
            'entry_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'entry_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'eln_id': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'product_name': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'batch_no': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'sample_description': forms.Textarea(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'rows': 2,
                'placeholder': 'Enter sample description...'
            }),
            'unit': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'instrument': forms.Select(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            # 🔁 NumberInput -> TextInput, add pattern & title
            'melting_point': forms.TextInput(attrs={
                'class':'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder':'e.g., 101-102',
                'pattern': r"\s*\d+(\.\d+)?(\s*-\s*\d+(\.\d+)?)?\s*",
                'title': 'Enter a number (e.g., 101.5) or a range (e.g., 101-102)'
            }),

            'analysed_by': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'completed_date': forms.DateInput(attrs={
                'type': 'date',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'completed_time': forms.TimeInput(attrs={
                'type': 'time',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
        }
        
    def clean_melting_point(self):
        v = self.cleaned_data.get('melting_point')
        if not v:
            return v
        # Normalize: convert en-dash/em-dash to hyphen and trim spaces around hyphen
        v = v.replace('–', '-').replace('—', '-').strip()
        v = re.sub(r"\s*-\s*", "-", v)
        if not re.match(MP_PATTERN, v):
            raise ValidationError("Use a number or a range like 101-102 or 101.5-102.2.")
        return v