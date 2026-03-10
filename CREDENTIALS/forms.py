from django import forms
from .models import Credentials,ExtentionList

class CredentialApplicationForm(forms.ModelForm):
    class Meta:
        model = Credentials
        fields = [
            'location', 'device', 'lan_ip', 'wan_ip', 'port_no', 'frwd_to', 'url',
            'user_name', 'old_password', 'new_password', 'status', 'expiry_on'
        ]
        widgets = {
            'location': forms.Select(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
            }),
            'device': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'Device',
            }),
            'lan_ip': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'LAN IP',
                'type': 'text',
            }),
            'wan_ip': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'WAN IP',
                'type': 'text',
            }),
            'port_no': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'Port No',
            }),
            'frwd_to': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'Forward To',
            }),
            'url': forms.URLInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'URL',
            }),
            'user_name': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'User Name',
            }),
            'old_password': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'Old Password',
            }),
            'new_password': forms.TextInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'placeholder': 'New Password',
            }),
            'status': forms.Select(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
            }),
            'expiry_on': forms.DateInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg',
                'type': 'date',
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.required = False



# ── Below code for Extension List ────────────────────────────────────────────────────────────────


# Tailwind base classes
BASE_INPUT = (
    "block w-full rounded-lg border border-slate-300 px-3 py-2 "
    "text-sm text-slate-800 placeholder-slate-400 "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500"
)
BASE_SELECT = (
    "block w-full rounded-lg border border-slate-300 px-3 py-2 "
    "text-sm text-slate-800 bg-white "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500"
)

class ExtentionListForm(forms.ModelForm):
    class Meta:
        model = ExtentionList
        fields = ["name", "department","designation", "extension_no", "mobile", "location"]
        labels = {
            "name": "Name",
            "department": "Department",
            "designation": "Designation",
            "extension_no": "Extension No.",
            "mobile": "Mobile",
            "location": "Location",
        }
        widgets = {
            "name": forms.TextInput(attrs={"class": BASE_INPUT, "placeholder": "Full name"}),
            "department": forms.Select(attrs={"class": BASE_SELECT}),
            "designation": forms.Select(attrs={"class": BASE_SELECT}),
            "extension_no": forms.TextInput(
                attrs={
                    "class": BASE_INPUT,
                    "placeholder": "e.g., 205,206",
                }
            ),
            "mobile": forms.TextInput(
                attrs={"class": BASE_INPUT, "inputmode": "tel", "placeholder": "+91XXXXXXXXXX"}
            ),
            "location": forms.Select(attrs={"class": BASE_SELECT}),
        }

    def clean_extension_no(self):
        """Ensure each extension number is numeric & ≤ 6 digits"""
        exts = (self.cleaned_data.get("extension_no") or "").replace(" ", "")
        if not exts:
            return exts

        for ext in exts.split(","):
            if not ext.isdigit():
                raise forms.ValidationError("Each extension must be digits only.")
            if len(ext) > 6:
                raise forms.ValidationError("Each extension must be max 6 digits.")
        return exts

    def clean_mobile(self):
        m = (self.cleaned_data.get("mobile") or "").replace(" ", "")
        return m

class DirectorySearchForm(forms.Form):
    """Optional: simple filters for the list page."""
    q = forms.CharField(
        required=False,
        label="Search",
        widget=forms.TextInput(
            attrs={"class": BASE_INPUT, "placeholder": "Search name / mobile / extension / designation"}
        ),
    )
    department = forms.ChoiceField(
        required=False,
        choices=[("", "All Departments")] + list(ExtentionList.DEPARTMENT_CHOICES),
        widget=forms.Select(attrs={"class": BASE_SELECT}),
        label="Department",
    )
    designation = forms.ChoiceField(
        required=False,
        choices=[("", "All Designations")] + list(ExtentionList.DESIGNATION_CHOICES),
        widget=forms.Select(attrs={"class": BASE_SELECT}),
        label="Designation",
    )
    location = forms.ChoiceField(
        required=False,
        choices=[("", "All Locations")] + list(ExtentionList.LOCATION_CHOICES),
        widget=forms.Select(attrs={"class": BASE_SELECT}),
        label="Location",
    )
