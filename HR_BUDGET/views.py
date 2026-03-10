import logging
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from HR_BUDGET.models import *
from HR_BUDGET.forms import (ContractorWagesForm,SecurityWagesForm,HrBudgetWelfareForm,
                             HrBudgetCanteenForm,HrBudgetMedicalForm,HrBudgetVehicleForm,HrBudgetTravellingForm,
                             HRBudgetGuestHouseForm, HRBudgetGeneralAdminForm,HRBudgetCommunicationForm,InsuranceMediclaimForm,
                             HRBudgetAMCForm,HRBudgetTrainingForm,HRBudgetLegalForm,AdminRepairAndMaintenanceForm,AdminCapexForm,HRBudgetPlanForm)
from django.shortcuts import render, redirect, get_object_or_404
from django.core.paginator import Paginator
from django.db.models import Sum, F, Min, Max, Value, DateField, DecimalField
from django.db.models.functions import Coalesce, TruncMonth,TruncDay
from django.utils import timezone
from decimal import Decimal
import datetime
from django import forms
from django.db.models.functions import TruncWeek
from django.db.models import DateField
import io
import xlsxwriter
from django.http import HttpResponse
from .templatetags.dict_utils import indian_currency_format
from django.db.models import Q
import pandas as pd
import calendar

logger = logging.getLogger('custom_logger')


# Add Contractor Wages
@login_required
def add_contractor_wages(request):
    if request.method == 'POST':
        form = ContractorWagesForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Contractor wage record added successfully!")
            return redirect('add_contractor_wages')  # Replace with your list view name or success page
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = ContractorWagesForm()
    return render(request, 'hrbudget/contractor/contractorwages_form.html', {'form': form,'active_link': 'contractor_wages'})


@login_required
def contractorwages_list(request):
    # Fetch filter params
    contractor_name = request.GET.get('contractor_name', '')
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    export = request.GET.get('export', '')

    # Build queryset with filters
    records = ContractorWages.objects.all()
    if contractor_name:
        records = records.filter(contractor_name=contractor_name)
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    records = records.order_by('-invoice_date')

    # Excel export
    if export == '1':
        # Create DataFrame
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime('%d-%m-%Y') if obj.invoice_date else '',
                "Invoice No": obj.invoice_no,
                "Contractor Name": obj.contractor_name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        import io
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Contractor Wages')
        output.seek(0)
        response = HttpResponse(
            output,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=ContractorWages.xlsx'
        return response

    # Pagination (only for non-export)
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # For dropdown options
    contractor_choices = ContractorWages._meta.get_field('contractor_name').choices

    return render(
        request,
        'hrbudget/contractor/contractorwages_list.html',
        {
            'page_obj': page_obj,
            'active_link': 'contractor_wages',
            'contractor_choices': contractor_choices,
            'contractor_name': contractor_name,
            'from_date': from_date,
            'to_date': to_date,
        }
    )



@login_required
def edit_contractor_wages(request, pk):
    from .models import ContractorWages  # adjust if needed
    obj = ContractorWages.objects.get(pk=pk)
    if request.method == 'POST':
        form = ContractorWagesForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Contractor wage record updated successfully!")
            return redirect('contractorwages_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = ContractorWagesForm(instance=obj)
    return render(request, 'hrbudget/contractor/contractorwages_form.html', {
        'form': form,
        'active_link': 'contractor_wages',
        'is_edit': True,  # this will let the template know we're editing
        'object': obj,    # in case you want to show details
    })



@login_required
def delete_contractor_wages(request, pk):
    from .models import ContractorWages  # adjust if needed
    obj = ContractorWages.objects.get(pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Contractor wage record deleted successfully!")
        return redirect('contractorwages_list')
    return redirect('contractorwages_list')



# Add Security Wages
@login_required
def add_security_wages(request):
    if request.method == 'POST':
        form = SecurityWagesForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Security wage record added successfully!")
            return redirect('add_security_wages')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = SecurityWagesForm()
    return render(request, 'hrbudget/security/securitywages_form.html', {'form': form,'active_link': 'security_wages'})

@login_required
def securitywages_list(request):
    # 1. Build contractor name choices (distinct, no blank)
    contractor_qs = SecurityWages.objects.values_list('contractor_name', flat=True).distinct()
    contractor_choices = [(name, name) for name in contractor_qs if name]

    # 2. Filters
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    contractor_name = request.GET.get('contractor_name', '')

    records = SecurityWages.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if contractor_name:
        records = records.filter(contractor_name=contractor_name)

    # 3. Excel download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Contractor Name": obj.contractor_name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=SecurityWages.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Security Wages')
        return response

    # 4. Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # 5. Render the page
    context = {
        'page_obj': page_obj,
        'from_date': from_date or '',
        'to_date': to_date or '',
        'contractor_name': contractor_name or '',
        'contractor_choices': contractor_choices,
        'active_link': 'security_wages',
    }
    return render(request, 'hrbudget/security/securitywages_list.html', context)


@login_required
def edit_security_wages(request, pk):
    obj = get_object_or_404(SecurityWages, pk=pk)
    if request.method == 'POST':
        form = SecurityWagesForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Security wage record updated successfully!")
            return redirect('securitywages_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = SecurityWagesForm(instance=obj)
    return render(request, 'hrbudget/security/securitywages_form.html', {'form': form,'active_link': 'security_wages'})


@login_required
def delete_security_wages(request, pk):
    obj = get_object_or_404(SecurityWages, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Security wage record deleted successfully!")
        return redirect('securitywages_list')
    # Optional: Redirect even on GET, for safety
    return redirect('securitywages_list')




# Add HR Budget Welfare
@login_required
def add_hrbudget_welfare(request):
    if request.method == 'POST':
        form = HrBudgetWelfareForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Welfare budget record added successfully!")
            return redirect('add_hrbudget_welfare')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetWelfareForm()
    return render(request, 'hrbudget/welfare/hrbudgetwelfare_form.html', {'form': form,'active_link': 'welfare'})



@login_required
def hrbudget_welfare_list(request):
    # 1. Build welfare name choices (distinct, no blank)
    welfare_qs = HrBudgetWelfare.objects.values_list('welfare_name', flat=True).distinct()
    welfare_choices = [(name, name) for name in welfare_qs if name]

    # 2. Filters
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    welfare_name = request.GET.get('welfare_name', '')

    records = HrBudgetWelfare.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if welfare_name:
        records = records.filter(welfare_name=welfare_name)

    # 3. Excel download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Welfare Name": obj.welfare_name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": getattr(obj, 'total_bill_amount', (obj.bill_amount or 0) + (obj.gst or 0)),
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=WelfareBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Welfare Budget')
        return response

    # 4. Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # 5. Render the page
    context = {
        'page_obj': page_obj,
        'from_date': from_date or '',
        'to_date': to_date or '',
        'welfare_name': welfare_name or '',
        'welfare_choices': welfare_choices,
        'active_link': 'welfare',
    }
    return render(request, 'hrbudget/welfare/hrbudgetwelfare_list.html', context)


@login_required
def edit_hrbudget_welfare(request, pk):
    obj = get_object_or_404(HrBudgetWelfare, pk=pk)
    if request.method == 'POST':
        form = HrBudgetWelfareForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Welfare record updated successfully!")
            return redirect('hrbudget_welfare_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetWelfareForm(instance=obj)
    return render(request, 'hrbudget/welfare/hrbudgetwelfare_form.html', {'form': form, 'active_link': 'welfare'})


@login_required
def delete_hrbudget_welfare(request, pk):
    obj = get_object_or_404(HrBudgetWelfare, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Welfare record deleted successfully!")
        return redirect('hrbudget_welfare_list')
    return redirect('hrbudget_welfare_list')



# Add HR Budget Canteen
@login_required
def add_hrbudget_canteen(request):
    if request.method == 'POST':
        form = HrBudgetCanteenForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Canteen budget record added successfully!")
            return redirect('add_hrbudget_canteen')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetCanteenForm()
    return render(request, 'hrbudget/canteen/hrbudgetcanteen_form.html', {'form': form,'active_link': 'canteen'})



@login_required
def hrbudget_canteen_list(request):
    # 1. Build distinct canteen name choices
    name_qs = HrBudgetCanteen.objects.values_list('name', flat=True).distinct()
    canteen_choices = [(n, n) for n in name_qs if n]

    # 2. Filters
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    name = request.GET.get('name', '')

    records = HrBudgetCanteen.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if name:
        records = records.filter(name=name)

    # 3. Excel download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Name": obj.name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=CanteenBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Canteen Budget')
        return response

    # 4. Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'from_date': from_date or '',
        'to_date': to_date or '',
        'name': name or '',
        'canteen_choices': canteen_choices,
        'active_link': 'canteen',
    }
    return render(request, 'hrbudget/canteen/hrbudgetcanteen_list.html', context)


@login_required
def edit_hrbudget_canteen(request, pk):
    obj = get_object_or_404(HrBudgetCanteen, pk=pk)
    if request.method == 'POST':
        form = HrBudgetCanteenForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Canteen record updated successfully!")
            return redirect('hrbudget_canteen_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetCanteenForm(instance=obj)
    return render(request, 'hrbudget/canteen/hrbudgetcanteen_form.html', {'form': form, 'active_link': 'canteen'})



@login_required
def delete_hrbudget_canteen(request, pk):
    obj = get_object_or_404(HrBudgetCanteen, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Canteen record deleted successfully!")
        return redirect('hrbudget_canteen_list')
    return redirect('hrbudget_canteen_list')


# Add HR Budget Medical
@login_required
def add_hrbudget_medical(request):
    if request.method == 'POST':
        form = HrBudgetMedicalForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Medical budget record added successfully!")
            return redirect('add_hrbudget_medical')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetMedicalForm()
    return render(request, 'hrbudget/medical/hrbudgetmedical_form.html', {'form': form,'active_link': 'medical'})


@login_required
def hrbudget_medical_list(request):
    # Distinct medical (doctor/hospital) name choices
    name_qs = HrBudgetMedical.objects.values_list('doctor_hospital_name', flat=True).distinct()
    medical_choices = [(n, n) for n in name_qs if n]

    # Filters
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    doctor_hospital_name = request.GET.get('doctor_hospital_name', '')

    records = HrBudgetMedical.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if doctor_hospital_name:
        records = records.filter(doctor_hospital_name=doctor_hospital_name)

    # Excel Download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Doctor/Hospital Name": obj.doctor_hospital_name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=MedicalBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Medical Budget')
        return response

    # Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'from_date': from_date or '',
        'to_date': to_date or '',
        'doctor_hospital_name': doctor_hospital_name or '',
        'medical_choices': medical_choices,
        'active_link': 'medical'
    }
    return render(request, 'hrbudget/medical/hrbudgetmedical_list.html', context)




@login_required
def edit_hrbudget_medical(request, pk):
    obj = get_object_or_404(HrBudgetMedical, pk=pk)
    if request.method == 'POST':
        form = HrBudgetMedicalForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Medical budget record updated successfully!")
            return redirect('hrbudget_medical_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetMedicalForm(instance=obj)
    return render(request, 'hrbudget/medical/hrbudgetmedical_form.html', {
        'form': form, 'active_link': 'medical'
    })



@login_required
def delete_hrbudget_medical(request, pk):
    obj = get_object_or_404(HrBudgetMedical, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Medical budget record deleted successfully!")
        return redirect('hrbudget_medical_list')
    return redirect('hrbudget_medical_list')


@login_required
def add_hrbudget_vehicle(request):
    if request.method == 'POST':
        form = HrBudgetVehicleForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Vehicle budget record added successfully!")
            return redirect('add_hrbudget_vehicle')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetVehicleForm()

    return render(request, 'hrbudget/vehicle/hrbudgetvehicle_form.html', {
        'form': form,
        'active_link': 'vehicle'
    })


@login_required
def hrbudget_vehicle_list(request):

    # -------------------- 1. FILTERS --------------------
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    category = request.GET.get('category', '')
    vehicle_name = request.GET.get('vehicle_name', '').strip()

    # ? NEW RANGE FILTERS
    min_liter = request.GET.get('min_liter')
    max_liter = request.GET.get('max_liter')
    min_km = request.GET.get('min_kilometer')
    max_km = request.GET.get('max_kilometer')

    records = HrBudgetVehicle.objects.all().order_by('-invoice_date', '-id')

    if from_date:
        records = records.filter(invoice_date__gte=from_date)

    if to_date:
        records = records.filter(invoice_date__lte=to_date)

    if category:
        records = records.filter(category=category)

    if vehicle_name:
        records = records.filter(vehicle_name__icontains=vehicle_name)

    # ? Liter Range Filter
    if min_liter:
        records = records.filter(liter__gte=min_liter)

    if max_liter:
        records = records.filter(liter__lte=max_liter)

    # ? Kilometer Range Filter
    if min_km:
        records = records.filter(kilometer__gte=min_km)

    if max_km:
        records = records.filter(kilometer__lte=max_km)


    # -------------------- 2. ADD MILEAGE CALCULATION --------------------
    for obj in records:
        if obj.liter and obj.kilometer:
            obj.mileage = round(float(obj.kilometer) / float(obj.liter), 2)
        else:
            obj.mileage = None


    # -------------------- 3. EXCEL DOWNLOAD --------------------
    if request.GET.get('export') == '1':

        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Vehicle Name": obj.vehicle_name,
                "Vehicle Number": obj.vehicle_number,
                "Category": obj.category,
                "Liter": obj.liter,
                "Kilometer": obj.kilometer,
                "Mileage (Km/L)": obj.mileage if obj.mileage else "",
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": (
                    obj.total_bill_amount if obj.total_bill_amount is not None
                    else (obj.bill_amount or 0) + (obj.gst or 0)
                ),
                "Description": obj.description,
            })

        df = pd.DataFrame(data)

        response = HttpResponse(
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=VehicleBudget.xlsx'

        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Vehicle Budget')

        return response


    # -------------------- 4. PAGINATION --------------------
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)


    # -------------------- 5. Persist filters in pagination --------------------
    filter_dict = request.GET.copy()
    if 'page' in filter_dict:
        del filter_dict['page']
    filter_query = filter_dict.urlencode()


    # -------------------- 6. RENDER --------------------
    context = {
        'page_obj': page_obj,
        'from_date': from_date or '',
        'to_date': to_date or '',
        'category': category or '',
        'vehicle_name': vehicle_name or '',
        'min_liter': min_liter or '',
        'max_liter': max_liter or '',
        'min_kilometer': min_km or '',
        'max_kilometer': max_km or '',
        'filter_query': filter_query,
        'active_link': 'vehicle',
    }

    return render(request, 'hrbudget/vehicle/hrbudgetvehicle_list.html', context)


@login_required
def edit_hrbudget_vehicle(request, pk):
    obj = get_object_or_404(HrBudgetVehicle, pk=pk)
    if request.method == 'POST':
        form = HrBudgetVehicleForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Vehicle budget record updated successfully!")
            return redirect('hrbudget_vehicle_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetVehicleForm(instance=obj)
    return render(request, 'hrbudget/vehicle/hrbudgetvehicle_form.html', {
        'form': form,
        'active_link': 'vehicle'
    })



@login_required
def delete_hrbudget_vehicle(request, pk):
    obj = get_object_or_404(HrBudgetVehicle, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Vehicle budget record deleted successfully!")
        return redirect('hrbudget_vehicle_list')
    return redirect('hrbudget_vehicle_list')



@login_required
def add_hrbudget_travelling_lodging(request):
    if request.method == 'POST':
        form = HrBudgetTravellingForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Travelling/Lodging/Boarding record added successfully!")
            return redirect('add_hrbudget_travelling_lodging')  # Change to your list view name if needed
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetTravellingForm()
    return render(request, 'hrbudget/travelling/hrbudgettravelling_form.html', {
        'form': form,
        'active_link': 'travelling_lodging_boarding'
    })

@login_required
def hrbudget_travelling_lodging_list(request):
    # Distinct Name choices (with label)
    name_qs = HrBudgetTravellingLodging.objects.values_list('name', flat=True).distinct()
    name_choices = [(n, n) for n in name_qs if n]

    # Filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    name = request.GET.get('name', '')

    records = HrBudgetTravellingLodging.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if name:
        records = records.filter(name=name)

    # Excel Download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Name": obj.name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=TravellingLodgingBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Travelling Lodging Budget')
        return response

    # Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'name': name,
        'name_choices': name_choices,
        'active_link': 'travelling_lodging_boarding',
    }
    return render(request, 'hrbudget/travelling/hrbudgettravelling_list.html', context)


@login_required
def edit_hrbudget_travelling_lodging(request, pk):
    obj = get_object_or_404(HrBudgetTravellingLodging, pk=pk)
    if request.method == 'POST':
        form = HrBudgetTravellingForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Travelling/Lodging/Boarding record updated successfully!")
            return redirect('hrbudget_travelling_lodging_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HrBudgetTravellingForm(instance=obj)
    return render(request, 'hrbudget/travelling/hrbudgettravelling_form.html', {
        'form': form,
        'active_link': 'travelling_lodging_boarding'
    })

@login_required
def delete_hrbudget_travelling_lodging(request, pk):
    obj = get_object_or_404(HrBudgetTravellingLodging, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Travelling/Lodging/Boarding record deleted successfully!")
        return redirect('hrbudget_travelling_lodging_list')
    return redirect('hrbudget_travelling_lodging_list')



@login_required
def add_hrbudget_guesthouse(request):
    if request.method == 'POST':
        form = HRBudgetGuestHouseForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Guest House record added successfully!")
            return redirect('add_hrbudget_guesthouse')  # Or your guest house list view
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetGuestHouseForm()
    return render(request, 'hrbudget/guesthouse/hrbudgetguesthouse_form.html', {
        'form': form,
        'active_link': 'guest_house'
    })


@login_required
def hrbudget_guesthouse_list(request):
    # Distinct Name and Category choices
    name_qs = HRBudgetGuestHouse.objects.values_list('name', flat=True).distinct()
    name_choices = [(n, n) for n in name_qs if n]
    category_qs = HRBudgetGuestHouse.objects.values_list('category', flat=True).distinct()
    category_choices = [(c, c) for c in category_qs if c]

    # Filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    name = request.GET.get('name', '')
    category = request.GET.get('category', '')

    records = HRBudgetGuestHouse.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if name:
        records = records.filter(name=name)
    if category:
        records = records.filter(category=category)

    # Excel Download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Name": obj.name,
                "Category": obj.category,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=GuestHouseBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Guest House Budget')
        return response

    # Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'name': name,
        'name_choices': name_choices,
        'category': category,
        'category_choices': category_choices,
        'active_link': 'guest_house',
    }
    return render(request, 'hrbudget/guesthouse/hrbudgetguesthouse_list.html', context)


@login_required
def edit_hrbudget_guesthouse(request, pk):
    obj = get_object_or_404(HRBudgetGuestHouse, pk=pk)
    if request.method == 'POST':
        form = HRBudgetGuestHouseForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Guest House record updated successfully!")
            return redirect('hrbudget_guesthouse_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetGuestHouseForm(instance=obj)
    return render(request, 'hrbudget/guesthouse/hrbudgetguesthouse_form.html', {
        'form': form,
        'active_link': 'guest_house'
    })

@login_required
def delete_hrbudget_guesthouse(request, pk):
    obj = get_object_or_404(HRBudgetGuestHouse, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Guest House record deleted successfully!")
        return redirect('hrbudget_guesthouse_list')
    return redirect('hrbudget_guesthouse_list')



@login_required
def add_hrbudget_general_admin(request):
    if request.method == 'POST':
        form = HRBudgetGeneralAdminForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "General Admin budget record added successfully!")
            return redirect('add_hrbudget_general_admin')  # Or use your list view url
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetGeneralAdminForm()
    return render(request, 'hrbudget/generaladmin/generaladmin_form.html', {
        'form': form,
        'active_link': 'general_admin'
    })


@login_required
def hrbudget_general_admin_list(request):
    # Distinct category choices (with label from model)
    category_qs = HRBudgetGeneralAdmin.objects.values_list('category', flat=True).distinct()
    category_map = dict(HRBudgetGeneralAdmin.CATEGORY_CHOICES)
    category_choices = [(c, category_map.get(c, c)) for c in category_qs if c]

    # Filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    category = request.GET.get('category', '')

    records = HRBudgetGeneralAdmin.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if category:
        records = records.filter(category=category)

    # Excel Download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no or "",
                "Category": category_map.get(obj.category, obj.category or ""),
                "GST": obj.gst,
                "Bill Amount": obj.bill_amount,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description or "",
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=GeneralAdminBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='General Admin Budget')
        return response

    # Pagination
    paginator = Paginator(records, 25)  # or 10 if you prefer
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'category': category,
        'category_choices': category_choices,
        'active_link': 'general_admin',
    }
    return render(request, 'hrbudget/generaladmin/generaladmin_list.html', context)



@login_required
def edit_hrbudget_general_admin(request, pk):
    obj = get_object_or_404(HRBudgetGeneralAdmin, pk=pk)
    if request.method == 'POST':
        form = HRBudgetGeneralAdminForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "General Admin budget record updated successfully!")
            return redirect('hrbudget_general_admin_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetGeneralAdminForm(instance=obj)
    return render(request, 'hrbudget/generaladmin/generaladmin_form.html', {
        'form': form,
        'active_link': 'general_admin'
    })

@login_required
def delete_hrbudget_general_admin(request, pk):
    obj = get_object_or_404(HRBudgetGeneralAdmin, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "General Admin budget record deleted successfully!")
        return redirect('hrbudget_general_admin_list')
    return redirect('hrbudget_general_admin_list')



@login_required
def add_hrbudget_communication(request):
    if request.method == 'POST':
        form = HRBudgetCommunicationForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Communication budget record added successfully!")
            return redirect('add_hrbudget_communication')  # or use your list page: 'hrbudget_communication_list'
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetCommunicationForm()
    return render(request, 'hrbudget/communication/hrbudgetcommunication_form.html', {
        'form': form,
        'active_link': 'communication'
    })


@login_required
def edit_hrbudget_communication(request, pk):
    obj = get_object_or_404(HRBudgetCommunication, pk=pk)
    if request.method == 'POST':
        form = HRBudgetCommunicationForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Communication budget record updated successfully!")
            return redirect('hrbudget_communication_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetCommunicationForm(instance=obj)
    return render(request, 'hrbudget/communication/hrbudgetcommunication_form.html', {
        'form': form,
        'active_link': 'communication'
    })


@login_required
def delete_hrbudget_communication(request, pk):
    obj = get_object_or_404(HRBudgetCommunication, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Communication budget record deleted successfully!")
        return redirect('hrbudget_communication_list')
    return redirect('hrbudget_communication_list')


@login_required
def hrbudget_communication_list(request):
    # Choices for filter
    name_qs = HRBudgetCommunication.objects.values_list('invoice_name', flat=True).distinct()
    invoice_name_choices = [(n, n) for n in name_qs if n]

    # Filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    invoice_name = request.GET.get('invoice_name', '')

    records = HRBudgetCommunication.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if invoice_name:
        records = records.filter(invoice_name=invoice_name)

    # Excel Export
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Invoice Name": obj.invoice_name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=CommunicationBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Communication Budget')
        return response

    # Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'hrbudget/communication/hrbudgetcommunication_list.html', {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'invoice_name': invoice_name,
        'invoice_name_choices': invoice_name_choices,
        'active_link': 'communication'
    })



@login_required
def add_insurance_mediclaim(request):
    if request.method == 'POST':
        form = InsuranceMediclaimForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Insurance Mediclaim record added successfully!")
            return redirect('insurance_mediclaim_list')  # Change to your list view name
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = InsuranceMediclaimForm()

    return render(request, 'hrbudget/insurance_mediclaim/add_insurance_mediclaim_form.html', {
        'form': form,
        'active_link': 'insurance_mediclaim',
    })


@login_required
def edit_insurance_mediclaim(request, pk):
    obj = get_object_or_404(InsuranceMediclaim, pk=pk)
    if request.method == 'POST':
        form = InsuranceMediclaimForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Insurance Mediclaim record updated successfully!")
            return redirect('insurance_mediclaim_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = InsuranceMediclaimForm(instance=obj)
    return render(request, 'hrbudget/insurance_mediclaim/add_insurance_mediclaim_form.html', {  # Use same template as add
        'form': form,
        'active_link': 'insurance_mediclaim',
    })


@login_required
def insurance_mediclaim_list(request):
    # Get distinct categories for filter dropdown
    categories_qs = InsuranceMediclaim.objects.values_list('category', flat=True).distinct()
    category_choices = [(c, c) for c in categories_qs if c]

    # Get filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    category = request.GET.get('category', '')

    records = InsuranceMediclaim.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if category:
        records = records.filter(category=category)

    # Excel Download (using xlsxwriter)
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Category": obj.category,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": (obj.bill_amount or 0) + (obj.gst or 0),
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=InsuranceMediclaim.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Insurance Mediclaim')
        return response

    # Pagination (with filters preserved in query)
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'category': category,
        'category_choices': category_choices,
        'active_link': 'insurance_mediclaim',
    }
    return render(request, 'hrbudget/insurance_mediclaim/insurance_mediclaim_list.html', context)


@login_required
def delete_insurance_mediclaim(request, pk):
    obj = get_object_or_404(InsuranceMediclaim, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Insurance Mediclaim record deleted successfully!")
        return redirect('insurance_mediclaim_list')
    # For GET request, optionally redirect or show confirmation page
    return redirect('insurance_mediclaim_list')



@login_required
def add_hrbudget_amc(request):
    if request.method == 'POST':
        form = HRBudgetAMCForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "AMC record added successfully!")
            return redirect('hrbudget_amc_list')  # Update with your list view name
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetAMCForm()

    return render(request, 'hrbudget/amc/amc_form.html', {
        'form': form,
        'active_link': 'hrbudget_amc',
    })



@login_required
def edit_hrbudget_amc(request, pk):
    obj = get_object_or_404(HRBudgetAMC, pk=pk)
    if request.method == 'POST':
        form = HRBudgetAMCForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "AMC record updated successfully!")
            return redirect('hrbudget_amc_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetAMCForm(instance=obj)
    return render(request, 'hrbudget/amc/amc_form.html', {
        'form': form,
        'active_link': 'hrbudget_amc',
    })

@login_required
def delete_hrbudget_amc(request, pk):
    obj = get_object_or_404(HRBudgetAMC, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "AMC record deleted successfully!")
        return redirect('hrbudget_amc_list')
    return redirect('hrbudget_amc_list')



@login_required
def hrbudget_amc_list(request):
    # Distinct AMC names for filter dropdown
    name_qs = HRBudgetAMC.objects.values_list('amc_name', flat=True).distinct()
    amc_choices = [(n, n) for n in name_qs if n]

    # Filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    amc_name = request.GET.get('amc_name', '')

    records = HRBudgetAMC.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if amc_name:
        records = records.filter(amc_name=amc_name)

    # Excel Export
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "AMC Name": obj.amc_name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": getattr(obj, 'total_bill_amount', (obj.bill_amount or 0) + (obj.gst or 0)),
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=AMCBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='AMC Budget')
        return response

    # Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'amc_name': amc_name,
        'amc_choices': amc_choices,
        'active_link': 'hrbudget_amc',
    }
    return render(request, 'hrbudget/amc/amc_list.html', context)



@login_required
def add_hrbudget_training(request):
    if request.method == 'POST':
        form = HRBudgetTrainingForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Training record added successfully!")
            return redirect('hrbudget_training_list')  # Update with your list view name
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetTrainingForm()

    return render(request, 'hrbudget/training/training_form.html', {
        'form': form,
        'active_link': 'hrbudget_training',
    })



@login_required
def edit_hrbudget_training(request, pk):
    obj = get_object_or_404(HRBudgetTraining, pk=pk)
    if request.method == 'POST':
        form = HRBudgetTrainingForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Training record updated successfully!")
            return redirect('hrbudget_training_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetTrainingForm(instance=obj)
    return render(request, 'hrbudget/training/training_form.html', {
        'form': form,
        'active_link': 'hrbudget_training',
    })

@login_required
def delete_hrbudget_training(request, pk):
    obj = get_object_or_404(HRBudgetTraining, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Training record deleted successfully!")
        return redirect('hrbudget_training_list')
    return redirect('hrbudget_training_list')


@login_required
def hrbudget_training_list(request):
    # Get distinct training names for dropdown filter
    name_qs = HRBudgetTraining.objects.values_list('name', flat=True).distinct()
    name_choices = [(n, n) for n in name_qs if n]

    # Filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    name = request.GET.get('name', '')

    records = HRBudgetTraining.objects.order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if name:
        records = records.filter(name=name)

    # Excel Download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Training Name": obj.name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": getattr(obj, 'total_bill_amount', (obj.bill_amount or 0) + (obj.gst or 0)),
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=TrainingRecords.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Training Records')
        return response

    # Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'hrbudget/training/training_list.html', {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'name': name,
        'name_choices': name_choices,
        'active_link': 'hrbudget_training',
    })


@login_required
def add_hrbudget_legal(request):
    if request.method == 'POST':
        form = HRBudgetLegalForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Legal record added successfully!")
            return redirect('hrbudget_legal_list')  # Update with your list view name
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetLegalForm()

    return render(request, 'hrbudget/legal/legal_form.html', {
        'form': form,
        'active_link': 'hrbudget_legal',
    })

@login_required
def edit_hrbudget_legal(request, pk):
    obj = get_object_or_404(HRBudgetLegal, pk=pk)
    if request.method == 'POST':
        form = HRBudgetLegalForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Legal record updated successfully!")
            return redirect('hrbudget_legal_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = HRBudgetLegalForm(instance=obj)
    return render(request, 'hrbudget/legal/legal_form.html', {
        'form': form,
        'active_link': 'hrbudget_legal',
    })

@login_required
def delete_hrbudget_legal(request, pk):
    obj = get_object_or_404(HRBudgetLegal, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Legal record deleted successfully!")
        return redirect('hrbudget_legal_list')
    return redirect('hrbudget_legal_list')



@login_required
def hrbudget_legal_list(request):
    # Distinct names for filter dropdown
    name_qs = HRBudgetLegal.objects.values_list('name', flat=True).distinct()
    name_choices = [(n, n) for n in name_qs if n]

    # Get filters from request
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    name = request.GET.get('name', '')

    records = HRBudgetLegal.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if name:
        records = records.filter(name=name)

    # Excel Download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Name": obj.name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": getattr(obj, 'total_bill_amount', (obj.bill_amount or 0)+(obj.gst or 0)),
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=LegalBudget.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Legal Budget')
        return response

    # Pagination
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'hrbudget/legal/legal_list.html', {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'name': name,
        'name_choices': name_choices,
        'active_link': 'hrbudget_legal',
    })



@login_required
def add_admin_repair(request):
    if request.method == 'POST':
        form = AdminRepairAndMaintenanceForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Record added successfully!")
            return redirect('admin_repair_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = AdminRepairAndMaintenanceForm()
    return render(request, 'hrbudget/admin_repair/admin_repair_form.html', {
        'form': form,
        'active_link': 'admin_repair_list',
    })


@login_required
def admin_repair_list(request):
    # Filters
    name_qs = AdminRepairAndMaintenance.objects.values_list('name', flat=True).distinct()
    name_choices = [(n, n) for n in name_qs if n]

    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    name = request.GET.get('name', '')

    records = AdminRepairAndMaintenance.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if name:
        records = records.filter(name=name)

    # Excel Export
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Name": obj.name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=AdminRepairAndMaintenance.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='AdminRepair')
        return response

    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'hrbudget/admin_repair/admin_repair_list.html', {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'name': name,
        'name_choices': name_choices,
        'active_link': 'admin_repair_list',
    })



@login_required
def edit_admin_repair(request, pk):
    obj = get_object_or_404(AdminRepairAndMaintenance, pk=pk)
    if request.method == 'POST':
        form = AdminRepairAndMaintenanceForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Record updated successfully!")
            return redirect('admin_repair_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = AdminRepairAndMaintenanceForm(instance=obj)
    return render(request, 'hrbudget/admin_repair/admin_repair_form.html', {
        'form': form,
        'active_link': 'admin_repair_list',
    })

@login_required
def delete_admin_repair(request, pk):
    obj = get_object_or_404(AdminRepairAndMaintenance, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Record deleted successfully!")
        return redirect('admin_repair_list')
    return redirect('admin_repair_list')





@login_required
def add_admin_capex(request):
    if request.method == 'POST':
        form = AdminCapexForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "CAPEX record added successfully!")
            return redirect('admin_capex_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = AdminCapexForm()
    return render(request, 'hrbudget/admin_capex/admin_capex_form.html', {
        'form': form,
        'active_link': 'admin_capex',
    })

@login_required
def admin_capex_list(request):
    # For dropdown filter (distinct names)
    name_qs = AdminCapex.objects.values_list('name', flat=True).distinct()
    name_choices = [(n, n) for n in name_qs if n]

    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    name = request.GET.get('name', '')

    records = AdminCapex.objects.all().order_by('-invoice_date', '-id')
    if from_date:
        records = records.filter(invoice_date__gte=from_date)
    if to_date:
        records = records.filter(invoice_date__lte=to_date)
    if name:
        records = records.filter(name=name)

    # Excel Download
    if request.GET.get('export') == '1':
        data = []
        for obj in records:
            data.append({
                "Invoice Date": obj.invoice_date.strftime("%d-%m-%Y") if obj.invoice_date else "",
                "Invoice No": obj.invoice_no,
                "Name": obj.name,
                "Bill Amount": obj.bill_amount,
                "GST": obj.gst,
                "Total Bill Amount": obj.total_bill_amount,
                "Description": obj.description,
            })
        df = pd.DataFrame(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=AdminCapex.xlsx'
        with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Admin Capex')
        return response

    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    return render(request, 'hrbudget/admin_capex/admin_capex_list.html', {
        'page_obj': page_obj,
        'from_date': from_date,
        'to_date': to_date,
        'name': name,
        'name_choices': name_choices,
        'active_link': 'admin_capex',
    })



@login_required
def edit_admin_capex(request, pk):
    obj = get_object_or_404(AdminCapex, pk=pk)
    if request.method == 'POST':
        form = AdminCapexForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "CAPEX record updated successfully!")
            return redirect('admin_capex_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = AdminCapexForm(instance=obj)
    return render(request, 'hrbudget/admin_capex/admin_capex_form.html', {
        'form': form,
        'active_link': 'admin_capex',
    })

@login_required
def delete_admin_capex(request, pk):
    obj = get_object_or_404(AdminCapex, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "CAPEX record deleted successfully!")
        return redirect('admin_capex_list')
    return redirect('admin_capex_list')










# ==========================Dashboard==================================================

def get_financial_year_choices():
    now = datetime.date.today()
    current_fy_start_year = now.year if now.month >= 4 else now.year - 1
    years = []
    for i in range(-1, 2):
        y1 = current_fy_start_year + i
        y2 = str(y1 + 1)[-2:]
        year_str = f"{y1}-{y2}"
        years.append((year_str, year_str))
    return sorted(years, reverse=True)

class PeriodFilterForm(forms.Form):
    year = forms.ChoiceField(
        choices=get_financial_year_choices,
        required=False,
        label="Year",
        widget=forms.Select(attrs={'class': 'border rounded px-2 py-1'})
    )
    
    # --- UPDATED: 'weekly' option is removed ---
    PERIOD_CHOICES = [
        ('monthly', 'Monthly'),
        ('daily', 'Daily'),
    ]
    period_type = forms.ChoiceField(
        choices=PERIOD_CHOICES,
        required=True,
        widget=forms.Select(attrs={'class': 'border rounded px-2 py-1'})
    )
    from_date = forms.DateField(required=False, widget=forms.DateInput(attrs={'type': 'date', 'class': 'border rounded px-2 py-1'}))
    to_date = forms.DateField(required=False, widget=forms.DateInput(attrs={'type': 'date', 'class': 'border rounded px-2 py-1'}))

# ... (get_month_range_local and get_financial_year are unchanged) ...
def get_month_range_local(start_date, end_date):
    months = []
    if not start_date or not end_date: return []
    current_date = datetime.date(start_date.year, start_date.month, 1)
    end_month_start = datetime.date(end_date.year, end_date.month, 1)
    while current_date <= end_month_start:
        months.append(current_date)
        year = current_date.year + (current_date.month // 12)
        month = current_date.month % 12 + 1
        current_date = datetime.date(year, month, 1)
    return months


def get_financial_year(d):
    if not d: d = timezone.now().date()
    if d.month >= 4:
        return f"{d.year}-{str(d.year + 1)[-2:]}"
    else:
        return f"{d.year - 1}-{str(d.year)[-2:]}"
        
# ===================================================================
#      COMPLETELY REVISED VIEW
# ===================================================================

@login_required
def monthly_hr_budget_summary(request):
    model_configs = [
        ("Contractor Wages", ContractorWages, 'contractor_name'),
        ("Security Wages", SecurityWages, 'contractor_name'),
        ("Welfare", HrBudgetWelfare, 'welfare_name'),
        ("Canteen", HrBudgetCanteen, 'name'),
        ("Medical Expenses", HrBudgetMedical, 'doctor_hospital_name'),
        ("Vehicle Expenses", HrBudgetVehicle, 'vehicle_name'),
        ("Travelling & Lodging", HrBudgetTravellingLodging, 'name'),
        ("Guest House Expenses", HRBudgetGuestHouse, 'name'),
        ("General Admin Expenses", HRBudgetGeneralAdmin, 'category'),
        ("Communication Expenses", HRBudgetCommunication, 'invoice_name'),
        ("Insurance Mediclaim", InsuranceMediclaim, 'category'),
        ("AMC", HRBudgetAMC, 'amc_name'),
        ("Training", HRBudgetTraining, 'name'),
        ("Legal", HRBudgetLegal, 'name'),
        ("Admin Repair & Maintenance", AdminRepairAndMaintenance, 'name'),
        ("Admin Capex", AdminCapex, 'name'),
    ]

    # --- CORRECTED: Manually handle GET parameters for robustness ---
    form = PeriodFilterForm(request.GET or None) # Still use form for validation and rendering Year/Period dropdowns

    period_type = request.GET.get('period_type', 'monthly')
    selected_year_str = request.GET.get('year')
    from_date_str = request.GET.get('from_date', '') # Default to empty string
    to_date_str = request.GET.get('to_date', '')   # Default to empty string
    
    from_date_obj, to_date_obj = None, None
    try:
        if from_date_str: from_date_obj = datetime.datetime.strptime(from_date_str, '%Y-%m-%d').date()
        if to_date_str: to_date_obj = datetime.datetime.strptime(to_date_str, '%Y-%m-%d').date()
    except ValueError:
        pass # Ignore invalid date formats

    # --- Date Range Determination Logic ---
    min_date, max_date = None, None
    if period_type == 'monthly':
        if not selected_year_str:
            selected_year_str = get_financial_year(timezone.now().date())
        fy_start_year = int(selected_year_str.split('-')[0])
        min_date = datetime.date(fy_start_year, 4, 1)
        max_date = datetime.date(fy_start_year + 1, 3, 31)
    
    elif period_type == 'daily':
        if from_date_obj and to_date_obj:
            min_date, max_date = from_date_obj, to_date_obj
            selected_year_str = get_financial_year(min_date)
        else:
            min_date, max_date = datetime.date.today(), datetime.date.today() - datetime.timedelta(days=1)
            if not selected_year_str:
                selected_year_str = get_financial_year(timezone.now().date())
    
    if not request.GET: # Set initial form values on first load only
        form.initial['year'] = selected_year_str
        form.initial['period_type'] = period_type

    # ... (The rest of the data processing logic remains the same) ...
    periods_with_data = set()
    group_by = TruncMonth('invoice_date') if period_type == 'monthly' else TruncDay('invoice_date', output_field=DateField())

    for _, model_class, _ in model_configs:
        periods = model_class.objects.filter(invoice_date__range=[min_date, max_date])\
            .annotate(period=group_by)\
            .values('period')\
            .annotate(total=Sum('bill_amount'))\
            .filter(total__gt=0)\
            .values_list('period', flat=True)
        periods_with_data.update(p for p in periods if p is not None)
    
    date_headers = []
    sorted_periods = sorted(list(periods_with_data))

    for period_obj in sorted_periods:
        header = {'date_obj': period_obj}
        if period_type == 'monthly':
            header['display'] = period_obj.strftime("%b-%y")
        else:
            header['display'] = period_obj.strftime("%d-%b-%y")
        date_headers.append(header)

    financial_year_str = selected_year_str
    start_year = int(financial_year_str.split('-')[0])
    days_in_year = 366 if calendar.isleap(start_year + 1) else 365
    plan_objs = HRBudgetPlan.objects.filter(year=financial_year_str)
    plan_by_category = {obj.category: obj.plan_amount * 100000 for obj in plan_objs}
    monthly_plan_by_category = {cat: annual_plan / 12 for cat, annual_plan in plan_by_category.items()}
    daily_plan_by_category = {cat: annual_plan / days_in_year for cat, annual_plan in plan_by_category.items()}

    processed_rows = []
    grand_totals = {
        'total_plan': Decimal('0.00'), 'total_actual': Decimal('0.00'),
        'period_values': {h['date_obj']: {'plan': Decimal('0.00'), 'actual': Decimal('0.00')} for h in date_headers}
    }
    category_index = 0
    
    for cat_display_name, model_class, item_field_name in model_configs:
        category_index += 1
        category_id_for_html = f"cat-{category_index}"
        
        monthly_plan = monthly_plan_by_category.get(cat_display_name, Decimal('0.00'))
        daily_plan = daily_plan_by_category.get(cat_display_name, Decimal('0.00'))

        item_actuals_qs = model_class.objects.filter(invoice_date__range=[min_date, max_date])\
            .annotate(period=group_by, item_name=F(item_field_name))\
            .values('period', 'item_name')\
            .annotate(total=Sum(Coalesce('bill_amount', Value(0, output_field=DecimalField()))))\
            .order_by('item_name', 'period')
        
        item_map, category_actuals_by_period = {}, {h['date_obj']: Decimal('0.00') for h in date_headers}
        for entry in item_actuals_qs:
            item_name, period_obj, amount = entry['item_name'] or "N/A", entry['period'], entry['total']
            if period_obj is None: continue
            if item_name not in item_map: item_map[item_name] = {'period_values': {h['date_obj']: Decimal('0.00') for h in date_headers}}
            if period_obj in item_map[item_name]['period_values']: item_map[item_name]['period_values'][period_obj] = amount
            if period_obj in category_actuals_by_period: category_actuals_by_period[period_obj] += amount

        category_period_plans, category_total_plan = {}, Decimal('0.00')
        for h in date_headers:
            period_obj, period_plan = h['date_obj'], Decimal('0.00')
            if period_type == 'monthly':
                period_plan = monthly_plan
            else:
                period_plan = daily_plan
            category_period_plans[period_obj], category_total_plan = period_plan, category_total_plan + period_plan

        category_total_actual = sum(category_actuals_by_period.values())
        processed_rows.append({
            'type': 'category', 'html_id': category_id_for_html, 'name': cat_display_name,
            'period_values': {h['date_obj']: {'plan': category_period_plans.get(h['date_obj'], Decimal('0.00')), 'actual': category_actuals_by_period.get(h['date_obj'], Decimal('0.00'))} for h in date_headers},
            'total_plan': category_total_plan, 'total_actual': category_total_actual
        })

        for item_name, data in sorted(item_map.items()):
            processed_rows.append({'type': 'item', 'parent_category_id': category_id_for_html, 'name': item_name, 'period_values': data['period_values'], 'total_actual': sum(data['period_values'].values())})
        
        grand_totals['total_plan'] += category_total_plan
        grand_totals['total_actual'] += category_total_actual
        for h in date_headers:
            period_obj = h['date_obj']
            grand_totals['period_values'][period_obj]['plan'] += category_period_plans.get(period_obj, Decimal('0.00'))
            grand_totals['period_values'][period_obj]['actual'] += category_actuals_by_period.get(period_obj, Decimal('0.00'))

    grand_totals.update({'name': 'Grand Total', 'type': 'grand_total'})
    processed_rows.append(grand_totals)
    
    context = {
        'report_title': "HR Budget Summary (Budget vs Actual)",
        'date_headers': date_headers,
        'processed_rows': processed_rows,
        'filter_form': form,
        'period_type': period_type,
        'selected_year': selected_year_str,
        # --- CORRECTED: Pass date strings back to template to fix UI ---
        'from_date_str': from_date_str,
        'to_date_str': to_date_str,
    }
    return render(request, 'hrbudget/hr_budget_summary.html', context)




@login_required
def download_hr_budget_excel(request):
    """
    FINAL VERSION: Generates a two-sheet Excel report (Detailed and Summary).
    - Headers now use "Budget" instead of "Plan".
    - All numerical values (Budget and Actual) are formatted as bold.
    """
    model_configs = [
        ("Contractor Wages", ContractorWages, 'contractor_name'),
        ("Security Wages", SecurityWages, 'contractor_name'),
        ("Welfare", HrBudgetWelfare, 'welfare_name'),
        ("Canteen", HrBudgetCanteen, 'name'),
        ("Medical Expenses", HrBudgetMedical, 'doctor_hospital_name'),
        ("Vehicle Expenses", HrBudgetVehicle, 'vehicle_name'),
        ("Travelling & Lodging", HrBudgetTravellingLodging, 'name'),
        ("Guest House Expenses", HRBudgetGuestHouse, 'name'),
        ("General Admin Expenses", HRBudgetGeneralAdmin, 'category'),
        ("Communication Expenses", HRBudgetCommunication, 'invoice_name'),
        ("Insurance Mediclaim", InsuranceMediclaim, 'category'),
        ("AMC", HRBudgetAMC, 'amc_name'),
        ("Training", HRBudgetTraining, 'name'),
        ("Legal", HRBudgetLegal, 'name'),
        ("Admin Repair & Maintenance", AdminRepairAndMaintenance, 'name'),
        ("Admin Capex", AdminCapex, 'name'),
    ]
    
    # 1. Manually parse GET parameters for robustness
    period_type = request.GET.get('period_type', 'monthly')
    selected_year_str = request.GET.get('year')
    from_date_str = request.GET.get('from_date')
    to_date_str = request.GET.get('to_date')

    from_date_obj, to_date_obj = None, None
    try:
        if from_date_str: from_date_obj = datetime.datetime.strptime(from_date_str, '%Y-%m-%d').date()
        if to_date_str: to_date_obj = datetime.datetime.strptime(to_date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError): pass

    min_date, max_date = None, None
    if period_type == 'daily':
        if from_date_obj and to_date_obj:
            min_date, max_date = from_date_obj, to_date_obj
            selected_year_str = get_financial_year(min_date)
        else:
            min_date, max_date = datetime.date.today(), datetime.date.today() - datetime.timedelta(days=1)
            if not selected_year_str: selected_year_str = get_financial_year(timezone.now().date())
    else: # 'monthly'
        if not selected_year_str: selected_year_str = get_financial_year(timezone.now().date())
        fy_start_year = int(selected_year_str.split('-')[0])
        min_date = datetime.date(fy_start_year, 4, 1)
        max_date = datetime.date(fy_start_year + 1, 3, 31)

    # 2. Data processing logic (This part is identical for both sheets)
    periods_with_data = set()
    group_by = TruncMonth('invoice_date') if period_type == 'monthly' else TruncDay('invoice_date', output_field=DateField())
    for _, model_class, _ in model_configs:
        periods = model_class.objects.filter(invoice_date__range=[min_date, max_date])\
            .annotate(period=group_by).values('period').annotate(total=Sum('bill_amount'))\
            .filter(total__gt=0).values_list('period', flat=True)
        periods_with_data.update(p for p in periods if p is not None)
    
    date_headers = []
    sorted_periods = sorted(list(periods_with_data))
    for period_obj in sorted_periods:
        header = {'date_obj': period_obj}
        header['display'] = period_obj.strftime("%b-%y" if period_type == 'monthly' else "%d-%b-%y")
        date_headers.append(header)

    financial_year_str = selected_year_str
    start_year = int(financial_year_str.split('-')[0])
    days_in_year = 366 if calendar.isleap(start_year + 1) else 365
    plan_objs = HRBudgetPlan.objects.filter(year=financial_year_str)
    plan_by_category = {obj.category: obj.plan_amount * 100000 for obj in plan_objs}
    monthly_plan_by_category = {cat: annual_plan / 12 for cat, annual_plan in plan_by_category.items()}
    daily_plan_by_category = {cat: annual_plan / days_in_year for cat, annual_plan in plan_by_category.items()}

    processed_rows = []
    grand_totals = {
        'total_plan': Decimal('0.00'), 'total_actual': Decimal('0.00'),
        'period_values': {h['date_obj']: {'plan': Decimal('0.00'), 'actual': Decimal('0.00')} for h in date_headers}
    }
    
    for cat_display_name, model_class, item_field_name in model_configs:
        monthly_plan = monthly_plan_by_category.get(cat_display_name, Decimal('0.00'))
        daily_plan = daily_plan_by_category.get(cat_display_name, Decimal('0.00'))
        
        item_actuals_qs = model_class.objects.filter(invoice_date__range=[min_date, max_date])\
            .annotate(period=group_by, item_name=F(item_field_name)).values('period', 'item_name')\
            .annotate(total=Sum(Coalesce('bill_amount', Value(0, output_field=DecimalField()))))\
            .order_by('item_name', 'period')
        
        item_map, category_actuals_by_period = {}, {h['date_obj']: Decimal('0.00') for h in date_headers}
        for entry in item_actuals_qs:
            item_name, period_obj, amount = entry['item_name'] or "N/A", entry['period'], entry['total']
            if period_obj is None: continue
            if item_name not in item_map: item_map[item_name] = {'period_values': {h['date_obj']: Decimal('0.00') for h in date_headers}}
            if period_obj in item_map[item_name]['period_values']: item_map[item_name]['period_values'][period_obj] = amount
            if period_obj in category_actuals_by_period: category_actuals_by_period[period_obj] += amount

        category_period_plans, category_total_plan = {}, Decimal('0.00')
        for h in date_headers:
            period_plan = monthly_plan if period_type == 'monthly' else daily_plan
            category_period_plans[h['date_obj']] = period_plan
            category_total_plan += period_plan

        category_total_actual = sum(category_actuals_by_period.values())
        processed_rows.append({
            'type': 'category', 'name': cat_display_name,
            'period_values': {h['date_obj']: {'plan': category_period_plans.get(h['date_obj'], Decimal('0.00')), 'actual': category_actuals_by_period.get(h['date_obj'], Decimal('0.00'))} for h in date_headers},
            'total_plan': category_total_plan, 'total_actual': category_total_actual
        })
        
        for item_name, data in sorted(item_map.items()):
            processed_rows.append({'type': 'item', 'name': item_name, 'period_values': data['period_values'], 'total_actual': sum(data['period_values'].values())})
        
        grand_totals['total_plan'] += category_total_plan
        grand_totals['total_actual'] += category_total_actual
        for h in date_headers:
            period_obj = h['date_obj']
            grand_totals['period_values'][period_obj]['plan'] += category_period_plans.get(period_obj, Decimal('0.00'))
            grand_totals['period_values'][period_obj]['actual'] += category_actuals_by_period.get(period_obj, Decimal('0.00'))

    grand_totals.update({'name': 'Grand Total', 'type': 'grand_total'})
    processed_rows.append(grand_totals)

    # 3. Generate Excel file in-memory
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})

    # --- UPDATED: All format definitions now include 'bold': True for amounts ---
    num_format = '#,##,##0.00'
    title_fmt = workbook.add_format({'bold': True, 'font_size': 16, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#4472C4', 'font_color': 'white'})
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#4472C4', 'font_color': 'white', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
    plan_header_fmt = workbook.add_format({'bold': True, 'bg_color': '#C6E0B4', 'border': 1, 'align': 'center'})
    actual_header_fmt = workbook.add_format({'bold': True, 'bg_color': '#DEEBF7', 'border': 1, 'align': 'center'})
    cat_name_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1})
    item_name_fmt = workbook.add_format({'border': 1, 'indent': 1})
    grand_total_name_fmt = workbook.add_format({'bold': True, 'bg_color': '#8FAADC', 'border': 1})
    
    # All amount formats are now explicitly bold
    plan_fmt = workbook.add_format({'bold': True, 'border': 1, 'num_format': num_format})
    actual_fmt = workbook.add_format({'bold': True, 'border': 1, 'num_format': num_format})
    actual_red_fmt = workbook.add_format({'bold': True, 'border': 1, 'num_format': num_format, 'font_color': '#C00000'})
    actual_green_fmt = workbook.add_format({'bold': True, 'border': 1, 'num_format': num_format, 'font_color': '#00B050'})
    
    # =================================================================
    #                      WRITE SHEET 1: DETAILED VIEW
    # =================================================================
    worksheet_detail = workbook.add_worksheet('Plan vs Actual')
    last_col_detail = 1 + (len(date_headers) * 2) + 1
    worksheet_detail.merge_range(0, 0, 0, last_col_detail, 'HR Budget Summary (Budget vs Actual)', title_fmt)
    worksheet_detail.set_row(0, 30)
    
    header_start_row = 1
    worksheet_detail.merge_range(header_start_row, 0, header_start_row + 1, 0, 'Name', header_fmt)
    col = 1
    for h in date_headers:
        worksheet_detail.merge_range(header_start_row, col, header_start_row, col + 1, h['display'], header_fmt)
        worksheet_detail.write(header_start_row + 1, col, 'Budget', plan_header_fmt) # UPDATED
        worksheet_detail.write(header_start_row + 1, col + 1, 'Actual', actual_header_fmt)
        col += 2
    worksheet_detail.merge_range(header_start_row, col, header_start_row, col + 1, 'Total', header_fmt)
    worksheet_detail.write(header_start_row + 1, col, 'Budget', plan_header_fmt) # UPDATED
    worksheet_detail.write(header_start_row + 1, col + 1, 'Actual', actual_header_fmt)

    row_idx_detail = header_start_row + 2
    for row_data in processed_rows:
        row_type = row_data['type']
        name_fmt = item_name_fmt if row_type == 'item' else (grand_total_name_fmt if row_type == 'grand_total' else cat_name_fmt)
        worksheet_detail.write(row_idx_detail, 0, row_data['name'], name_fmt)
        
        col_idx = 1
        for header in date_headers:
            date_obj = header['date_obj']
            if row_type == 'item':
                worksheet_detail.write(row_idx_detail, col_idx, '', plan_fmt)
                worksheet_detail.write_number(row_idx_detail, col_idx + 1, row_data['period_values'].get(date_obj, Decimal('0.00')), actual_fmt)
            else:
                period_vals = row_data['period_values'].get(date_obj, {})
                plan_val = period_vals.get('plan', Decimal('0.00'))
                actual_val = period_vals.get('actual', Decimal('0.00'))
                worksheet_detail.write_number(row_idx_detail, col_idx, plan_val, plan_fmt)
                actual_cell_fmt = actual_green_fmt if actual_val <= plan_val else actual_red_fmt
                worksheet_detail.write_number(row_idx_detail, col_idx + 1, actual_val, actual_cell_fmt)
            col_idx += 2
        
        if row_type == 'item':
            worksheet_detail.write(row_idx_detail, col_idx, '', plan_fmt)
            worksheet_detail.write_number(row_idx_detail, col_idx + 1, row_data.get('total_actual', Decimal('0.00')), actual_fmt)
        else:
            total_plan = row_data.get('total_plan', Decimal('0.00'))
            total_actual = row_data.get('total_actual', Decimal('0.00'))
            worksheet_detail.write_number(row_idx_detail, col_idx, total_plan, plan_fmt)
            total_actual_cell_fmt = actual_green_fmt if total_actual <= total_plan else actual_red_fmt
            worksheet_detail.write_number(row_idx_detail, col_idx + 1, total_actual, total_actual_cell_fmt)
        row_idx_detail += 1
    
    worksheet_detail.set_column('A:A', 40)
    worksheet_detail.set_column('B:Z', 18)

    # =================================================================
    #                      WRITE SHEET 2: SUMMARY VIEW
    # =================================================================
    worksheet_summary = workbook.add_worksheet('Summary')
    last_col_summary = 1 + (len(date_headers) * 2) + 1
    worksheet_summary.merge_range(0, 0, 0, last_col_summary, 'HR Budget Summary (Budget vs Actual)', title_fmt)
    worksheet_summary.set_row(0, 30)
    
    summary_header_row = 1
    worksheet_summary.merge_range(summary_header_row, 0, summary_header_row + 1, 0, 'Name', header_fmt)
    col = 1
    for h in date_headers:
        worksheet_summary.merge_range(summary_header_row, col, summary_header_row, col + 1, h['display'], header_fmt)
        worksheet_summary.write(summary_header_row + 1, col, 'Budget', plan_header_fmt) # UPDATED
        worksheet_summary.write(summary_header_row + 1, col + 1, 'Actual', actual_header_fmt)
        col += 2
    worksheet_summary.merge_range(summary_header_row, col, summary_header_row, col + 1, 'Total', header_fmt)
    worksheet_summary.write(summary_header_row + 1, col, 'Budget', plan_header_fmt) # UPDATED
    worksheet_summary.write(summary_header_row + 1, col + 1, 'Actual', actual_header_fmt)

    row_idx_summary = summary_header_row + 2
    for row_data in processed_rows:
        if row_data['type'] in ['category', 'grand_total']:
            name_fmt = grand_total_name_fmt if row_data['type'] == 'grand_total' else cat_name_fmt
            worksheet_summary.write(row_idx_summary, 0, row_data['name'], name_fmt)
            
            col_idx = 1
            for header in date_headers:
                date_obj = header['date_obj']
                period_vals = row_data['period_values'].get(date_obj, {})
                plan_val = period_vals.get('plan', Decimal('0.00'))
                actual_val = period_vals.get('actual', Decimal('0.00'))
                worksheet_summary.write_number(row_idx_summary, col_idx, plan_val, plan_fmt)
                actual_cell_fmt = actual_green_fmt if actual_val <= plan_val else actual_red_fmt
                worksheet_summary.write_number(row_idx_summary, col_idx + 1, actual_val, actual_cell_fmt)
                col_idx += 2
            
            total_plan = row_data.get('total_plan', Decimal('0.00'))
            total_actual = row_data.get('total_actual', Decimal('0.00'))
            worksheet_summary.write_number(row_idx_summary, col_idx, total_plan, plan_fmt)
            total_actual_cell_fmt = actual_green_fmt if total_actual <= total_plan else actual_red_fmt
            worksheet_summary.write_number(row_idx_summary, col_idx + 1, total_actual, total_actual_cell_fmt)
            row_idx_summary += 1
            
    worksheet_summary.set_column('A:A', 40)
    worksheet_summary.set_column('B:Z', 18)

    # =================================================================
    #                      FINALIZE AND SEND RESPONSE
    # =================================================================
    workbook.close()
    output.seek(0)
    
    filename_period = 'Custom_Range' if period_type == 'daily' else period_type.title()
    filename = f'HR_Budget_Plan_vs_Actual_{selected_year_str}_{filename_period}.xlsx'
    
    response = HttpResponse(output.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response





# =====================================================================================================


# This helper function is required to determine the current financial year.
def get_financial_year(d):
    """Calculates financial year string (e.g., '2025-26') from a date."""
    if not d: d = datetime.date.today()
    if d.month >= 4:
        return f"{d.year}-{str(d.year + 1)[-2:]}"
    else:
        return f"{d.year - 1}-{str(d.year)[-2:]}"



@login_required
def add_hr_budget_plan(request):
    if not request.user.has_perm('HR_BUDGET.add_hrbudgetplan'):
        messages.error(request, "You do not have permission to add HR Budget Plan records.")
        logger.warning(f"User '{request.user.username}' tried to add a hr budget plan record without permission.")
        return redirect('indexpage')
    
    current_fy = get_financial_year(datetime.date.today())
    
    form_year = request.GET.get('year', '')
    edit_id = request.GET.get('edit_id')
    display_year = request.GET.get('display_year')

    initial_year_for_form = form_year if form_year else current_fy
    initial = {'year': initial_year_for_form}
    
    plan_to_edit = None

    if form_year:
        plans_for_form = HRBudgetPlan.objects.filter(year=form_year)
        for plan in plans_for_form:
            initial[plan.category] = plan.plan_amount
    
    if edit_id:
        try:
            plan_to_edit = HRBudgetPlan.objects.get(id=edit_id)
            initial['year'] = plan_to_edit.year
            initial[plan_to_edit.category] = plan_to_edit.plan_amount
        except HRBudgetPlan.DoesNotExist:
            plan_to_edit = None

    if request.method == 'POST':
        form = HRBudgetPlanForm(request.POST)
        if form.is_valid():
            year_val = form.cleaned_data['year']
            edit_id_from_post = request.POST.get('edit_id')
            
            if edit_id_from_post:
                try:
                    plan = HRBudgetPlan.objects.get(id=int(edit_id_from_post))
                    amount = form.cleaned_data.get(plan.category)
                    plan.plan_amount = amount if amount is not None else 0
                    plan.save()
                    messages.success(request, f"Plan for {plan.category} ({plan.year}) updated successfully.")
                except HRBudgetPlan.DoesNotExist:
                    messages.error(request, "Failed to update: Plan not found.")
            else:
                for code, _ in HR_BUDGET_CATEGORY_CHOICES:
                    amount = form.cleaned_data.get(code)
                    if amount is not None:
                        HRBudgetPlan.objects.update_or_create(
                            year=year_val, category=code,
                            defaults={'plan_amount': amount}
                        )
                messages.success(request, f"Plans for the year {year_val} saved successfully.")
            
            return redirect(f"{request.path}?display_year={year_val}")
    else:
        form = HRBudgetPlanForm(initial=initial)

    # --- UPDATED: Changed sort order from '-year' to 'year' ---
    distinct_years = HRBudgetPlan.objects.values_list('year', flat=True).distinct().order_by('year')
    # -----------------------------------------------------------
    
    current_display_year = display_year if display_year else current_fy

    if current_display_year:
        plans_for_display = HRBudgetPlan.objects.filter(year=current_display_year).order_by('category')
        total_plan_amount = plans_for_display.aggregate(total=Sum('plan_amount'))['total'] or 0
    else:
        plans_for_display = HRBudgetPlan.objects.none()
        total_plan_amount = 0

    paginator = Paginator(plans_for_display, 10)
    page_number = request.GET.get('page')
    plans_page = paginator.get_page(page_number)

    context = {
        'form': form,
        'plans_page': plans_page,
        'plan_to_edit': plan_to_edit,
        'total_plan_amount': total_plan_amount,
        'distinct_years': distinct_years,
        'current_display_year': current_display_year,
        'selected_year_for_form': form_year,
    }
    return render(request, 'hrbudget/hr_budget_plan_form.html', context)


@login_required
def hrbudget_delete_plan(request, pk):
    if not request.user.has_perm('HR_BUDGET.delete_hrbudgetplan'):
        messages.error(request, "You do not have permission to delete HR Budget Plan records.")
        logger.warning(f"User '{request.user.username}' tried to delete a hr budget plan record without permission.")
        return redirect('indexpage')
    
    plan = get_object_or_404(HRBudgetPlan, pk=pk)
    plan.delete()
    return redirect('add_hr_budget_plan')

def hrbudget_edit_plan(request, pk):
    if not request.user.has_perm('HR_BUDGET.change_hrbudgetplan'):
        messages.error(request, "You do not have permission to update HR Budget Plan records.")
        logger.warning(f"User '{request.user.username}' tried to update a hr budget plan record without permission.")
        return redirect('indexpage')
    plan = get_object_or_404(HRBudgetPlan, pk=pk)
    # Pre-fill form for only that category/year (you can reuse your form logic)
    initial = {
        'year': plan.year,
        plan.category: plan.plan_amount
    }
    if request.method == 'POST':
        form = HRBudgetPlanForm(request.POST, initial=initial)
        if form.is_valid():
            # Only update this one plan/category/year
            amount = form.cleaned_data.get(plan.category)
            plan.plan_amount = amount
            plan.save()
            return redirect('add_hr_budget_plan')
    else:
        form = HRBudgetPlanForm(initial=initial)
    return render(request, 'hrbudget/hr_budget_plan_edit.html', {'form': form, 'plan': plan})