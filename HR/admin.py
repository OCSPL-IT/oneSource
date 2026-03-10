from django.contrib import admin
from HR.models import HR,DailyAttendance, AttendanceRegulation,DailyCheckIn,Late_Early_Go,On_Duty_Request,OvertimeReport,ShortLeave,Helpdesk_Ticket
from import_export.admin import ExportMixin, ImportExportModelAdmin
from import_export import resources, fields
from import_export.widgets import DateWidget
from datetime import timedelta, date
import pandas as pd
from django.utils import timezone
from django.core.exceptions import MultipleObjectsReturned
from import_export.widgets import DateWidget, DateTimeWidget


@admin.register(HR)
class HRAdmin(admin.ModelAdmin):
    list_display = ('date', 'permanent_employees', 'contract_labour_production', 'contract_labour_others', 'total_employee', 'hrs', 'total_no_of_hrs')
    search_fields = ('date',)
    list_filter = ('date',)

# ===============================================================================================================
class DailyAttendanceResource(resources.ModelResource):
    # ── column → field mappings ────────────────────────────────────────────────
    employee_code            = fields.Field(attribute='employee_code',            column_name='Employee Code')
    full_name                = fields.Field(attribute='full_name',                column_name='Full name')
    employment_status        = fields.Field(attribute='employment_status',        column_name='Employment status')
    company                  = fields.Field(attribute='company',                  column_name='Company')
    business_unit            = fields.Field(attribute='business_unit',            column_name='Business Unit')
    department               = fields.Field(attribute='department',               column_name='Department')
    sub_department           = fields.Field(attribute='sub_department',           column_name='Sub department')
    designation              = fields.Field(attribute='designation',              column_name='Designation')
    branch                   = fields.Field(attribute='branch',                   column_name='Branch')
    sub_branch               = fields.Field(attribute='sub_branch',               column_name='Sub branch')
    attendance_date          = fields.Field(attribute='attendance_date',          column_name='Attendance date', widget=DateWidget(format='%d-%m-%Y'))
    punch_in_punch_out_time  = fields.Field(attribute='punch_in_punch_out_time',  column_name='Punch/clocking time')
    status_in_out            = fields.Field(attribute='status_in_out',            column_name='Status')
    shift_code               = fields.Field(attribute='shift_code',               column_name='Shift code')
    shift_timing             = fields.Field(attribute='shift_timing',             column_name='Shift timings')
    Late_or_early            = fields.Field(attribute='Late_or_early',            column_name='Late or early')
    working_hours            = fields.Field(attribute='working_hours',            column_name='Working hour')
    total_office_hours       = fields.Field(attribute='total_office_hours',       column_name='Total office hours')
    source                   = fields.Field(attribute='source',                   column_name='Source')
    date_of_joining          = fields.Field(attribute='date_of_joining',          column_name='Date of joining', widget=DateWidget(format='%d-%m-%Y'))
    employment_type          = fields.Field(attribute='employment_type',          column_name='Employment type')
    grade                    = fields.Field(attribute='grade',                    column_name='Grade')
    lattitude_longitude      = fields.Field(attribute='lattitude_longitude',      column_name='Lat long')
    level                    = fields.Field(attribute='level',                    column_name='Level')
    location                 = fields.Field(attribute='location',                 column_name='Location')
    mobile                   = fields.Field(attribute='mobile',                   column_name='Mobile number')
    region                   = fields.Field(attribute='region',                   column_name='Region')
    reporting_manager        = fields.Field(attribute='reporting_manager',        column_name='Reporting manager')
    work_email               = fields.Field(attribute='work_email',               column_name='Work email')

    class Meta:
        model = DailyAttendance
        import_id_fields = ('employee_code', 'attendance_date')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def get_instance(self, instance_loader, row):
        """
        Look up by (employee_code, attendance_date). 
        Update the existing one if found; if multiple, pick the lowest-pk.
        """
        code = row.get('employee_code')
        dt   = row.get('attendance_date')
        if not code or not dt:
            return None

        qs = DailyAttendance.objects.filter(
            employee_code=code,
            attendance_date=dt
        )
        try:
            return qs.get()
        except DailyAttendance.DoesNotExist:
            return None
        except MultipleObjectsReturned:
            return qs.order_by('pk').first()

    def before_import_row(self, row, **kwargs):
        # 1) require a non‐blank code
        raw_code = row.get('Employee Code')
        if not raw_code or pd.isna(raw_code) or not str(raw_code).strip():
            row['SKIP'] = True
            return

        # 2) parse date
        try:
            dt_raw = pd.to_datetime(row.get('Attendance date'), errors='coerce')
        except Exception:
            row['SKIP'] = True
            return
        if pd.isna(dt_raw):
            row['SKIP'] = True
            return

        dt = dt_raw.date()
        # 3) skip if too old
        if dt < date.today() - timedelta(days=50):
            row['SKIP'] = True
            return

        # 4) normalize into import_id_fields
        row['employee_code']   = str(raw_code).strip()
        row['attendance_date'] = dt

    def skip_row(self, instance, original, row, import_validation_errors=None):
        return row.get('SKIP', False) is True


@admin.register(DailyAttendance)
class DailyAttendanceAdmin(ImportExportModelAdmin, ExportMixin):
    resource_class = DailyAttendanceResource
    list_display   = ['employee_code','full_name','attendance_date','status_in_out']
    list_filter    = ('attendance_date','status_in_out','company','branch','department')
    search_fields  = ('employee_code','full_name','status_in_out')
    ordering       = ('-attendance_date',)

# ===============================================================================================================

class DailyCheckInResource(resources.ModelResource):
    # Map the spreadsheet headers to your model fields
    employee_code = fields.Field(
        attribute='employee_code',
        column_name='Employee Code'        # must match your file exactly
    )
    attendance_date = fields.Field(attribute='attendance_date',column_name='Attendance date')
    full_name = fields.Field(attribute='full_name',column_name='Full name')
    employment_status = fields.Field(attribute='employment_status',column_name='Employment status')
    company = fields.Field(attribute='company',column_name='Company')
    business_unit = fields.Field(attribute='business_unit',column_name='Business Unit')
    department = fields.Field(attribute='department',column_name='Department')
    designation = fields.Field(attribute='designation',column_name='Designation')
    branch = fields.Field(attribute='branch',column_name='Branch')
    sub_branch = fields.Field(attribute='sub_branch',column_name='Sub branch')
    shift = fields.Field(attribute='shift',column_name='Shift' )
    check_in = fields.Field(attribute='check_in',column_name='Check in')
    source = fields.Field(attribute='source',column_name='Punches with source')
    first_punch = fields.Field(attribute='first_punch', column_name='First Punch')
    last_punch = fields.Field(attribute='last_punch',column_name='Last Punch')
    raw_punch = fields.Field(attribute='raw_punch',column_name='Raw Punch')

    class Meta:
        model = DailyCheckIn
        # These must be the *model field names*, not the column_name
        import_id_fields = ('employee_code', 'attendance_date')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def before_import_row(self, row, **kwargs):
        # Retrieve and validate 'Employee Code' from the spreadsheet
        emp_code_raw = row.get('Employee Code')
        if not emp_code_raw or pd.isna(emp_code_raw) or not str(emp_code_raw).strip():
            row['SKIP'] = True
            return

        emp_code = str(emp_code_raw).strip()

        # Parse the attendance date from the spreadsheet
        try:
            attendance_date_raw = pd.to_datetime(row.get('Attendance date'), errors='coerce')
        except Exception:
            row['SKIP'] = True
            return

        if pd.isna(attendance_date_raw):
            row['SKIP'] = True
            return

        attendance_date = attendance_date_raw.date()

        # Example: skip if older than 50 days
        if attendance_date < date.today() - timedelta(days=50):
            row['SKIP'] = True
            return

        # You can do more custom validation here if needed:
        # e.g. normalizing certain strings, skipping incomplete rows, etc.

        # If everything is good, set the columns to the model fields
        row['employee_code'] = emp_code
        row['attendance_date'] = attendance_date
        # The rest are mapped automatically by the Field definitions above

    def skip_row(self, instance, original, row, import_validation_errors=None):
        return row.get('SKIP', False) is True

@admin.register(DailyCheckIn)
class DailyCheckInAdmin(ImportExportModelAdmin):
    resource_class = DailyCheckInResource
    list_display = (
        'employee_code','full_name','employment_status','company','business_unit','department','designation',
        'branch','sub_branch','attendance_date','shift','check_in','source','first_punch','last_punch','raw_punch',
    )
    search_fields = ('employee_code', 'full_name')

# ===============================================================================================================
    

class LateEarlyGoResource(resources.ModelResource):
    employee_code = fields.Field(attribute='employee_code', column_name='Employee Code')
    full_name = fields.Field(attribute='full_name', column_name='Full name')
    employment_status = fields.Field(attribute='employment_status', column_name='Employment status')
    company = fields.Field(attribute='company', column_name='Company')
    business_unit = fields.Field(attribute='business_unit', column_name='Business Unit')
    department = fields.Field(attribute='department', column_name='Department')
    designation = fields.Field(attribute='designation', column_name='Designation')
    branch = fields.Field(attribute='branch', column_name='Branch')
    sub_branch = fields.Field(attribute='sub_branch', column_name='Sub branch')
    late_early = fields.Field(attribute='late_early', column_name='Late / early')  # Note the space
    attendance_date = fields.Field(attribute='attendance_date', column_name='Attendance date')
    late_early_by_min = fields.Field(attribute='late_early_by_min', column_name='Late/early by (min)')
    shift_code = fields.Field(attribute='shift_code', column_name='Shift code')
    shift_timings = fields.Field(attribute='shift_timings', column_name='Shift timings')

    class Meta:
        model = Late_Early_Go
        import_id_fields = ('employee_code', 'attendance_date')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def before_import_row(self, row, **kwargs):
        emp_code_raw = row.get('Employee Code')
        if not emp_code_raw or pd.isna(emp_code_raw) or not str(emp_code_raw).strip():
            row['SKIP'] = True
            return

        emp_code = str(emp_code_raw).strip()

        try:
            # Adjust parsing to match dd/mm/yyyy format in Excel
            attendance_date_raw = pd.to_datetime(
                row.get('Attendance date'),
                format='%d/%m/%Y',
                errors='coerce'
            )
        except Exception:
            row['SKIP'] = True
            return

        if pd.isna(attendance_date_raw):
            row['SKIP'] = True
            return

        attendance_date = attendance_date_raw.date()

        if attendance_date < date.today() - timedelta(days=50):
            row['SKIP'] = True
            return

        row['employee_code'] = emp_code
        row['attendance_date'] = attendance_date

    def skip_row(self, instance, original, row, import_validation_errors=None):
        return row.get('SKIP', False) is True


@admin.register(Late_Early_Go)
class LateEarlyGoAdmin(ImportExportModelAdmin):
    resource_class = LateEarlyGoResource
    list_display = (
        'employee_code', 'full_name', 'employment_status', 'company', 'business_unit', 'department',
        'designation', 'branch', 'sub_branch', 'late_early', 'attendance_date',
        'late_early_by_min', 'shift_code', 'shift_timings',
    )
    search_fields = ('employee_code', 'full_name')



# ===============================================================================================================


class AttendanceRegulationResource(resources.ModelResource):
    employee_code       = fields.Field(attribute='employee_code',      column_name='Employee Code')
    full_name           = fields.Field(attribute='full_name',          column_name='Full name')
    employment_status   = fields.Field(attribute='employment_status',  column_name='Employment status')
    company             = fields.Field(attribute='company',             column_name='Company')
    business_unit       = fields.Field(attribute='business_unit',       column_name='Business Unit')
    department          = fields.Field(attribute='department',          column_name='Department')
    designation         = fields.Field(attribute='designation',         column_name='Designation')
    branch              = fields.Field(attribute='branch',              column_name='Branch')
    sub_branch          = fields.Field(attribute='sub_branch',          column_name='Sub branch')
    request_type        = fields.Field(attribute='request_type',        column_name='Request type')
    attendance_date     = fields.Field(attribute='attendance_date',    column_name='Attendance date', widget=DateWidget(format='%d/%m/%Y'))
    attendance_day      = fields.Field(attribute='attendance_day',      column_name='Attendance day')
    reason              = fields.Field(attribute='reason',              column_name='Reason')
    shift_code          = fields.Field(attribute='shift_code',          column_name='Shift code')
    shift_timings       = fields.Field(attribute='shift_timings',       column_name='Shift timings')
    actual_punch_in_out = fields.Field(attribute='actual_punch_in_out', column_name='Actual punch in/ out')
    punch_in_date       = fields.Field(attribute='punch_in_date',      column_name='Punch in (date)', widget=DateWidget(format='%d/%m/%Y'))
    punch_in_time       = fields.Field(attribute='punch_in_time',       column_name='Punch in timing')
    punch_out_date      = fields.Field(attribute='punch_out_date',     column_name='Punch out (date)', widget=DateWidget(format='%d/%m/%Y'))
    punch_out_time      = fields.Field(attribute='punch_out_time',      column_name='Punch out timing')
    remarks             = fields.Field(attribute='remarks',             column_name='Remarks')
    request_status      = fields.Field(attribute='request_status',      column_name='Request status')
    requested_by        = fields.Field(attribute='requested_by',        column_name='Request by')
    requested_on        = fields.Field(attribute='requested_on',       column_name='Request on', widget=DateWidget(format='%d/%m/%Y'))
    approved_by         = fields.Field(attribute='approved_by',         column_name='Approved by')
    approved_on         = fields.Field(attribute='approved_on',        column_name='Approved on', widget=DateWidget(format='%d/%m/%Y'))
    approver_remark     = fields.Field(attribute='approver_remark',     column_name='Approver remark')

    class Meta:
        model = AttendanceRegulation
        import_id_fields = ('employee_code', 'attendance_date')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def before_import_row(self, row, **kwargs):
        # 1) Must have Employee Code
        emp_raw = row.get('Employee Code')
        if not emp_raw or pd.isna(emp_raw) or not str(emp_raw).strip():
            row['SKIP'] = True
            return
        row['employee_code'] = str(emp_raw).strip()

        # 2) Parse Attendance date (dd/mm/Y) and skip invalid / too old
        dt = pd.to_datetime(row.get('Attendance date'), dayfirst=True, errors='coerce')
        if pd.isna(dt):
            row['SKIP'] = True
            return
        attendance_date = dt.date()
        if attendance_date < date.today() - timedelta(days=50):
            row['SKIP'] = True
            return
        row['attendance_date'] = attendance_date

        # (All other columns map automatically via their Field definitions.)

    def skip_row(self, instance, original, row, import_validation_errors=None):
        # Skip if we flagged SKIP, or if attendance_date wasn't set
        if row.get('SKIP', False):
            return True
        if not row.get('attendance_date'):
            return True
        return False


@admin.register(AttendanceRegulation)
class AttendanceRegulationAdmin(ImportExportModelAdmin):
    resource_class = AttendanceRegulationResource
    list_display = (
        'employee_code', 'full_name', 'attendance_date', 'request_type', 'reason',
        'request_status', 'requested_by', 'requested_on', 'approved_by', 'approved_on'
    )
    list_filter = ('request_status', 'company', 'branch', 'attendance_date')
    search_fields = ('employee_code', 'full_name', 'requested_by', 'request_status')
    ordering = ('-attendance_date',)



class OnDutyRequestResource(resources.ModelResource):
    employee_code = fields.Field(attribute='employee_code', column_name='Employee Code')
    full_name = fields.Field(attribute='full_name', column_name='Full name')
    employment_status = fields.Field(attribute='employment_status', column_name='Employment status')
    company = fields.Field(attribute='company', column_name='Company')
    business_unit = fields.Field(attribute='business_unit', column_name='Business Unit')
    department = fields.Field(attribute='department', column_name='Department')
    designation = fields.Field(attribute='designation', column_name='Designation')
    branch = fields.Field(attribute='branch', column_name='Branch')
    sub_branch = fields.Field(attribute='sub_branch', column_name='Sub branch')
    request_type = fields.Field(attribute='request_type', column_name='Request type')
    attendance_date = fields.Field(
        attribute='attendance_date',
        column_name='Attendance date',
        widget=DateWidget(format='%d/%m/%Y')  # or '%d-%m-%Y' or '%Y-%m-%d' as per your file
    )
    attendance_day = fields.Field(attribute='attendance_day', column_name='Attendance day')
    on_duty_type = fields.Field(attribute='on_duty_type', column_name='On duty type')
    shift_code = fields.Field(attribute='shift_code', column_name='Shift code')
    shift_timings = fields.Field(attribute='shift_timings', column_name='Shift timings')
    actual_punch_in_out = fields.Field(attribute='actual_punch_in_out', column_name='Actual punch in/ out')
    punch_in_date = fields.Field(
        attribute='punch_in_date',
        column_name='Punch in (date)',
        widget=DateWidget(format='%d/%m/%Y')
    )
    punch_out_date = fields.Field(
        attribute='punch_out_date',
        column_name='Punch out (date)',
        widget=DateWidget(format='%d/%m/%Y')
    )
    remarks = fields.Field(attribute='remarks', column_name='Remarks')
    request_status = fields.Field(attribute='request_status', column_name='Request status')
    request_by = fields.Field(attribute='request_by', column_name='Request by')
    request_on = fields.Field(
        attribute='request_on',
        column_name='Request on',
        widget=DateWidget(format='%d/%m/%Y')
    )
    pending_with = fields.Field(attribute='pending_with', column_name='Pending with')
    approved_by = fields.Field(attribute='approved_by', column_name='Approved by')
    approved_on = fields.Field(
        attribute='approved_on',
        column_name='Approved on',
        widget=DateWidget(format='%d/%m/%Y')
    )
    approver_remark = fields.Field(attribute='approver_remark', column_name='Approver remark')
    billable_type = fields.Field(attribute='billable_type', column_name='Billable type')
    punch_in_timing = fields.Field(attribute='punch_in_timing', column_name='Punch in timing')
    punch_out_timing = fields.Field(attribute='punch_out_timing', column_name='Punch out timing')

    class Meta:
        model = On_Duty_Request
        import_id_fields = ('employee_code', 'attendance_date')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def before_import_row(self, row, **kwargs):
        emp_code_raw = row.get('Employee Code')
        if not emp_code_raw or pd.isna(emp_code_raw) or not str(emp_code_raw).strip():
            row['SKIP'] = True
            return

        emp_code = str(emp_code_raw).strip()

        # Convert naive datetime to aware for `request_on`
        try:
            if row.get('Request on'):
                dt = pd.to_datetime(row['Request on'], errors='coerce')
                if pd.notna(dt):
                    row['request_on'] = timezone.make_aware(dt)
        except Exception:
            row['SKIP'] = True
            return

        # Convert naive datetime to aware for `approved_on`
        try:
            if row.get('Approved on'):
                dt2 = pd.to_datetime(row['Approved on'], errors='coerce')
                if pd.notna(dt2):
                    row['approved_on'] = timezone.make_aware(dt2)
        except Exception:
            row['SKIP'] = True
            return
        try:
            attendance_date_raw = pd.to_datetime(
                row.get('Attendance date'),
                format='%d/%m/%Y',
                errors='coerce'
            )
        except Exception:
            row['SKIP'] = True
            return

        if pd.isna(attendance_date_raw):
            row['SKIP'] = True
            return

        attendance_date = attendance_date_raw.date()

        if attendance_date < date.today() - timedelta(days=50):
            row['SKIP'] = True
            return

        row['employee_code'] = emp_code
        row['attendance_date'] = attendance_date

    def skip_row(self, instance, original, row, import_validation_errors=None):
        return row.get('SKIP', False) is True



@admin.register(On_Duty_Request)
class OnDutyRequestAdmin(ImportExportModelAdmin):
    resource_class = OnDutyRequestResource
    list_display = (
        'employee_code', 'full_name', 'company', 'department',
        'attendance_date', 'request_type', 'request_status', 'shift_code',
        'request_on', 'approved_by', 'approved_on'
    )
    list_filter = ('company', 'department', 'branch', 'request_status', 'attendance_date')
    search_fields = ('employee_code', 'full_name', 'request_status', 'request_by', 'approved_by')





class OvertimeReportResource(resources.ModelResource):
    employee_code = fields.Field(attribute='employee_code', column_name='Employee Code')
    full_name = fields.Field(attribute='full_name', column_name='Full name')
    employment_status = fields.Field(attribute='employment_status', column_name='Employment status')
    company = fields.Field(attribute='company', column_name='Company')
    business_unit = fields.Field(attribute='business_unit', column_name='Business Unit')
    department = fields.Field(attribute='department', column_name='Department')
    designation = fields.Field(attribute='designation', column_name='Designation')
    branch = fields.Field(attribute='branch', column_name='Branch')
    sub_branch = fields.Field(attribute='sub_branch', column_name='Sub branch')
    attendance_date = fields.Field(
        attribute='attendance_date',
        column_name='Attendance date',
        widget=DateWidget(format='%d/%m/%Y')
    )
    attendance_day = fields.Field(attribute='attendance_day', column_name='Attendance day')
    shift_code = fields.Field(attribute='shift_code', column_name='Shift code')
    shift = fields.Field(attribute='shift', column_name='Shift')
    shift_timings = fields.Field(attribute='shift_timings', column_name='Shift timings')
    punch_in_time = fields.Field(attribute='punch_in_time', column_name='Punch in time')
    punch_out_time = fields.Field(attribute='punch_out_time', column_name='Punch out time')
    working_hours = fields.Field(attribute='working_hours', column_name='Working hours')
    overtime_hours = fields.Field(attribute='overtime_hours', column_name='Overtime hours')
    request_status = fields.Field(attribute='request_status', column_name='Request status')
    request_by = fields.Field(attribute='request_by', column_name='Request by')
    request_on = fields.Field(
        attribute='request_on',
        column_name='Request on',
        widget=DateTimeWidget(format='%d/%m/%Y %H:%M')
    )
    pending_with = fields.Field(attribute='pending_with', column_name='Pending with')
    approved_by = fields.Field(attribute='approved_by', column_name='Approved by')
    approved_on = fields.Field(
        attribute='approved_on',
        column_name='Approved on',
        widget=DateTimeWidget(format='%d/%m/%Y %H:%M')
    )
    approver_remark = fields.Field(attribute='approver_remark', column_name='Approver remark')
    overtime_minutes = fields.Field(attribute='overtime_minutes', column_name='Overtime hours (in minutes)')

    class Meta:
        model = OvertimeReport
        import_id_fields = ('employee_code', 'attendance_date', 'request_on')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def before_import_row(self, row, **kwargs):
        emp_code_raw = row.get('Employee Code')
        if not emp_code_raw or pd.isna(emp_code_raw) or not str(emp_code_raw).strip():
            row['SKIP'] = True
            return

        emp_code = str(emp_code_raw).strip()
        row['employee_code'] = emp_code

        # Validate attendance date (should not be older than 50 days)
        try:
            attendance_date_raw = pd.to_datetime(row.get('Attendance date'), format='%d/%m/%Y', errors='coerce')
        except Exception:
            row['SKIP'] = True
            return

        if pd.isna(attendance_date_raw):
            row['SKIP'] = True
            return

        attendance_date = attendance_date_raw.date()
        if attendance_date < date.today() - timedelta(days=50):
            row['SKIP'] = True
            return

        row['attendance_date'] = attendance_date

        # --- Parse request_on as datetime (date+time) ---
        try:
            request_on_raw = row.get('Request on')
            if request_on_raw:
                # Accepts "07/06/2025 17:57" or "07/06/2025"
                dt = pd.to_datetime(request_on_raw, dayfirst=True, errors='coerce')
                if pd.notna(dt):
                    if timezone.is_naive(dt):
                        dt = timezone.make_aware(dt)
                    row['request_on'] = dt
                else:
                    row['SKIP'] = True
                    return
        except Exception:
            row['SKIP'] = True
            return

        # --- Parse approved_on as datetime (date+time) ---
        try:
            approved_on_raw = row.get('Approved on')
            if approved_on_raw:
                dt2 = pd.to_datetime(approved_on_raw, dayfirst=True, errors='coerce')
                if pd.notna(dt2):
                    if timezone.is_naive(dt2):
                        dt2 = timezone.make_aware(dt2)
                    row['approved_on'] = dt2
        except Exception:
            row['SKIP'] = True
            return
    

@admin.register(OvertimeReport)
class OvertimeReportAdmin(ImportExportModelAdmin):
    resource_class = OvertimeReportResource
    list_display = (
        'employee_code', 'full_name', 'attendance_date', 'attendance_day',
        'shift_code', 'punch_in_time', 'punch_out_time',
        'working_hours', 'overtime_hours', 'request_status', 'request_on', 'approved_on'
    )
    list_filter = ('company', 'branch', 'request_status', 'attendance_date')
    search_fields = ('employee_code', 'full_name', 'department', 'designation')
    



class ShortLeaveResource(resources.ModelResource):
    employee_code = fields.Field(attribute='employee_code', column_name='Employee Code')
    full_name = fields.Field(attribute='full_name', column_name='Full name')
    employment_status = fields.Field(attribute='employment_status', column_name='Employment status')
    company = fields.Field(attribute='company', column_name='Company')
    business_unit = fields.Field(attribute='business_unit', column_name='Business Unit')
    department = fields.Field(attribute='department', column_name='Department')
    designation = fields.Field(attribute='designation', column_name='Designation')
    branch = fields.Field(attribute='branch', column_name='Branch')
    sub_branch = fields.Field(attribute='sub_branch', column_name='Sub branch')
    request_type = fields.Field(attribute='request_type', column_name='Request type')
    attendance_date = fields.Field(attribute='attendance_date', column_name='Attendance date', widget=DateWidget(format='%d/%m/%Y'))
    attendance_day = fields.Field(attribute='attendance_day', column_name='Attendance day')
    shift_code = fields.Field(attribute='shift_code', column_name='Shift code')
    shift_timings = fields.Field(attribute='shift_timings', column_name='Shift timings')
    actual_punch_in_out = fields.Field(attribute='actual_punch_in_out', column_name='Actual punch in/ out')
    punch_in_date = fields.Field(attribute='punch_in_date', column_name='Punch in (date)', widget=DateWidget(format='%d/%m/%Y'))
    punch_in_timing = fields.Field(attribute='punch_in_timing', column_name='Punch in timing')
    punch_out_date = fields.Field(attribute='punch_out_date', column_name='Punch out (date)', widget=DateWidget(format='%d/%m/%Y'))
    punch_out_timing = fields.Field(attribute='punch_out_timing', column_name='Punch out timing')
    remarks = fields.Field(attribute='remarks', column_name='Remarks')
    request_status = fields.Field(attribute='request_status', column_name='Request status')
    request_by = fields.Field(attribute='request_by', column_name='Request by')
    request_on = fields.Field(attribute='request_on', column_name='Request on', widget=DateWidget(format='%d/%m/%Y'))
    pending_with = fields.Field(attribute='pending_with', column_name='Pending with')
    approved_by = fields.Field(attribute='approved_by', column_name='Approved by')
    approved_on = fields.Field(attribute='approved_on', column_name='Approved on', widget=DateWidget(format='%d/%m/%Y'))
    approver_remark = fields.Field(attribute='approver_remark', column_name='Approver remark')

    class Meta:
        model = ShortLeave
        import_id_fields = ('employee_code', 'attendance_date')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def before_import_row(self, row, **kwargs):
        emp_code = str(row.get('Employee Code')).strip()
        if not emp_code or pd.isna(emp_code):
            row['SKIP'] = True
            return

        try:
            attendance_date = pd.to_datetime(row.get('Attendance date'), dayfirst=True, errors='coerce')
            if pd.isna(attendance_date) or attendance_date.date() < date.today() - timedelta(days=50):
                row['SKIP'] = True
                return
            row['attendance_date'] = attendance_date.date()
        except:
            row['SKIP'] = True

    def skip_row(self, instance, original, row, import_validation_errors=None):
        return row.get('SKIP', False) is True
    

@admin.register(ShortLeave)
class ShortLeaveAdmin(ImportExportModelAdmin):
    resource_class = ShortLeaveResource
    list_display = ('employee_code', 'full_name', 'attendance_date', 'shift_code', 'request_status', 'approved_by')
    list_filter = ('company', 'branch', 'request_status', 'attendance_date')
    search_fields = ('employee_code', 'full_name', 'designation')




class HelpdeskTicketResource(resources.ModelResource):
    employee_code = fields.Field(attribute='employee_code', column_name='Employee Code')
    full_name = fields.Field(attribute='full_name', column_name='Full name')
    employment_status = fields.Field(attribute='employment_status', column_name='Employment status')
    company = fields.Field(attribute='company', column_name='Company')
    business_unit = fields.Field(attribute='business_unit', column_name='Business Unit')
    department = fields.Field(attribute='department', column_name='Department')
    designation = fields.Field(attribute='designation', column_name='Designation')
    branch = fields.Field(attribute='branch', column_name='Branch')
    sub_branch = fields.Field(attribute='sub_branch', column_name='Sub branch')
    ticket_id = fields.Field(attribute='ticket_id', column_name='Ticket id')
    ticket_details = fields.Field(attribute='ticket_details', column_name='Ticket details')
    category = fields.Field(attribute='category', column_name='Category')
    sub_category = fields.Field(attribute='sub_category', column_name='Sub category')
    priority = fields.Field(attribute='priority', column_name='Priority')
    status = fields.Field(attribute='status', column_name='Status')
    raised_on = fields.Field(attribute='raised_on', column_name='Raised on')
    assigned_to = fields.Field(attribute='assigned_to', column_name='Assigned to')
    pending_with = fields.Field(attribute='pending_with', column_name='Pending with')
    closed_by = fields.Field(attribute='closed_by', column_name='Closed by')
    closed_on = fields.Field(attribute='closed_on', column_name='Closed on')
    is_closed_on_time = fields.Field(attribute='is_closed_on_time', column_name='Is closed on time')
    feedback_rating = fields.Field(attribute='feedback_rating', column_name='Feedback rating')
    was_ticket_escalated = fields.Field(attribute='was_ticket_escalated', column_name='Was ticket escalated')
    escalated_to = fields.Field(attribute='escalated_to', column_name='Escalated to')
    rca = fields.Field(attribute='rca', column_name='Rca')
    time_to_close = fields.Field(attribute='time_to_close', column_name='Time to close')

    class Meta:
        model = Helpdesk_Ticket
        import_id_fields = ('employee_code', 'ticket_id')
        exclude = ('id',)
        skip_unchanged = True
        report_skipped = True

    def before_import_row(self, row, **kwargs):
        # Validate Employee Code
        emp_code_raw = row.get('Employee Code')
        if not emp_code_raw or pd.isna(emp_code_raw) or not str(emp_code_raw).strip():
            row['SKIP'] = True
            return
        row['employee_code'] = str(emp_code_raw).strip()

        # Convert and validate Raised On
        try:
            if row.get('Raised on'):
                dt = pd.to_datetime(row['Raised on'], errors='coerce')
                if pd.notna(dt):
                    row['raised_on'] = timezone.make_aware(dt)
        except Exception:
            row['SKIP'] = True
            return

        # Convert and validate Closed On
        try:
            if row.get('Closed on'):
                dt2 = pd.to_datetime(row['Closed on'], errors='coerce')
                if pd.notna(dt2):
                    row['closed_on'] = timezone.make_aware(dt2)
        except Exception:
            row['SKIP'] = True
            return

        # Optional check: Skip very old records
        if 'Raised on' in row:
            try:
                raised_on_date = pd.to_datetime(row['Raised on'], errors='coerce')
                if pd.notna(raised_on_date) and raised_on_date.date() < date.today() - timedelta(days=180):
                    row['SKIP'] = True
            except Exception:
                row['SKIP'] = True

    def skip_row(self, instance, original, row, import_validation_errors=None):
        return row.get('SKIP', False) is True
    


@admin.register(Helpdesk_Ticket)
class HelpdeskTicketAdmin(ImportExportModelAdmin):
    resource_class = HelpdeskTicketResource
    list_display = (
        'ticket_id', 'employee_code', 'full_name', 'company', 'department',
        'status', 'priority', 'raised_on', 'closed_by', 'closed_on'
    )
    list_filter = ('company', 'branch', 'status', 'priority', 'raised_on', 'closed_on')
    search_fields = ('ticket_id', 'employee_code', 'full_name', 'assigned_to', 'closed_by')