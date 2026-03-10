# products/admin.py

from django.contrib import admin
from .models import (
    Product, Spec, AppearanceOption,
    QCEntry, SpecEntry,
    LocalItemMaster, LocalEquipmentMaster,
    BmrIssue, LocalBOMDetail,SampleDescriptionOption
)
from .models import AnalyticalDowntime
from .models import QCInstrument
from .models import AlfaProductMaster

class AppearanceOptionAdmin(admin.ModelAdmin):
    search_fields = ["name"]
    list_display = ["name"]

class SpecInline(admin.TabularInline):
    model = Spec
    extra = 0

class ProductAdmin(admin.ModelAdmin):
    list_display = ["name", "code", "item_type", "stages"]
    search_fields = ["name", "code", "item_type", "stages"]
    list_filter = ["item_type"]
    filter_horizontal = ("appearance_options",)
    inlines = [SpecInline]

class SpecAdmin(admin.ModelAdmin):
    list_display = ["product", "name", "spec_type", "min_val", "max_val"]
    list_filter = ["spec_type", "product"]
    search_fields = ["name", "product__name"]
    autocomplete_fields = ["product"]

class SpecEntryInline(admin.TabularInline):
    model = SpecEntry
    extra = 0

class QCEntryAdmin(admin.ModelAdmin):
    list_display = [
        "entry_no", "product", "stage", "batch_no", "status", "entry_date", "created_by"
    ]
    search_fields = ["entry_no", "product__name", "batch_no", "block"]
    list_filter = ["status", "stage", "decision_status", "entry_date", "product"]
    autocomplete_fields = ["product", "created_by", "qc_completed_by"]
    inlines = [SpecEntryInline]

class SpecEntryAdmin(admin.ModelAdmin):
    list_display = ["qc_entry", "spec", "value", "remark"]
    list_filter = ["spec", "remark"]
    search_fields = ["qc_entry__entry_no", "spec__name", "value"]


@admin.register(SampleDescriptionOption)
class SampleDescriptionOptionAdmin(admin.ModelAdmin):
    list_display = ('name','created_at')
    search_fields = ('name',)





class LocalItemMasterAdmin(admin.ModelAdmin):
    list_display = ["product_id", "product_name", "item_type"]
    search_fields = ["product_id", "product_name", "item_type"]
    ordering = ["product_name"]

class LocalEquipmentMasterAdmin(admin.ModelAdmin):
    list_display = ["eqp_code", "eqp_name", "block_name", "tag_no", "unit_code"]
    search_fields = ["eqp_code", "eqp_name", "block_name", "tag_no", "unit_code"]
    ordering = ["eqp_name"]

class BmrIssueAdmin(admin.ModelAdmin):
    list_display = [
        "bmr_issue_no", "line_no", "bmr_issue_date", "fg_name", "op_batch_no",
        "product_name", "block", "item_type", "item_code", "item_name", "uom", "batch_quantity"
    ]
    search_fields = [
        "bmr_issue_no", "fg_name", "op_batch_no", "product_name",
        "block", "item_code", "item_name"
    ]
    list_filter = ["bmr_issue_date", "item_type", "fg_name"]
    ordering = ["-bmr_issue_date", "bmr_issue_no"]

class LocalBOMDetailAdmin(admin.ModelAdmin):
    list_display = [
        "sr_no", "item_name", "itm_type", "fg_name", "item_code",
        "quantity", "bom_code", "bom_name", "type", "bom_item_code", "unit", "bom_qty", "cflag"
    ]
    search_fields = [
        "item_name", "item_code", "bom_code", "bom_name", "bom_item_code"
    ]
    list_filter = ["itm_type", "type", "unit"]
    ordering = ["sr_no"]

admin.site.register(Product, ProductAdmin)
admin.site.register(Spec, SpecAdmin)
admin.site.register(AppearanceOption, AppearanceOptionAdmin)
admin.site.register(QCEntry, QCEntryAdmin)
admin.site.register(SpecEntry, SpecEntryAdmin)
admin.site.register(LocalItemMaster, LocalItemMasterAdmin)
admin.site.register(LocalEquipmentMaster, LocalEquipmentMasterAdmin)
admin.site.register(BmrIssue, BmrIssueAdmin)
admin.site.register(LocalBOMDetail, LocalBOMDetailAdmin)

# -------------------------
# Daily QA Report admin (moved from QUALITY/admin_daily_report.py)
# -------------------------

from django.contrib import admin
from .models import DailyQAReport, IncomingMaterial, PDLSample


class InlineBase(admin.TabularInline):
    extra = 0
    show_change_link = True


class IncomingInline(InlineBase):
    model = IncomingMaterial


class PDLInline(InlineBase):
    model = PDLSample


@admin.register(DailyQAReport)
class DailyQAReportAdmin(admin.ModelAdmin):
    date_hierarchy = "report_date"
    list_display = (
        "report_date",
        "ftr_percent",
        "analytical_downtime_hrs",
        "finished_goods_inspections",
    )
    search_fields = ("report_date",)
    list_filter = ("report_date",)
    inlines = [IncomingInline, PDLInline]

# -------------------------------------------------------------------------------
# CustomerComplaint
# -------------------------------------------------------------------------------
from .models import CustomerComplaint

@admin.register(CustomerComplaint)
class CustomerComplaintAdmin(admin.ModelAdmin):
    list_display = ("complaint_date", "complaint_no", "product_name", "customer_name", "status")
    search_fields = ("complaint_no", "product_name", "customer_name")
    list_filter = ("status", "complaint_date")

# ----------------------------------------------------------------------------
#           AnalyticalDowntime
# ----------------------------------------------------------------------------

@admin.register(AnalyticalDowntime)
class AnalyticalDowntimeAdmin(admin.ModelAdmin):
    list_display = ("incident_no","instrument_id","category","status","start_at","end_at")
    list_filter  = ("status","category","instrument_id")
    search_fields= ("incident_no","instrument_id","short_reason","detail_reason","product_name","batch_no")

@admin.register(QCInstrument)
class QCInstrumentAdmin(admin.ModelAdmin):
    list_display  = ("instument_id","name", "code", "category", "is_active")
    list_filter   = ("category", "is_active")
    search_fields = ("name", "code")

@admin.register(AlfaProductMaster)
class AlfaProductAdmin(admin.ModelAdmin):
    list_display  = ("alfa_name", "finished_product_name", "is_active")
    search_fields = ("alfa_name", "finished_product_name")
    list_filter   = ("is_active",)
