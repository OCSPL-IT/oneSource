from django.contrib import admin
from .models import RDMaster, R_and_D_Moisture
from import_export import resources
from import_export.admin import ImportExportModelAdmin


class RDMasterResource(resources.ModelResource):
    class Meta:
        model = RDMaster
        fields = ('category', 'name')
        import_id_fields = ('category', 'name')  # To avoid duplicate imports


@admin.register(RDMaster)
class RDMasterAdmin(ImportExportModelAdmin, admin.ModelAdmin):
    resource_class = RDMasterResource
    list_display = ("name", "category")
    list_filter = ("category",)
    search_fields = ("name",)
    ordering = ("category", "name")
     

@admin.register(R_and_D_Moisture)
class R_and_D_MoistureAdmin(admin.ModelAdmin):
    list_display = (
        "entry_date", "eln_id", "product_name", "batch_no",
        "unit", "instrument", "analysed_by", "moisture_percent",
        "completed_date"
    )
    list_filter = ("entry_date", "product_name", "unit", "instrument", "analysed_by")
    search_fields = ("eln_id", "batch_no", "product_name", "analysed_by__name")
    date_hierarchy = "entry_date"
    autocomplete_fields = ["unit", "instrument", "analysed_by"]

    # Optional: to make related ForeignKeys display as "name" in dropdowns
    raw_id_fields = ["unit", "instrument", "analysed_by"]

