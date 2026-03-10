from django.contrib import admin
from .models import (
    MEEReadingCategory,
    MEEReadingSubCategory,
    MEEDailyReading,
)


# ── Inlines ──────────────────────────────────────────────────────
class MEEReadingSubCategoryInline(admin.TabularInline):
    model = MEEReadingSubCategory
    extra = 1
    fields = ("name",)
    show_change_link = True


# ── Category Admin ───────────────────────────────────────────────
@admin.register(MEEReadingCategory)
class MEEReadingCategoryAdmin(admin.ModelAdmin):
    list_display = ("name", "unit", "order")
    list_editable = ("order",)          # 👈 change order directly in list
    search_fields = ("name", "unit")
    inlines = [MEEReadingSubCategoryInline]



# ── SubCategory Admin ────────────────────────────────────────────
@admin.register(MEEReadingSubCategory)
class MEEReadingSubCategoryAdmin(admin.ModelAdmin):
    list_display = ("name", "category")
    list_filter = ("category",)
    search_fields = ("name", "category__name")


# ── Daily Reading Admin ─────────────────────────────────────────
@admin.register(MEEDailyReading)
class MEEDailyReadingAdmin(admin.ModelAdmin):
    list_display = (
        "reading_date",
        "subcategory",
        "get_category",
        "value",
        "entered_by",
        "created_at",
    )
    list_filter = ("reading_date", "subcategory__category")
    search_fields = (
        "subcategory__name",
        "subcategory__category__name",
        "entered_by__username",
    )
    date_hierarchy = "reading_date"
    autocomplete_fields = ("subcategory", "entered_by")
    ordering = ("-reading_date", "subcategory__category__name", "subcategory__name")

    def get_category(self, obj):
        return obj.subcategory.category
    get_category.short_description = "Category"
    get_category.admin_order_field = "subcategory__category__name"
