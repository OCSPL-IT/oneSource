from django.contrib import admin
from .models import Downtime

@admin.register(Downtime)
class DowntimeAdmin(admin.ModelAdmin):
    # Fields to display in the admin list view
    list_display = (
        'eqpt_id',
        'eqpt_name',
        'date',
        'idle',
        'start_date',
        'end_date',
        'start_time',
        'end_time',
        'block',
    )
    
    # Fields you can filter by in the right sidebar
    list_filter = ('idle', 'date', 'block', 'downtime_dept', 'downtime_category')
    
    # Fields you can search by (shows a search box above the list)
    search_fields = ('eqpt_id', 'eqpt_name', 'product_name', 'batch_no', 'reason')

    # Optional: default ordering in the admin
    ordering = ('-date',)
