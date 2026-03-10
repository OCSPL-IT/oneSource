from django.contrib import admin
from .models import GeneralEffluent


# Register your models here.
@admin.register(GeneralEffluent)
class GeneralEffluentAdmin(admin.ModelAdmin):
    list_display = ('id', 'record_date', 'location', 'effluent_nature', 'actual_quantity')
    list_filter = ('location', 'effluent_nature', 'record_date')
    search_fields = ('location', 'effluent_nature')
    ordering = ('-record_date',)



    
# ETP/admin.py
from django.contrib import admin
from .models import EffluentTank, EffluentOpeningBalance

@admin.register(EffluentTank)
class EffluentTankAdmin(admin.ModelAdmin):
    list_display = ("name", "capacity")
    search_fields = ("name",)

@admin.register(EffluentOpeningBalance)
class EffluentOpeningBalanceAdmin(admin.ModelAdmin):
    list_display = ("tank", "month", "opening_balance")
    list_filter  = ("month", "tank")
    search_fields = ("tank__name",)
