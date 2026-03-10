from django.db import models


class CapexGrnLine(models.Model):
    batch_no = models.CharField( "BatchNo",max_length=50, blank=True,  null=True,)
    location = models.CharField("Location", max_length=100,  blank=True,  null=True, )
    virtual_location = models.CharField( "Virtual Location", max_length=100, blank=True, null=True, )
    item_code = models.CharField( max_length=50, db_index=True, )
    item_name = models.CharField( max_length=255,)
    doc_no = models.CharField("DocNo",max_length=50,  db_index=True, )
    txn_total = models.DecimalField("TxnTotal", max_digits=18, decimal_places=2,)
    quantity = models.DecimalField( "Quantity", max_digits=18, decimal_places=3,  )
    rate = models.DecimalField( "Rate",  max_digits=18,  decimal_places=4, )
    rate_amount = models.DecimalField( "Rate Amount",  max_digits=18,decimal_places=2, )
    total_amount = models.DecimalField( "Total Amount", max_digits=18,  decimal_places=2, )

    class Meta:
        db_table = "capex_grn_line"   # or name of your table / view
        indexes = [
            models.Index(fields=["doc_no"]),
            models.Index(fields=["item_code"]),
            models.Index(fields=["batch_no"]),
        ]

    def __str__(self):
        return f"{self.doc_no} | {self.item_code} | {self.batch_no or ''}".strip()



class MaterialIssueLine(models.Model):
    """Projection of Material Issue (TXNTYP 987) – one row per line."""
    doc_date = models.DateField("DocDate", blank=True, null=True, help_text="Document date" )
    doc_no = models.CharField("DocNumber",max_length=100, blank=True, null=True, help_text="Document Number")
    material_requisition_date = models.DateField("Material Requisition Date",blank=True,  null=True, help_text="Material requisition date from header CF" )
    batch_no = models.CharField("BatchNo", max_length=100, blank=True,  null=True,  )
    location_from = models.CharField("Location From", max_length=200, blank=True, null=True, )
    virtual_location = models.CharField( "Virtual Location", max_length=200, blank=True, null=True, )
    item_code = models.CharField( max_length=50, db_index=True, )
    item_name = models.CharField( max_length=255,  )
    quantity = models.DecimalField(  "Quantity", max_digits=18,   decimal_places=3,)
    txn_total = models.DecimalField( "TxnTotal", max_digits=18,decimal_places=2, help_text="Header-level total (d.dTotal)", )

    class Meta:
        db_table = "material_issue_line"  # change to your actual table / view name
        verbose_name = "Material Issue Line"
        verbose_name_plural = "Material Issue Lines"
        indexes = [
            models.Index(fields=["doc_date"]),
            models.Index(fields=["item_code"]),
            models.Index(fields=["batch_no"]),
            models.Index(fields=["location_from"]),
        ]

    def __str__(self):
        return f"{self.doc_date} | {self.item_code} | {self.batch_no or ''}".strip()





class LocationStockTransferCapex(models.Model):
    location_transfer_capex_no = models.CharField(max_length=100)
    location_transfer_capex_date = models.DateField()

    location = models.CharField(max_length=255, null=True, blank=True)
    to_location = models.CharField(max_length=255, null=True, blank=True)

    item_code = models.CharField(max_length=100)
    item_name = models.CharField(max_length=255)
    batch_no = models.CharField(max_length=100, null=True, blank=True)

    transfer_quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    issue_value = models.DecimalField(max_digits=18, decimal_places=3, default=0)

    virtual_location = models.CharField(max_length=255, null=True, blank=True)

    def save(self, *args, **kwargs):
        if not self.location and self.from_location:
            self.location = self.from_location
        super().save(*args, **kwargs)

    class Meta:
        db_table = "location_stock_transfer_capex"
        indexes = [
            models.Index(fields=["location_transfer_capex_no"]),
            models.Index(fields=["location_transfer_capex_date"]),
            models.Index(fields=["item_code"]),
            models.Index(fields=["batch_no"]),
        ]

    def __str__(self):
        return f"{self.location_transfer_capex_no} | {self.item_code} | {self.batch_no or ''}".strip()