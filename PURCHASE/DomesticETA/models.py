from django.db import models


class DomesticETATracking(models.Model):
    STATUS_CHOICES = [
        ("Pending",   "Pending"),
        ("In Transit", "In Transit"),
        ("Cleared",   "Cleared"),
        ("Rejection", "Rejection"),
        ("Hold",      "Hold"),
    ]
    EVALUATION_CHOICES=[
        ("Safety","Safety"),
        ("Service","Service")
    ]
    PoNumber        = models.CharField(max_length=50)
    Status          = models.CharField(max_length=20,choices=STATUS_CHOICES,default="Pending")
    RequiredDate    = models.DateField(blank=True, null=True,verbose_name="Required Date at Plant")
    ETDDate         = models.DateField(blank=True, null=True, verbose_name="ETD Date")
    RevisedETADate  = models.DateField(blank=True, null=True, verbose_name="Revised ETA Date")
    RawMaterial     = models.CharField(max_length=255)
    Packing         = models.CharField(max_length=100, blank=True, null=True)
    Qty             = models.DecimalField(max_digits=12, decimal_places=3)
    Supplier        = models.CharField(max_length=255)
    LiftingLocation = models.CharField(max_length=255, blank=True, null=True)
    TransporterName = models.CharField(max_length=255, blank=True, null=True)
    VehicleNo       = models.CharField(max_length=50, blank=True, null=True)
    LRNo            = models.CharField(max_length=50, blank=True, null=True)
    DriverNo        = models.CharField(max_length=20, blank=True, null=True)
    Evaluation      = models.CharField(max_length=50,choices=EVALUATION_CHOICES, blank=True, null=True)
    FreightCharges  = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True,verbose_name="Freight Charges" )
    # 🔹 Single binary field to store the image bytes
    Photos          = models.BinaryField(blank=True, null=True)
    InvoiceNoRemark = models.CharField(max_length=255,default="Pending",verbose_name="Invoice No / Remark",)
    InvoiceDate     = models.DateField(blank=True, null=True)
    Remark = models.CharField(max_length=1000, blank=True, null=True)
    created_at      = models.DateTimeField(auto_now_add=True)
    updated_at      = models.DateTimeField(auto_now=True)


    class Meta:
        db_table = "domestic_eta_tracking"
        verbose_name = "Domestic ETA Tracking"
        verbose_name_plural = "Domestic ETA Tracking"
        indexes = [
            models.Index(fields=["PoNumber"]),
            models.Index(fields=["Status"]),
            models.Index(fields=["RequiredDate"]),
            models.Index(fields=["Supplier"]),
            models.Index(fields=["TransporterName"]),
        ]

    def __str__(self):
        return f"{self.PoNumber} - {self.RawMaterial}"
