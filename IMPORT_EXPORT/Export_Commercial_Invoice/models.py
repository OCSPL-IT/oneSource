# export/models.py
from django.db import models


class InvoicePostShipment(models.Model):
    # -------- Header ----------
    invoice_number = models.CharField("Invoice No.", max_length=50)
    transaction_type = models.CharField("Transaction Type", max_length=100,null=True, blank=True)
    invoice_date = models.DateField("Invoice Date")
    buyers_order_no = models.CharField("Buyer's Order No.", max_length=50,null=True, blank=True)
    buyers_order_date = models.DateField("Buyer's Order Date",null=True, blank=True)
    # -------- Consignee / Notify ----------
    consignee_name = models.CharField("Consignee Name", max_length=255,null=True, blank=True)
    consignee_address = models.TextField("Consignee Address",null=True, blank=True)

    notify_party1_name = models.CharField("Notify Party 1 Name", max_length=255,null=True, blank=True)
    notify_party1_address = models.TextField("Notify Party 1 Address", null=True, blank=True)

    notify_party2_name = models.CharField("Notify Party 2 Name", max_length=255,null=True, blank=True)
    notify_party2_address = models.TextField("Notify Party 2 Address",null=True, blank=True)

    # -------- Origin / Destination ----------
    country_origin = models.CharField("Country of Origin of Goods", max_length=100,null=True, blank=True)
    country_destination = models.CharField("Country of Final Destination", max_length=100,null=True, blank=True)
    district_origin = models.CharField("District of Origin of Goods", max_length=100,null=True, blank=True)
    state_origin = models.CharField("State of Origin of Goods", max_length=100,null=True, blank=True)

    vessel_name_no = models.CharField("Vessel Name & No.", max_length=100,null=True, blank=True)
    port_loading = models.CharField("Port of Loading", max_length=100,null=True, blank=True)
    port_discharge = models.CharField("Port of Discharge", max_length=100,null=True, blank=True)
    final_destination = models.CharField("Final Destination", max_length=100,null=True, blank=True)
    preferential_agreement = models.CharField("Preferential Agreement", max_length=10, default="NO", null=True, blank=True)
    standard_unit_qty_code = models.CharField( "Standard Unit Quantity Code", max_length=20, default="KGS", null=True, blank=True)


    delivery = models.CharField(max_length=200, null=True, blank=True)
    shipment_mode = models.CharField(max_length=50, null=True, blank=True)
    payment_terms = models.CharField(max_length=200, null=True, blank=True)
    due_date = models.CharField("Payment Due Date (text)", max_length=50, null=True, blank=True)


    bank_name = models.CharField(max_length=200, null=True, blank=True)
    bank_account_no = models.CharField(max_length=50,null=True, blank=True)
    ad_code = models.CharField(max_length=50, null=True, blank=True)
    swift_code = models.CharField(max_length=50, null=True, blank=True)
    bank_address = models.TextField(null=True, blank=True)
    # -------- Marks & Nos. / Container ----------
    product_name = models.CharField(max_length=50,null=True, blank=True)
    container_no = models.CharField("Container No.", max_length=50,null=True, blank=True)
    merks_and_container_no = models.CharField("Merks/No & Container No.", max_length=300, blank=True)
    merks_and_container_no1 = models.CharField("Merks/No & Container No.1", max_length=300, blank=True)
    # -------- No. & Kind of Packing ----------
    packing_details = models.TextField("No. & Kind of Packing", blank=True, null=True)
    # -------- Description of Goods ----------
    description_of_goods = models.CharField("Description of Goods", max_length=70, null=True, blank=True)
    item_no = models.CharField("Item No.", max_length=50,null=True, blank=True)
    hsn_no = models.CharField("HSN No.", max_length=50,null=True, blank=True)
    quantity = models.DecimalField("Quantity", max_digits=12, decimal_places=2)
    quantity_unit = models.CharField("Quantity Unit", max_length=50, default="KILOGRAMS")
    conversion_rate = models.DecimalField("Conversion Rate", max_digits=18, decimal_places=2, null=True, blank=True)
    rate_usd = models.DecimalField("Rate (US$)", max_digits=12, decimal_places=4)
    amount_usd = models.DecimalField("Amount (US$)", max_digits=12, decimal_places=2)
    gross_wt = models.CharField("Gross wt", max_length=100, null=True, blank=True)
    # -------- BL / Shipping Bill ----------
    bl_number = models.CharField("Bill of Lading Number", max_length=50)
    bl_date = models.DateField("Bill of Lading Date")
    shipping_bill_no = models.CharField("Shipping Bill Number", max_length=50)
    shipping_bill_date = models.DateField("Shipping Bill Date")
    attachment = models.BinaryField("Attachment", null=True, blank=True, editable=False)
    attachment_name = models.CharField("Attachment file name", max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "invoice_post_shipment"
        ordering = ["-invoice_date", "invoice_number"]

    def __str__(self):
        return f"{self.invoice_number} ({self.invoice_date})"
    
