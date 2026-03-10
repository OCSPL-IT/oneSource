from django.db import models
from decimal import Decimal, InvalidOperation


class BioreactorDailyReading(models.Model):
    date = models.DateField(db_column="Date")
    bioreactor_feed = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True, db_column="Bioreactor_Feed", verbose_name="Bioreactor Feed" )
    # ------------ Bioreactor 1 ------------
    bioreactor_1_ph = models.DecimalField(
        max_digits=6, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_1_ph", verbose_name="Bioreactor-1-Ph",
    )
    bioreactor_1_cod = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_1_cod", verbose_name="Bioreactor-1-COD",
    )
    bioreactor_1_mlss = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_1_mlss", verbose_name="Bioreactor-1-MLSS",
    )
    bioreactor_1_mlvss = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_1_mlvss", verbose_name="Bioreactor-1-MLVSS",
    )
    bioreactor_1_svi = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_1_svi", verbose_name="Bioreactor-1-SVI",
    )
    bioreactor_1_do = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_1_do", verbose_name="Bioreactor-1-DO",
    )
    bioreactor_1_fm_ratio = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_1_FM_ratio",
        verbose_name="Bioreactor 1 F/M ratio",
    )

    # ------------ Bioreactor 2 ------------
    bioreactor_2_ph = models.DecimalField(
        max_digits=6, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_2_ph", verbose_name="Bioreactor-2-Ph",
    )
    bioreactor_2_cod = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_2_cod", verbose_name="Bioreactor-2-COD",
    )
    bioreactor_2_mlss = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_2_mlss", verbose_name="Bioreactor-2-MLSS",
    )
    bioreactor_2_mlvss = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,verbose_name="Bioreactor-2-MLVSS",
        db_column="Bioreactor_2_mlvss",
    )
    bioreactor_2_svi = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,verbose_name="Bioreactor-2-SVI",
        db_column="Bioreactor_2_svi",
    )
    bioreactor_2_do = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,verbose_name="Bioreactor-2-DO",
        db_column="Bioreactor_2_do",
    )
    bioreactor_2_fm_ratio = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_2_FM_ratio",
        verbose_name="Bioreactor 2 F/M ratio",
    )

    # ------------ Polishing Tank ------------
    polishing_tank_ph = models.DecimalField(
        max_digits=6, decimal_places=3, null=True, blank=True,
        db_column="Polishing_tank_ph",verbose_name="Polishing tank PH"
    )
    # You had both `Polishing_tank_ph` and `Polishing_Tank_ph` listed.
    polishing_tank_tss = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Polishing_Tank_tss",verbose_name="Polishing tank TSS"
    )
    polishing_tank_tds = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Polishing_Tank_tds",verbose_name="Polishing tank TDS",
    )
    polishing_tank_cod = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Polishing_Tank_cod",verbose_name="Polishing tank COD",
    )

    # ------------ Bioreactor Feed (detailed) ------------
    bioreactor_feed_ph = models.DecimalField(
        max_digits=6, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_Feed_ph",verbose_name="Bioreactor Feed Ph"
    )
    bioreactor_feed_temp = models.DecimalField(
        max_digits=6, decimal_places=2, null=True, blank=True,
        db_column="Bioreactor_Feed_temp",verbose_name="Bioreactor Feed TEMP"
    )
    bioreactor_feed_tds = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_Feed_tds",verbose_name="Bioreactor Feed TDS"
    )
    bioreactor_feed_tss = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_Feed_tss",verbose_name="Bioreactor Feed TSS"
    )
    bioreactor_feed_cod = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True,
        db_column="Bioreactor_Feed_cod", verbose_name="Bioreactor Feed COD"
    )
    class Meta:
        db_table = "bioreactor_daily_reading"
        ordering = ["-date"]

    def __str__(self):
        return f"Bioreactor reading on {self.date}"
    
    # --- NEW: internal helper to calculate F/M ratios ---
    def _recalculate_fm_ratios(self):
        """
        Recalculate and set bioreactor_1_fm_ratio and bioreactor_2_fm_ratio
        from the current instance fields.

        Formulas provided:

          bioreactor_1_fm_ratio = ((bioreactor_feed*bioreactor_feed_cod)/1000)
                                  /((200*bioreactor_1_mlss)/1000)

          bioreactor_2_fm_ratio = (bioreactor_feed*bioreactor_1_cod)
                                  /(200*bioreactor_2_mlss/1000)
        """
        try:
            # ---- Bioreactor 1 FM ratio ----
            if (
                self.bioreactor_feed is not None
                and self.bioreactor_feed_cod is not None
                and self.bioreactor_1_mlss is not None
                and self.bioreactor_1_mlss != 0
            ):
                num1 = (self.bioreactor_feed * self.bioreactor_feed_cod) / Decimal("1000")
                den1 = (Decimal("200") * self.bioreactor_1_mlss) / Decimal("1000")
                if den1 != 0:
                    self.bioreactor_1_fm_ratio = (num1 / den1).quantize(Decimal("0.001"))
                else:
                    self.bioreactor_1_fm_ratio = None
            else:
                self.bioreactor_1_fm_ratio = None

            # ---- Bioreactor 2 FM ratio ----
            if (
                self.bioreactor_feed is not None
                and self.bioreactor_1_cod is not None      # as per your formula
                and self.bioreactor_2_mlss is not None
                and self.bioreactor_2_mlss != 0
            ):
                num2 = self.bioreactor_feed * self.bioreactor_1_cod
                den2 = (Decimal("200") * self.bioreactor_2_mlss) / Decimal("1000")
                if den2 != 0:
                    self.bioreactor_2_fm_ratio = (num2 / den2).quantize(Decimal("0.001"))
                else:
                    self.bioreactor_2_fm_ratio = None
            else:
                self.bioreactor_2_fm_ratio = None

        except InvalidOperation:
            # any bad Decimal math -> clear the ratios
            self.bioreactor_1_fm_ratio = None
            self.bioreactor_2_fm_ratio = None

    def save(self, *args, **kwargs):
        # always recompute before saving
        self._recalculate_fm_ratios()
        super().save(*args, **kwargs)
    
    
    
    
    

class BioreactorChemical(models.Model):
    """Chemicals dosed for a given bioreactor reading (PAC / DAP)."""

    CHEMICAL_CHOICES = [
        ("PAC", "PAC"),
        ("DAP", "DAP"),
    ]

    reading = models.ForeignKey(
        BioreactorDailyReading,
        on_delete=models.CASCADE,
        related_name="chemicals",
    )
    chemical_name = models.CharField(
        max_length=20,verbose_name="Chemical Name"
    )
    quantity = models.DecimalField(
        max_digits=12, decimal_places=3,
        help_text="Quantity dosed (kg / L – as per your convention)",
        verbose_name="Quantity"
    )

    class Meta:
        db_table = "bioreactor_chemical"

    def __str__(self):
        return f"{self.chemical_name} - {self.quantity} ( {self.reading.date} )"