# oneSource/QC/utils_fg.py
from datetime import datetime, time
from django.apps import apps
from django.db.models import F, Case, When, DateTimeField
from django.utils import timezone

def _QCEntry():
    # Pull QCEntry from the QC app without hard-coding a module path
    return apps.get_model("QC", "QCEntry")

def count_finished_goods_done(on_date):
    """
    Count FG inspections completed on the given date.
    """
    QCEntry = _QCEntry()

    tz = timezone.get_current_timezone()
    start = datetime.combine(on_date, time.min).replace(tzinfo=tz)
    end   = datetime.combine(on_date, time.max).replace(tzinfo=tz)

    done_ts = Case(
        When(release_by_qc_at__isnull=False, then=F("release_by_qc_at")),
        default=F("entry_date"),
        output_field=DateTimeField(),
    )

    return (
        QCEntry.objects
        .annotate(done_at=done_ts)
        .filter(
            ar_type__iexact="FG",
            status__in=["qc_completed", "QC Completed", "Released", "released"],
            done_at__range=(start, end),
        )
        .distinct()
        .count()
    )
