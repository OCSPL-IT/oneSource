# ETP/BIOREACTOR/templatetags/bioreactor_tags.py
from decimal import Decimal, InvalidOperation
from django import template

register = template.Library()


@register.filter
def br_num(value):
    """
    Format numbers like Excel requirement:
      - if value has no fractional part -> show as integer (e.g. 70)
      - else -> show with 2 decimal places (e.g. 70.50)

    None -> "-" (for your tables).
    """
    if value in (None, ""):
        return "-"

    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        # not numeric, just return as-is
        return value

    # round to 2 decimals for display
    d = d.quantize(Decimal("0.01"))

    # if no fractional part -> integer
    if d == d.to_integral_value():
        return str(d.to_integral_value())

    # otherwise exactly 2 decimals
    return f"{d:.2f}"
