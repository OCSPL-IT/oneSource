# ACCOUNTS/templatetags/inr_format.py
from decimal import Decimal, InvalidOperation
from django import template

register = template.Library()


def _format_indian_number(value, places=2):
    """
    Format a number in Indian style:
    147809106.22 -> '14,78,09,106.22'
    """
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return ""

    try:
        places = int(places)
    except (TypeError, ValueError):
        places = 2

    q = d.quantize(Decimal(10) ** -places)

    sign = "-" if q < 0 else ""
    s = f"{abs(q):f}"  # plain string, no scientific notation

    if "." in s:
        integer, frac = s.split(".")
    else:
        integer, frac = s, ""

    # pad/trim fraction
    if places > 0:
        frac = (frac + "0" * places)[:places]
    else:
        frac = ""

    # Indian grouping for integer part
    if len(integer) > 3:
        last3 = integer[-3:]
        rest = integer[:-3]
        groups = []
        while rest:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        integer = ",".join(groups + [last3])

    if places > 0:
        return f"{sign}{integer}.{frac}"
    return f"{sign}{integer}"


@register.filter
def inr(value, places=2):
    """
    Usage in templates:
      {{ value|inr }}      -> '14,78,09,106.22'
      {{ value|inr:0 }}    -> '14,78,09,106'
    """
    if value in (None, ""):
        return _format_indian_number("0", places)
    return _format_indian_number(value, places)
