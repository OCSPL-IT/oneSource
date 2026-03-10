# REPORTS/templatetags/numfmt.py
from django import template

register = template.Library()

def _indian_commas(n: int) -> str:
    """Return string with Indian digit grouping (e.g., 12345678 -> '1,23,45,678')."""
    neg = n < 0
    s = str(abs(n))
    if len(s) <= 3:
        out = s
    else:
        last3 = s[-3:]
        rest = s[:-3]
        groups = []
        while rest:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        out = ",".join(groups + [last3])
    return "-" + out if neg else out

@register.filter(name="indnum")
def indnum(value):
    """
    Render as a whole number with Indian commas.
    • Rounds to nearest integer (0.5 -> 1).
    • Non-numeric/None -> '0'
    """
    try:
        n = int(round(float(value)))
    except Exception:
        return "0"
    return _indian_commas(n)
