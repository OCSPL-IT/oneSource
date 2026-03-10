# ---- Indian comma formatter (whole numbers) -------------------------------
from django import template
register = template.Library()  # if this file already has register, reuse it

def _indian_commas(n: int) -> str:
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
    return ("-" if neg else "") + out

@register.filter(name="indnum")
def indnum(value):
    """
    Whole number with Indian digit grouping, e.g. 12345678 -> 1,23,45,678
    Non-numeric -> '0'
    """
    try:
        n = int(round(float(value)))
    except Exception:
        return "0"
    return _indian_commas(n)
