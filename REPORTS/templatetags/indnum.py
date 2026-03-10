# REPORTS/templatetags/indnum.py
from django import template
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

register = template.Library()

DEFAULT_PLACES = 0  # <<< no decimals everywhere by default

def _format_indian(nstr: str) -> str:
    """Indian digit grouping for the integer part of a decimal string."""
    if "." in nstr:
        int_part, frac = nstr.split(".", 1)
        dot = "." + frac
    else:
        int_part, frac, dot = nstr, "", ""

    neg = ""
    if int_part.startswith("-"):
        neg, int_part = "-", int_part[1:]

    if len(int_part) <= 3:
        grouped = int_part
    else:
        head, tail = int_part[:-3], int_part[-3:]
        pairs = []
        while head:
            pairs.append(head[-2:])
            head = head[:-2]
        grouped = ",".join(reversed(pairs)) + "," + tail

    return neg + grouped + dot

@register.filter(name="indnum")
def indnum(value, places=None):
    """
    Indian-number formatting with fixed decimals (default now 0).
    Usage:
      {{ v|indnum }}      -> 0 decimals
      {{ v|indnum:2 }}    -> 2 decimals (only if you explicitly ask)
    """
    if places is None or str(places).strip() == "":
        places = DEFAULT_PLACES
    try:
        places = max(0, int(places))
    except Exception:
        places = DEFAULT_PLACES

    if value in (None, ""):
        return "0" if places == 0 else ("0." + ("0" * places))

    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return value

    exp = Decimal(1).scaleb(-places)  # 10 ** -places
    d = d.quantize(exp, rounding=ROUND_HALF_UP)

    s = f"{d:f}"  # plain string, no scientific notation
    if places > 0:
        if "." not in s:
            s += "." + ("0" * places)
        else:
            frac_len = len(s.split(".", 1)[1])
            if frac_len < places:
                s += "0" * (places - frac_len)

    # When places == 0, s is already an integer string (no dot).
    return _format_indian(s)
