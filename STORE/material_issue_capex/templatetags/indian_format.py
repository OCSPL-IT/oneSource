from decimal import Decimal, InvalidOperation
from django import template

register = template.Library()


def _format_indian_group(int_part: str) -> str:
    """
    Format the integer part using Indian numbering system.
    Example: '1504879' -> '15,04,879'
    """
    int_part = int_part.lstrip()  # just in case
    if len(int_part) <= 3:
        return int_part

    last3 = int_part[-3:]
    rest = int_part[:-3]

    groups = []
    while len(rest) > 2:
        groups.insert(0, rest[-2:])
        rest = rest[:-2]

    if rest:
        groups.insert(0, rest)

    return ",".join(groups + [last3])


@register.filter(name="indian_number")
def indian_number(value, decimal_places=None):
    """
    Formats a number in Indian style with commas.
    Usage in template:
        {{ value|indian_number }}       -> no forced decimal places
        {{ value|indian_number:3 }}    -> always 3 decimals
    """
    if value in ("", None):
        return ""

    # Convert to Decimal safely
    try:
        num = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return value  # fall back to original

    # Handle decimal places
    if decimal_places is not None:
        try:
            places = int(decimal_places)
        except (ValueError, TypeError):
            places = None
    else:
        places = None

    if places is not None and places >= 0:
        fmt = "1." + ("0" * places)
        num = num.quantize(Decimal(fmt))
        s = f"{num:f}"  # no scientific notation
    else:
        s = f"{num:f}"

    if "." in s:
        int_part, frac_part = s.split(".", 1)
    else:
        int_part, frac_part = s, ""

    int_part_formatted = _format_indian_group(int_part)

    if places is not None and places > 0:
        # ensure fixed decimals
        frac_part = frac_part.ljust(places, "0")[:places]
        return f"{int_part_formatted}.{frac_part}"
    elif frac_part:
        return f"{int_part_formatted}.{frac_part}"
    else:
        return int_part_formatted
