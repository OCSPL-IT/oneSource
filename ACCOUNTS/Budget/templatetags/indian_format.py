from django import template
from decimal import Decimal

register = template.Library()

@register.filter
def indian_comma(value):
    """
    Format a number in Indian number system with commas.
    e.g. 12345678.50 → 1,23,45,678.50
    """
    if value is None:
        return "0.00"
    try:
        value = Decimal(str(value))
        # Split integer and decimal parts
        str_val = f"{value:.2f}"
        integer_part, decimal_part = str_val.split(".")

        # Handle negative
        negative = integer_part.startswith("-")
        if negative:
            integer_part = integer_part[1:]

        # Indian grouping: last 3 digits, then groups of 2
        if len(integer_part) <= 3:
            formatted = integer_part
        else:
            # last 3
            last3 = integer_part[-3:]
            rest = integer_part[:-3]
            # group rest in 2s from right
            groups = []
            while len(rest) > 2:
                groups.append(rest[-2:])
                rest = rest[:-2]
            if rest:
                groups.append(rest)
            groups.reverse()
            formatted = ",".join(groups) + "," + last3

        result = formatted + "." + decimal_part
        return ("-" if negative else "") + result
    except Exception:
        return value