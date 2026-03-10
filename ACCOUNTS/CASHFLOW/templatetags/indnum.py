from django import template

register = template.Library()


def _format_indian_number(value, decimals=2):
    """
    Format a number with Indian digit grouping:
    12345678.9 -> 1,23,45,678.90
    """
    if value is None or value == "":
        return ""

    try:
        num = float(value)
    except (TypeError, ValueError):
        return value  # fallback – just show original

    # Format with fixed decimals first
    s = f"{num:.{decimals}f}"

    if "." in s:
        int_part, dec_part = s.split(".")
    else:
        int_part, dec_part = s, ""

    sign = ""
    if int_part.startswith("-"):
        sign = "-"
        int_part = int_part[1:]

    # Indian grouping: last 3 digits, then groups of 2
    if len(int_part) > 3:
        last3 = int_part[-3:]
        rest = int_part[:-3]

        groups = []
        while len(rest) > 2:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.insert(0, rest)

        int_part = ",".join(groups + [last3])

    result = sign + int_part
    if decimals > 0:
        result += "." + dec_part

    return result


@register.filter(name="indian_comma")
def indian_comma(value, decimals=2):
    """
    Usage in template:
      {{ value|indian_comma }}      -> 2 decimals
      {{ value|indian_comma:0 }}    -> 0 decimals
    """
    try:
        decimals = int(decimals)
    except (TypeError, ValueError):
        decimals = 2
    return _format_indian_number(value, decimals)
