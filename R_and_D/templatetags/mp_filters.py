# R_and_D/templatetags/mp_filters.py
from django import template
import re

register = template.Library()

_num_re = re.compile(r"^\s*(\d+(?:\.\d+)?)(?:\s*[-–—]\s*(\d+(?:\.\d+)?))?\s*$")

def _tidy(n: str) -> str:
    # remove trailing zeros in decimals: "101.50" -> "101.5", "101.0" -> "101"
    return n.rstrip("0").rstrip(".") if "." in n else n

@register.filter(name="mp_celsius")
def mp_celsius(value: str | None) -> str:
    """Render '101-102' / '101.5-102.2' / '101' as '101°C - 102°C' / '101.5°C - 102.2°C' / '101°C'."""
    if not value:
        return ""
    s = str(value).replace("–", "-").replace("—", "-").strip()
    m = _num_re.match(s)
    if not m:
        return value  # fall back to raw text if it doesn't match
    a, b = _tidy(m.group(1)), _tidy(m.group(2)) if m.group(2) else None
    return f"{a}°C - {b}°C" if b else f"{a}°C"
