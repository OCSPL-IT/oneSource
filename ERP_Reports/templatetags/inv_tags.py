# yourapp/templatetags/inv_tags.py
from django import template

register = template.Library()

@register.filter
def get(d, key):
    """Safely get dict value by dynamic key inside templates."""
    if isinstance(d, dict):
        return d.get(key, "")
    return ""
