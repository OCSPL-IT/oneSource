from django import template

register = template.Library()

@register.filter
def get_item(obj, key):
    """
    Template helper to access dict-like or form field by dynamic key.
    Usage:
      {{ form|get_item:key }}
    """
    try:
        return obj[key]
    except Exception:
        return None
