from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """
    Given a dict and a key, return dictionary.get(key, '').
    """
    if isinstance(dictionary, dict):
        return dictionary.get(key, '')
    return ''
