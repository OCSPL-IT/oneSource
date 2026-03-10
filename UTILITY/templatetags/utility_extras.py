from django import template

register = template.Library()

@register.filter
def index(List, i):
    return List[i]




@register.filter
def get_item(dictionary, key):
    """
    A template filter to get an item from a dictionary using a variable key.
    Usage: {{ my_dictionary|get_item:variable_containing_key }}
    Returns the value, or None if the key doesn't exist.
    """
    # Use .get() which is the safe way to access a dictionary key
    return dictionary.get(key)