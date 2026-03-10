from django import template
register = template.Library()

@register.filter
def dict_get(d, key):
    if d is None:
        return ""
    return d.get(key, "")


@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)