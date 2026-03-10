from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    return dictionary.get(key, 0)



def indian_currency_format(amount):
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return amount
    # Format as 12,34,56,789.00 (Indian format)
    s = f"{amount:,.2f}"
    parts = s.split('.')
    int_part = parts[0].replace(',', '')
    if len(int_part) > 3:
        int_part, last3 = int_part[:-3], int_part[-3:]
        int_part = list(int_part)
        # Insert commas after every two digits from right, except the first group
        for i in range(len(int_part)-2, 0, -2):
            int_part.insert(i, ',')
        int_part = ''.join(int_part)
        formatted = int_part + ',' + last3
    else:
        formatted = int_part
    return formatted + '.' + parts[1]

register.filter('indian', indian_currency_format)