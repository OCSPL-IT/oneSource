from django import template
register = template.Library()

@register.filter
def avg(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


@register.filter
def avg_kf_line(lines):
    vals = [l.kf_factor for l in lines if l and l.kf_factor is not None]
    return round(sum(vals) / len(vals), 4)  if vals else None


@register.filter
def index(sequence, pos):
    try:
        return sequence[int(pos)]
    except (IndexError, ValueError, TypeError):
        return None