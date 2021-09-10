from django import template

register = template.Library()


@register.filter(name='format_aps_cycle_v2')
def format_aps_cycle_v2(value):
    try:
        cycle, user_str = value.split('/')
        return cycle
    except Exception:
        return 'No Cycle'
