import re
from beancount.core.number import D
from beancount.core.amount import Amount

def parse_negative_parentheses(x):
    """Parses a string in parentheses as a negative."""
    m = re.fullmatch(r'^\((.*)\)$', x)
    if m is not None:
        return -1, m.group(1).strip()
    return 1, x

def parse_possible_negative(x):
    x = x.strip()
    m = re.fullmatch(r'^(-|\+)(.*)$', x)
    if m is not None:
        sign = -1 if m.group(1) == '-' else 1
        return sign, m.group(2).strip()
    return parse_negative_parentheses(x)

def parse_number(x):
    """Parses a number in the format of the CSV file.

    A number in parentheses is interpreted as a negative number.
    """
    sign, number_str = parse_possible_negative(x)
    return sign * D(number_str)

def parse_amount(x):
    """Parses a number and currency."""
    if not x:
        return None
    sign, amount_str = parse_possible_negative(x)
    if amount_str.startswith('$'):
        currency = 'USD'
        number = D(amount_str[1:])
    else:
        raise ValueError('Unable to determine currency from %r' % amount_str)
    return Amount(number * sign, currency)

