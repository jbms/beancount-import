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
    m = re.fullmatch(r'([\$€])?([0-9]+(?:,[0-9]+)*(?:\.[0-9]*)?)(?:\s+([A-Z]{3}))?', amount_str)
    if m is None:
        raise ValueError('Failed to parse amount from %r' % amount_str)
    if m.group(1):
        currency = {'$': 'USD', '€': 'EUR'}[m.group(1)]
    number = D(m.group(2))
    if m.group(3):
        currency = m.group(3)
    return Amount(number * sign, currency)
