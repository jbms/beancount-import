from beancount.core.number import D
from beancount.core.amount import Amount
from .amount_parsing import parse_amount

def test_parsing():
    cases = {
        '$12345.67': Amount(D('12345.67'), 'USD'),
        '$12,345.67': Amount(D('12345.67'), 'USD'),
        '$12.34': Amount(D('12.34'), 'USD'),
        '+$123': Amount(D('123'), 'USD'),
        '$1.23': Amount(D('1.23'), 'USD'),
        '$0.12': Amount(D('0.12'), 'USD'),
        '$0': Amount(D('0'), 'USD'),
        '$0.00': Amount(D('0.00'), 'USD'),
        '$.12': Amount(D('0.12'), 'USD'),
        '-$.12': Amount(D('-0.12'), 'USD'),
        '-$123.45': Amount(D('-123.45'), 'USD'),

        '12345.67 CAD': Amount(D('12345.67'), 'CAD'),
        '12,345.67 CAD': Amount(D('12345.67'), 'CAD'),
        '12.34 CAD': Amount(D('12.34'), 'CAD'),
        '+123 CAD': Amount(D('123'), 'CAD'),
        '1.23 CAD': Amount(D('1.23'), 'CAD'),
        '0.12 CAD': Amount(D('0.12'), 'CAD'),
        '0 CAD': Amount(D('0'), 'CAD'),
        '0.00 CAD': Amount(D('0.00'), 'CAD'),
        '.12 CAD': Amount(D('0.12'), 'CAD'),
        '-.12 CAD': Amount(D('-0.12'), 'CAD'),
        '-123.45 CAD': Amount(D('-123.45'), 'CAD'),

        '€12345.67': Amount(D('12345.67'), 'EUR'),
        '€12,345.67': Amount(D('12345.67'), 'EUR'),
        '+€123': Amount(D('123'), 'EUR'),
        '€12.34': Amount(D('12.34'), 'EUR'),
        '€1.23': Amount(D('1.23'), 'EUR'),
        '€0.12': Amount(D('0.12'), 'EUR'),
        '€.12': Amount(D('0.12'), 'EUR'),
        '€0': Amount(D('0'), 'EUR'),
        '€0.00': Amount(D('0.00'), 'EUR'),
        '-€.12': Amount(D('-0.12'), 'EUR'),
        '-€123.45': Amount(D('-123.45'), 'EUR'),
        '£12345.67': Amount(D('12345.67'), 'GBP'),
        '£12,345.67': Amount(D('12345.67'), 'GBP'),
        '+£123': Amount(D('123'), 'GBP'),
        '£12.34': Amount(D('12.34'), 'GBP'),
        '£1.23': Amount(D('1.23'), 'GBP'),
        '£0.12': Amount(D('0.12'), 'GBP'),
        '£0': Amount(D('0'), 'GBP'),
        '£0.00': Amount(D('0.00'), 'GBP'),
        '£.12': Amount(D('0.12'), 'GBP'),
        '-£.12': Amount(D('-0.12'), 'GBP'),
        '-£123.45': Amount(D('-123.45'), 'GBP'),

        '$': None,
        '$.': None,
        '€0.': None,
        '£,': None,
        '$123,.00': None,
        '€,123.00': None,
        '£,47.1': None,
        '$€!@#$': None,
        ' INS': None,
        "I'm a parrot, cluck cluck": None,
        '1,234,567.89': None,
    }
    for input, expected in cases.items():
        try:
            actual = parse_amount(input)
        except ValueError:
            actual = None
        assert expected == actual, input
