"""Parses a Google employee PDF pay statement from Ultipro."""
import os
from typing import NamedTuple, Dict, Any, List, Optional, Tuple, Union, Callable, Match
import datetime
import collections
import re
import subprocess
from beancount.core.number import D, ZERO, Decimal
from beancount.core.data import Amount

ParsedValues = List[Tuple[str, Dict[str, Any]]]

ParseResult = NamedTuple('ParseResult', [
    ('general', Dict[str, Any]),
    ('all_values', Dict[str, ParsedValues]),
    ('errors', List[str]),
])


def parse(text: str) -> ParseResult:
    """Parse a payroll statement.

    :param text: pdftotext -raw output from a payroll statement.

    :return: dict containing the keys 'general', 'Earnings', 'Deductions',
             'Taxes', 'Paid Time Off', 'Net Pay Distribution', 'Pay Summary'.
             The values for 'general' and 'Pay Summary' are dicts; the values
             for all other keys are lists of entries, as there can be
             duplicates.
    """
    date_re = r'[0-9]{2}/[0-9]{2}/[0-9]{4}'
    decimal_re = r'[0-9]+(?:,[0-9]+)*\.[0-9]+'
    yesno_re = r'(?:Yes|No)'
    currency_amount_re = r'(?:\$' + decimal_re + r'|\(\$' + decimal_re + r'\))'
    number_re = r'[0-9]+'
    account_re = r'[0-9x]+'

    # One or more space separated words.  Each word starts with
    # alphanumeric and remaining characters are alphanumeric + hyphen. A
    # single hyphen is also allowed as a word after the first word.
    field_name_re = r'[0-9a-zA-Z][0-9a-zA-Z\-]*(?:[ \n]+(?:[0-9a-zA-Z][0-9a-zA-Z\-]*|-))*'

    def parse_date(x: str) -> datetime.date:
        return datetime.datetime.strptime(x, '%m/%d/%Y').date()

    def parse_currency(x: Optional[str]) -> Optional[Decimal]:
        if x is None:
            return None
        x = x.replace('$', '')
        if x.startswith('('):
            return -D(x[1:-1])
        return D(x)

    def parse_yesno(x: Optional[str]) -> Optional[bool]:
        if x is None:
            return None
        return x == 'Yes'

    def parse_hours(x: Optional[str]) -> Optional[Decimal]:
        if x is None:
            return None
        return D(x)

    # The sections are expected to occur in the order specified here.  This is a
    # list of (section_header_regex, row_matchers) pairs.  First, the section
    # headers are matched in order.  Then, within the bounds determined by those
    # matches, the rows for each section are matched.
    #
    # The name of a section is specified by the first match group of the
    # section_header_regex.  row_matchers is a list of (row_regex, (field_name,
    # parser), ...) tuples.  The (field_name, parser) pairs specify a dictionary
    # key and a corresponding transformation function to apply to each match
    # group obtained from row_regex, except the first match group which is
    # assumed to specify the row name.
    section_parsers = [
        ('^(Pay Statement)$', [
            (r'^(Period Start Date|Period End Date|Pay Date) (' + date_re +
             r')$',
             ('date', parse_date)),
            (r'^(Document) ?([0-9A-Z]*)$',
             ('number', str)),
            (r'^(Net Pay) (' + currency_amount_re + r')$',
             ('Amount', parse_currency)),
        ]),
        ('^(Pay Details)$', [
            (r'^(Employee Number) (' + number_re + r')$',
             ('number', str)),
        ]),
        [
            (
                r'^(Earnings)\nPay Type Hours Pay Rate Current YTD$',
                [
                    (r'^(' + field_name_re + r')[ \n](?:(' + decimal_re +
                     r') (' + currency_amount_re + r') )?(' +
                     currency_amount_re + r')(?: (' + currency_amount_re +
                     r'))?$',
                     ('Hours', parse_hours),
                     ('Pay Rate', parse_currency),
                     ('Current', parse_currency),
                     ('YTD', parse_currency)),
                    #(r'^(Total Hours) (' + decimal_re + r')$', ('hours', D)),
                ]),
            (
                r'^(Earnings)\nPay Type Week Job Hours[ \n]Pay[ \n]Rate Current YTD$',
                [
                    (r'^(' + field_name_re + r')[ \n](?:[0-5] [a-zA-Z ]+)?(' +
                     decimal_re + r')' + 3 *
                     (r' (' + currency_amount_re + r')') + r'$',
                     ('Hours', D),
                     ('Pay Rate', parse_currency),
                     ('Current', parse_currency),
                     ('YTD', parse_currency)),
                    #(r'^(Total Hours) (' + decimal_re + r')$', ('hours', D)),
                    (r'^(' + field_name_re + r')[ \n](?:[0-5] [a-zA-Z ]+)?(' +
                     decimal_re + r')' + 2 *
                     (r' (' + currency_amount_re + r')') + r'$',
                     ('Hours', D),
                     ('Pay Rate', parse_currency),
                     ('Current', parse_currency)),
                ]),
            (
                r'^(Earnings)\nPay Type Hours\nPay\nRate\nPiece\nUnits\nPiece\nRate Current YTD$',
                [
                    (r'^(' + field_name_re + r')[ \n](' + decimal_re + r')' +
                     (r' (' + currency_amount_re + r')') + r' (' + decimal_re +
                     r')' + 3 * (r' (' + currency_amount_re + r')') + r'$',
                     ('Hours', D),
                     ('Pay Rate', parse_currency),
                     ('Piece Units', D),
                     ('Piece Rate', parse_currency),
                     ('Current', parse_currency),
                     ('YTD', parse_currency)),
                    #(r'^(Total Hours) (' + decimal_re + r')$', ('hours', D)),
                ]),
        ],
        [
            (r'^(Deductions)\nEmployee\nDeduction Current YTD$', [
                (r'^(' + field_name_re + r')' + 2 *
                 (r' (' + currency_amount_re + r')') + r'$',
                 ('Current', parse_currency),
                 ('YTD', parse_currency)),
            ]),
            (r'^(Deductions)\nEmployee Employer\nDeduction Current YTD Current YTD$',
             [
                 (r'^(' + field_name_re + r')' + 4 *
                  (r' (' + currency_amount_re + r')') + r'$',
                  ('Current', parse_currency),
                  ('YTD', parse_currency),
                  ('Current:Employer', parse_currency),
                  ('YTD:Employer', parse_currency)),
             ]),
            (r'^(Deductions)\nEmployee Employer\nDeduction\sBased\sOn\sPre-\s?Tax Current YTD Current YTD$',
             [
                 (r'^(' + field_name_re + r')' +
                  (r'\s(' + currency_amount_re + r')') +
                  (r' (' + yesno_re + r')') + 4 *
                  (r' (' + currency_amount_re + r')') + r'$',
                  ('Based On', parse_currency),
                  ('Pre-tax', parse_yesno),
                  ('Current', parse_currency),
                  ('YTD', parse_currency),
                  ('Current:Employer', parse_currency),
                  ('YTD:Employer', parse_currency)),
             ]),
            (r'^(Deductions)\nDeduction\sBased\sOn\sPre-\s?Tax\sEmployee\sCurrent\sEmployee\sYTD\sEmployer\sCurrent\sEmployer\sYTD$',
             [
                 # These patterns are the same as in the preceding section.
                 (r'^(' + field_name_re + r')' +
                  (r'\s(' + currency_amount_re + r')') +
                  (r' (' + yesno_re + r')') + 4 *
                  (r' (' + currency_amount_re + r')') + r'$',
                  ('Based On', parse_currency),
                  ('Pre-tax', parse_yesno),
                  ('Current', parse_currency),
                  ('YTD', parse_currency),
                  ('Current:Employer', parse_currency),
                  ('YTD:Employer', parse_currency)),
             ]),
        ],
        (r'^(Taxes)\nTax(?:es)? Based On Current YTD$', [
            (r'^(' + field_name_re + r')' + 3 *
             (r' (' + currency_amount_re + r')') + r'$',
             ('Based On', parse_currency),
             ('Current', parse_currency),
             ('YTD', parse_currency)),
        ]),
        [
            (r'^(Paid Time Off)\nPlan Current Balance$', [
                (r'^(' + field_name_re + r')' + 2 *
                 (r' (' + decimal_re + r')') + r'$',
                 ('Current', D),
                 ('Balance', D)),
            ]),
            (r'^(Paid Time Off)(?= Net Pay Distribution\n)', []),
        ],
        (
            r'(?:^| )(Net Pay Distribution)\nAccount Number Account Type Amount$',
            [
                (r'^(' + account_re + r') ([a-zA-Z]+) (' + currency_amount_re +
                 ')$',
                 ('Account Type', str),
                 ('Amount', parse_currency)),
                #(r'^(Total) (' + currency_amount_re + ')$', (('amount', parse_currency))),
            ]),
        (r'^(Pay Summary)\nGross FIT Taxable Wages Taxes Deductions Net Pay$',
         [
             (r'^(Current|YTD)' + 5 *
              (r' (' + currency_amount_re + r')') + r'$',
              ('Earnings', parse_currency),
              ('FIT Taxable Wages', parse_currency),
              ('Taxes', parse_currency),
              ('Deductions', parse_currency),
              ('Net Pay', parse_currency)),
         ]),
    ]

    section_matches = [] # type: List[Tuple[Match, Any]]
    for entry in section_parsers:
        if section_matches:
            start_i = section_matches[-1][0].end()
        else:
            start_i = 0

        if not isinstance(entry, list):
            entry = [entry]
        found_match = False
        for section_re, rows in entry:
            m = re.compile(section_re, flags=re.MULTILINE).search(
                text, start_i)
            if m is None:
                continue
            section_matches.append((m, rows))
            found_match = True
            break
        if not found_match:
            raise ValueError('Failed to match section\n\n%r\n\nin text\n\n%r' % (entry, text[start_i:]))

    all_values = collections.OrderedDict() # type: Dict[str, ParsedValues]

    for section_i, (section_match, rows) in enumerate(section_matches):
        section_name = section_match.group(1)
        if section_name in all_values:
            raise RuntimeError('Duplicate section name: %r' % section_name)
        values = [] # type: ParsedValues
        all_values[section_name] = values

        start_i = section_match.end()
        if section_i + 1 < len(section_matches):
            end_i = section_matches[section_i + 1][0].start()
        else:
            end_i = len(text)
        while True:
            first_match = None
            first_match_fields = None
            for row in rows:
                row_re = row[0]
                m = re.compile(row_re, flags=re.MULTILINE).search(
                    text, start_i, end_i)
                if m is None:
                    continue
                if first_match is None or m.start() < first_match.start():
                    first_match = m
                    first_match_fields = row[1:]
            if first_match is None:
                break
            row_name = re.sub('[ \n]+', ' ', first_match.group(1)).strip()
            assert first_match_fields is not None
            assert len(first_match_fields) == len(first_match.groups()) - 1, (
                row_re, len(first_match_fields), len(first_match.groups()))
            if row_name in values:
                raise RuntimeError('Duplicate field %r in section %r' %
                                   (row_name, section_name))
            values.append(
                (row_name,
                 collections.OrderedDict(
                     (field_name, field_parser(group_value))
                     for (field_name, field_parser
                          ), group_value in zip(first_match_fields,
                                                first_match.groups()[1:]))))
            start_i = first_match.end()
    general = dict(
        all_values.pop('Pay Statement') + all_values.pop('Pay Details'))
    pay_summary_section = 'Pay Summary'
    pay_summary = dict(all_values[pay_summary_section])

    # Perform some sanity checks
    expected_net_pay = sum(
        (x['Amount'] for key, x in all_values['Net Pay Distribution']), ZERO)
    listed_net_pay = general['Net Pay']['Amount']
    summary_net_pay = pay_summary['Current']['Net Pay']
    assert expected_net_pay == listed_net_pay, (expected_net_pay, listed_net_pay)
    assert summary_net_pay == listed_net_pay, (summary_net_pay, listed_net_pay)

    errors = []
    for period in [
            'Current',
            # Skip YTD because it may be incorrect if two pay statements occur on the same day.
            # 'YTD',
    ]:
        for value in ('Earnings', 'Taxes', 'Deductions'):
            expected_value = sum((x[period] for key, x in all_values[value]),
                                 ZERO)
            summary_value = pay_summary[period][value]
            if expected_value != summary_value:
                errors.append(
                    'The %r section specifies %s %s of %s, but computed total is %s.'
                    % (pay_summary_section, period, value, summary_value,
                       expected_value))
    return ParseResult(general=general, all_values=all_values, errors=errors)


def parse_filename(path: str):
    PDFTOTEXT_ENV='PDFTOTEXT_BINARY'
    pdftotext='pdftotext'
    replacement_pdftotext = os.getenv(PDFTOTEXT_ENV)
    if replacement_pdftotext is not None:
        pdftotext=replacement_pdftotext
    text = subprocess.check_output([pdftotext, '-raw', path, '-']).decode()
    return parse(text)


def to_json(obj):
    if hasattr(obj, '_asdict'):
        return to_json(obj._asdict())
    if isinstance(obj, (list, tuple)):
        return [to_json(x) for x in obj]
    if isinstance(obj, dict):
        return collections.OrderedDict((k, to_json(v)) for k, v in obj.items())
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')
    return obj


def main():
    import argparse
    import json

    ap = argparse.ArgumentParser()
    ap.add_argument('path')
    ap.add_argument('--json', default=False, action='store_true',
                    help='Output in JSON format.')

    args = ap.parse_args()
    result = parse_filename(args.path)
    if args.json:
        print(json.dumps(to_json(result), indent=4))
    else:
        print(result.all_values)
        for error in result.errors:
            print('Error: %s' % error)


if __name__ == '__main__':
    main()
