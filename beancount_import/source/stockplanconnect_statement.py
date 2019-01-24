"""Parses a Stockplanconnect release/trade confirmation PDF statement."""

from typing import Union, Type
import collections
import datetime
import subprocess
import re

import dateutil.parser
from beancount.core.amount import div as amount_div
from beancount.core.amount import Amount
from beancount.core.number import D, ZERO, Decimal
from ..amount_parsing import parse_amount


def get_pdf_text(path):
    return subprocess.check_output(['pdftotext', '-l', '1', '-raw', path,
                                    '-']).decode()


def get_release_fields(path):
    lines = get_pdf_text(path).splitlines()
    fields = dict()
    for line in lines:
        m = re.match('^([^:]+): (.*)$', line)
        if m is not None:
            fields[m.group(1)] = m.group(2)
        m = re.match(
            '^(State Tax|Federal Tax|Soc\\. Sec\\. Ta|Medicare Tax|Local1 Tax) ([0-9]+\\.[0-9]+) % (.*)$',
            line)
        if m is not None:
            fields[m.group(1)] = m.group(3)
    return fields


def parse_price_and_date(x):
    m = re.match('^ *([^ ]+) */ *([^ ]+) *$', x)
    if m is None:
        raise ValueError('Failed to parse price and date: %r' % (x, ))
    return parse_amount(m.group(1)), dateutil.parser.parse(m.group(2)).date()


class Release(object):
    def __init__(self, path):
        fields = self.fields = get_release_fields(path)
        self.award_id = fields['Award ID']
        self.release_date = dateutil.parser.parse(fields['Release Date']).date()
        if 'Settlement Date' in fields:
            self.settlement_date = dateutil.parser.parse(
                fields['Settlement Date']).date()
        else:
            self.settlement_date = None

        self.symbol = fields['Trading Symbol']
        self.path = path
        self.amount_released = Amount(
            currency=self.symbol,
            number=D(
                fields.get('Quantity Released') or
                fields.get('Quantity Released / Sold')))
        if '*FMV @ Vest / FMV Date' in fields:
            self.vest_price, self.vest_date = parse_price_and_date(
                fields['*FMV @ Vest / FMV Date'])
        else:
            self.vest_price = parse_amount(fields['*FMV @ Vest'])
            self.vest_date = self.release_date

        self.fee_amount = parse_amount(
            fields.get('Sup Trn Fee') or fields.get('SuppTranFee'))
        self.total_tax_amount = parse_amount(fields['Total Tax Amount'])

        # positive
        self.total_release_cost = -parse_amount(fields['Total Release Cost'])
        self.released_market_value = Amount(
            round(self.vest_price.number * self.amount_released.number, 2),
            self.vest_price.currency)

        self.released_market_value_minus_taxes = Amount(
            currency=self.total_tax_amount.currency,
            number=self.released_market_value.number -
            self.total_tax_amount.number)

        self.transfer_amount = None
        self.transfer_description = None
        for excess_field in [
                *(key for key in fields
                  if key.lower().endswith('due to participant')),
                '**Excess Amount'
        ]:
            if excess_field in fields:
                self.transfer_amount = parse_amount(fields[excess_field])
                self.transfer_description = excess_field
                break

        net_quantity = fields.get('Net Quantity')
        if net_quantity is None:
            self.net_release_shares = None
        else:
            #self.share_price = parse_amount(fields['Share'])

            # Release value after costs
            self.net_release_shares = Amount(
                currency=self.symbol, number=D(net_quantity))

        net_proceeds = fields.get('Net Proceeds')
        if net_proceeds is None:
            self.net_proceeds = None
        else:
            self.net_proceeds = parse_amount(net_proceeds)
            self.sale_price, self.sale_date = parse_price_and_date(
                fields['**WA Sale Price for Quantity Sold/Sale Date'])
            self.total_proceeds = parse_amount(
                fields['Sale PricexQuantity Sold'])
            # The sale price listed does not have sufficient precision; calculate it from the total instead.
            self.sale_price = amount_div(self.total_proceeds,
                                         self.amount_released.number)

            capital_gains_number = round(
                (self.sale_price.number - self.vest_price.number) *
                self.amount_released.number, 2)
            self.capital_gains = Amount(capital_gains_number,
                                        self.sale_price.currency)


def match_or_fail(pattern, text):
    m = re.search(pattern, text, re.MULTILINE)
    if m is None:
        raise ValueError('Failed to match pattern: %r' % (pattern, ))
    return m


class TradeConfirmation(object):
    def __init__(self, path):
        self.path = path
        text = get_pdf_text(path)
        self.symbol = match_or_fail(' Symbol #: ([^ ]+)', text).group(1)
        m = match_or_fail(
            r'^You sold ([0-9]+\.[0-9]+) at a price of ([0-9]+\.[0-9]+) on Trade Date ([^ ]+)$',
            text)
        self.quantity = Amount(D(m.group(1)), self.symbol)
        self.trade_date = dateutil.parser.parse(m.group(3)).date()
        self.gross_amount = parse_amount(
            match_or_fail('^Gross Amount ([^ ]+) *$', text).group(1))
        self.share_price = Amount(D(m.group(2)), self.gross_amount.currency)
        self.fees = parse_amount(
            match_or_fail('^Transaction Costs ([^ ]+) *$', text).group(1))
        self.net_amount = parse_amount(
            match_or_fail('^Net Amount ([^ ]+) *$', text).group(1))
        self.settlement_date = dateutil.parser.parse(
            match_or_fail(r'^Settlement Date\(mm/dd/yy\) ([^ ]+) *$',
                          text).group(1)).date()
        self.reference_number = match_or_fail(' REF # ([^ ]+) *$',
                                              text).group(1)


def to_json(obj):
    if isinstance(obj, (Release, TradeConfirmation)):
        return to_json(vars(obj))
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

_release_filename_pattern = r'.*\.Restricted_Units\.Trade_Confirmations\.Release_Confirmation(?:\.[0-9]+)?\.pdf'
_trade_filename_pattern = r'.*\.Restricted_Units\.Trade_Confirmations\.Confirmation(?:\.[0-9]+)?\.pdf'


def get_document_type(path: str) -> Union[None, Type[Release], Type[TradeConfirmation]]:
    if re.fullmatch(_release_filename_pattern, path):
        return Release
    if re.fullmatch(_trade_filename_pattern, path):
        return TradeConfirmation
    return None


def parse(path: str) -> Union[None, Release, TradeConfirmation]:
    class_type = get_document_type(path)
    if class_type is None:
        return None
    return class_type(path)


def main():
    import argparse
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument('path')
    ap.add_argument('--release', action='store_true')
    args = ap.parse_args()
    if args.release:
        value = Release(args.path)
    else:
        value = TradeConfirmation(args.path)
    print(json.dumps(to_json(value), sort_keys=True, indent=4))


if __name__ == '__main__':
    main()
