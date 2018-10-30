"""Parses an Amazon.com regular or digital order details HTML file."""

from typing import NamedTuple, Optional, List, Union, Iterable, Dict, Sequence, cast
import collections
import re
import os
import functools
import datetime

import bs4
import dateutil.parser
import beancount.core.amount
from beancount.core.amount import Amount
from beancount.core.number import D, ZERO, Decimal

from ..amount_parsing import parse_amount, parse_number


Errors = List[str]
Adjustment = NamedTuple('Adjustment', [
    ('description', str),
    ('amount', Amount),
])
Item = NamedTuple('Item', [
    ('quantity', Decimal),
    ('description', str),
    ('sold_by', str),
    ('condition', Optional[str]),
    ('price', Amount),
])

DigitalItem = NamedTuple('DigitalItem', [
    ('description', str),
    ('url', Optional[str]),
    ('sold_by', Optional[str]),
    ('by', Optional[str]),
    ('price', Amount),
])

Shipment = NamedTuple('Shipment', [
    ('shipped_date', datetime.date),
    ('items', Sequence[Union[Item, DigitalItem]]),
    ('items_subtotal', Amount),
    ('pretax_adjustments', Sequence[Adjustment]),
    ('total_before_tax', Amount),
    ('posttax_adjustments', Sequence[Adjustment]),
    ('tax', Amount),
    ('total', Amount),
    ('errors', Errors),
])
CreditCardTransaction = NamedTuple('CreditCardTransaction', [
    ('date', datetime.date),
    ('card_description', str),
    ('card_ending_in', str),
    ('amount', Amount),
])
Order = NamedTuple('Order', [
    ('order_id', str),
    ('order_date', datetime.date),
    ('shipments', Sequence[Shipment]),
    ('credit_card_transactions', Sequence[CreditCardTransaction]),
    ('pretax_adjustments', Sequence[Adjustment]),
    ('tax', Amount),
    ('posttax_adjustments', Sequence[Adjustment]),
    ('errors', Errors),
])

pretax_adjustment_fields_pattern = 'Shipping & Handling:|Free Shipping:|Promotion Applied:|Your Coupon Savings:|[0-9]+% off savings:|Subscribe & Save:|[0-9]+ Audible Credit Applied:|.*[0-9]+% Off.*:|Courtesy Credit:|(.*) Discount:'
posttax_adjustment_fields_pattern = 'Gift Card Amount:|Rewards Points:'


def to_json(obj):
    if hasattr(obj, '_asdict'):
        return to_json(obj._asdict())
    if isinstance(obj, list):
        return [to_json(x) for x in obj]
    if isinstance(obj, dict):
        return collections.OrderedDict((k, to_json(v)) for k, v in obj.items())
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')
    return obj


def add_amount(a: Optional[Amount], b: Optional[Amount]) -> Optional[Amount]:
    if a is None:
        return b
    if b is None:
        return a
    return beancount.core.amount.add(a, b)


def reduce_amounts(amounts: Iterable[Amount]) -> Optional[Amount]:
    return functools.reduce(add_amount, amounts, None)


def get_field_in_table(table, pattern, allow_multiple=False,
                       return_label=False):
    def predicate(node):
        return node.name == 'td' and re.fullmatch(pattern, node.text.strip(),
                                                  re.I)

    tds = table.find_all(predicate)
    results = [(td.text.strip().strip(':'),
                td.find_next_sibling('td').text.strip()) for td in tds]
    if not return_label:
        results = [r[1] for r in results]
    if not allow_multiple:
        if not results:
            return None
        return results[0]
    return results


def get_adjustments_in_table(table, pattern):
    adjustments = []
    for label, amount_str in get_field_in_table(
            table, pattern, allow_multiple=True, return_label=True):
        adjustments.append(
            Adjustment(amount=parse_amount(amount_str), description=label))
    return adjustments


def reduce_adjustments(adjustments: List[Adjustment]) -> List[Adjustment]:
    all_adjustments = collections.OrderedDict()  # type: Dict[str, List[Amount]]
    for adjustment in adjustments:
        all_adjustments.setdefault(adjustment.description,
                                   []).append(adjustment.amount)
    return [
        Adjustment(k, reduce_amounts(v)) for k, v in all_adjustments.items()
    ]


def parse_shipments(soup) -> List[Shipment]:

    shipped_pattern = '^Shipped on ([^\\n]+)$'

    def is_shipment_header_table(node):
        if node.name != 'table':
            return False
        text = node.text.strip()
        m = re.match(shipped_pattern, text)
        return m is not None

    header_tables = soup.find_all(is_shipment_header_table)

    shipments = []  # type: List[Shipment]
    errors = []  # type: Errors

    for header_table in header_tables:
        text = header_table.text.strip()
        m = re.match(shipped_pattern, text)
        assert m is not None
        shipped_date = dateutil.parser.parse(m.group(1)).date()

        items = []

        shipment_table = header_table.find_parent('table')

        def is_items_ordered_header(node):
            if node.name != 'tr':
                return False
            tds = node('td')
            if len(tds) < 2:
                return False
            return (tds[0].text.strip() == 'Items Ordered' and
                    tds[1].text.strip() == 'Price')

        items_ordered_header = shipment_table.find(is_items_ordered_header)

        item_rows = items_ordered_header.find_next_siblings('tr')

        for item_row in item_rows:
            tds = item_row('td')
            description_node = tds[0]
            price_node = tds[1]
            price = price_node.text.strip()

            pattern_without_condition = r'^\s*(?P<quantity>[0-9]+)\s+of:(?P<description>.*)\n\s*Sold by: (?P<sold_by>[^\n]+)'
            pattern_with_condition = pattern_without_condition + r'\n.*\n\s*Condition: (?P<condition>[^\n]+)'

            m = re.match(pattern_with_condition, description_node.text,
                         re.UNICODE | re.DOTALL)
            if m is None:
                m = re.match(pattern_without_condition, description_node.text,
                             re.UNICODE | re.DOTALL)
            assert m is not None
            description = re.sub(r'\s+', ' ', m.group('description').strip())
            sold_by = re.sub(r'\s+', ' ', m.group('sold_by').strip())
            try:
                condition = re.sub(r'\s+', ' ', m.group('condition').strip())
            except IndexError:
                condition = None
            suffix = ' (seller profile)'
            if sold_by.endswith(suffix):
                sold_by = sold_by[:-len(suffix)]
            items.append(
                Item(
                    quantity=D(m.group('quantity')),
                    description=description,
                    sold_by=sold_by,
                    condition=condition,
                    price=parse_amount(price),
                ))

        items_subtotal = parse_amount(
            get_field_in_table(shipment_table, r'Item\(s\) Subtotal:'))
        expected_items_subtotal = reduce_amounts(
            beancount.core.amount.mul(x.price, D(x.quantity)) for x in items)
        if expected_items_subtotal != items_subtotal:
            errors.append(
                'expected items subtotal is %r, but parsed value is %r' %
                (expected_items_subtotal, items_subtotal))

        output_fields = dict()
        output_fields['pretax_adjustments'] = get_adjustments_in_table(
            shipment_table, pretax_adjustment_fields_pattern)
        output_fields['posttax_adjustments'] = get_adjustments_in_table(
            shipment_table, posttax_adjustment_fields_pattern)
        pretax_parts = [items_subtotal] + [
            a.amount for a in output_fields['pretax_adjustments']
        ]
        total_before_tax = parse_amount(
            get_field_in_table(shipment_table, 'Total before tax:'))
        expected_total_before_tax = reduce_amounts(pretax_parts)
        if expected_total_before_tax != total_before_tax:
            errors.append(
                'expected total before tax is %s, but parsed value is %s' %
                (expected_total_before_tax, total_before_tax))

        sales_tax = get_adjustments_in_table(shipment_table, 'Sales Tax:')

        posttax_parts = (
            [total_before_tax] + [a.amount for a in sales_tax] +
            [a.amount for a in output_fields['posttax_adjustments']])
        total = parse_amount(
            get_field_in_table(shipment_table, 'Total for This Shipment:'))
        expected_total = reduce_amounts(posttax_parts)
        if expected_total != total:
            errors.append('expected total is %s, but parsed value is %s' %
                          (expected_total, total))

        shipments.append(
            Shipment(
                shipped_date=shipped_date,
                items=items,
                items_subtotal=items_subtotal,
                total_before_tax=total_before_tax,
                tax=sales_tax,
                total=total,
                errors=errors,
                **output_fields))

    return shipments


def parse_credit_card_transactions_from_payments_table(
        payment_table,
        order_date: datetime.date) -> List[CreditCardTransaction]:
    payment_text = '\n'.join(payment_table.strings)
    m = re.search(r'\n\s*Grand Total:\s+(.*)\n', payment_text)
    assert m is not None
    grand_total = parse_amount(m.group(1).strip())

    m = re.search(
        r'\n\s*([^\s|][^|\n]*[^|\s])\s+\|\s+Last (?:4 )?digits:\s+([0-9]{4})\n',
        payment_text)
    if m is None:
        m = re.search(r'\n\s*(.+)\s+ending in\s+([0-9]{4})\n', payment_text)

    if m is not None:
        credit_card_transactions = [
            CreditCardTransaction(
                date=order_date,
                amount=grand_total,
                card_description=m.group(1).strip(),
                card_ending_in=m.group(2).strip(),
            )
        ]
    else:
        credit_card_transactions = []
    return credit_card_transactions


def parse_credit_card_transactions(soup) -> List[CreditCardTransaction]:
    def is_header_node(node):
        return node.name == 'td' and node.text.strip(
        ) == 'Credit Card transactions'

    header_node = soup.find(is_header_node)
    if header_node is None:
        return []
    sibling = header_node.find_next_sibling('td')
    rows = sibling.find_all('tr')
    transactions = []
    for row in rows:
        if not row.text.strip():
            continue
        tds = row('td')
        description = tds[0].text.strip()
        amount_text = tds[1].text.strip()
        m = re.match(r'^([^:]+) ending in ([0-9]+):\s+([^:]+):$', description,
                     re.UNICODE)
        assert m is not None
        transactions.append(
            CreditCardTransaction(
                date=dateutil.parser.parse(m.group(3)).date(),
                card_description=m.group(1),
                card_ending_in=m.group(2),
                amount=parse_amount(amount_text),
            ))
    return transactions


def parse_invoice(path: str) -> Order:
    if os.path.basename(path).startswith('D'):
        return parse_digital_order_invoice(path)
    return parse_regular_order_invoice(path)


def parse_regular_order_invoice(path: str) -> Order:
    errors = []
    with open(path, 'r') as f:
        soup = bs4.BeautifulSoup(f.read(), 'lxml')
    shipments = parse_shipments(soup)
    payment_table_header = soup.find(
        lambda node: node.name == 'table' and re.match('^Payment information$', node.text.strip()))

    payment_table = payment_table_header.find_parent('table')

    output_fields = dict()
    output_fields['pretax_adjustments'] = get_adjustments_in_table(
        payment_table, pretax_adjustment_fields_pattern)
    expected_amount = reduce_amounts(
        a.amount for shipment in shipments for a in shipment.pretax_adjustments)
    amount = reduce_amounts(
        a.amount for a in output_fields['pretax_adjustments'])
    if expected_amount != amount:
        errors.append(
            'expected total pretax adjustment to be %s, but parsed total is %s'
            % (expected_amount, amount))

    payments_total_adjustments = []
    shipments_total_adjustments = []

    def resolve_posttax_adjustments():
        payment_adjustments = collections.OrderedDict(
            reduce_adjustments(
                get_adjustments_in_table(payment_table,
                                         posttax_adjustment_fields_pattern)))
        all_shipments_adjustments = collections.OrderedDict(
            reduce_adjustments(
                sum((x.posttax_adjustments for x in shipments), [])))
        all_keys = collections.OrderedDict(payment_adjustments.items())
        all_keys.update(all_shipments_adjustments.items())

        all_adjustments = collections.OrderedDict()  # type: Dict[str, Amount]
        for key in all_keys:
            payment_amount = payment_adjustments.get(key)
            shipments_amount = all_shipments_adjustments.get(key)
            amount = payment_amount
            if payment_amount is None and shipments_amount is not None:
                # Amazon sometimes doesn't include adjustments in the Payments table
                amount = shipments_amount
                payments_total_adjustments.append(expected_amount)
            elif payment_amount is not None and shipments_amount is None:
                # Amazon sometimes doesn't include these adjustments in the Shipment table
                shipments_total_adjustments.append(amount)
            elif payment_amount != shipments_amount:
                errors.append(
                    'expected total %r to be %s, but parsed total is %s' %
                    (key, shipments_amount, payment_amount))
            all_adjustments[key] = amount
        return [Adjustment(k, v) for k, v in all_adjustments.items()]

    output_fields['posttax_adjustments'] = resolve_posttax_adjustments()

    tax = parse_amount(
        get_field_in_table(payment_table, 'Estimated tax to be collected:'))

    expected_tax = reduce_amounts(
        a.amount for shipment in shipments for a in shipment.tax)
    if expected_tax != tax:
        errors.append(
            'expected tax is %s, but parsed value is %s' % (expected_tax, tax))

    payments_total_adjustment = reduce_amounts(payments_total_adjustments)
    shipments_total_adjustment = reduce_amounts(shipments_total_adjustments)
    grand_total = parse_amount(
        get_field_in_table(payment_table, 'Grand Total:'))

    expected_total = add_amount(shipments_total_adjustment,
                                reduce_amounts(x.total for x in shipments))
    adjusted_grand_total = add_amount(payments_total_adjustment, grand_total)
    if expected_total != adjusted_grand_total:
        errors.append('expected grand total is %s, but parsed value is %s' %
                      (expected_total, adjusted_grand_total))
    order_placed_pattern = r'(?:Subscribe and Save )?Order Placed:\s+([^\s]+ \d+, \d{4})'

    def is_order_placed_node(node):
        m = re.fullmatch(order_placed_pattern, node.text.strip())
        return m is not None

    node = soup.find(is_order_placed_node)
    m = re.fullmatch(order_placed_pattern, node.text.strip())
    assert m is not None
    order_date = dateutil.parser.parse(m.group(1)).date()

    credit_card_transactions = parse_credit_card_transactions(soup)
    if not credit_card_transactions:
        credit_card_transactions = parse_credit_card_transactions_from_payments_table(
            payment_table, order_date)

    if credit_card_transactions:
        total_payments = reduce_amounts(
            x.amount for x in credit_card_transactions)
    else:
        total_payments = Amount(number=ZERO, currency=grand_total.currency)
    if total_payments != adjusted_grand_total:
        errors.append('total payment amount is %s, but grand total is %s' %
                      (total_payments, adjusted_grand_total))

    title = soup.find('title').text.strip()
    m = re.fullmatch(r'.*Order ([0-9\-]+)', title.strip())
    assert m is not None

    return Order(
        order_date=order_date,
        order_id=m.group(1),
        shipments=shipments,
        credit_card_transactions=credit_card_transactions,
        tax=tax,
        errors=sum((shipment.errors
                    for shipment in shipments), cast(Errors, [])) + errors,
        **output_fields)


def get_text_lines(parent_node):
    text_lines = ['']
    for node in parent_node.children:
        if isinstance(node, bs4.NavigableString):
            text_lines[-1] += str(node)
        elif node.name == 'br':
            text_lines.append('')
        else:
            text_lines[-1] += node.text
    return text_lines


def parse_digital_order_invoice(path: str) -> Order:
    errors = []
    with open(path, 'r') as f:
        soup = bs4.BeautifulSoup(f.read(), 'lxml')

    digital_order_pattern = 'Digital Order: (.*)'

    def is_digital_order_row(node):
        if node.name != 'tr':
            return False
        m = re.match(digital_order_pattern, node.text.strip())
        if m is None:
            return False
        try:
            dateutil.parser.parse(m.group(1))
            return True
        except:
            return False

    # Find Digital Order row
    digital_order_header = soup.find(is_digital_order_row)
    digital_order_table = digital_order_header.find_parent('table')
    m = re.match(digital_order_pattern, digital_order_header.text.strip())
    assert m is not None
    order_date = dateutil.parser.parse(m.group(1)).date()

    def is_items_ordered_header(node):
        if node.name != 'tr':
            return False
        tds = node('td')
        if len(tds) < 2:
            return False
        return (tds[0].text.strip() == 'Items Ordered' and
                tds[1].text.strip() == 'Price')

    items_ordered_header = digital_order_table.find(is_items_ordered_header)

    item_rows = items_ordered_header.find_next_siblings('tr')
    items = []

    other_fields_td = None

    for item_row in item_rows:
        tds = item_row('td')
        if len(tds) != 2:
            other_fields_td = tds[0]
            continue
        description_node = tds[0]
        price_node = tds[1]
        price = price_node.text.strip()

        a = description_node.find('a')
        if a is not None:
            description = a.text.strip()
            url = a['href']
        else:
            bold_node = description_node.find('b')
            description = bold_node.text.strip()
            url = None

        text_lines = get_text_lines(description_node)

        def get_label_value(label):
            for line in text_lines:
                m = re.match(r'^\s*' + label + ': (.*)$', line,
                             re.UNICODE | re.DOTALL)
                if m is None:
                    continue
                return m.group(1)

        by = get_label_value('By')
        sold_by = get_label_value(r'Sold\s+By')

        items.append(
            DigitalItem(
                description=description,
                by=by,
                sold_by=sold_by,
                url=url,
                price=parse_amount(price),
            ))

    other_fields_text_lines = get_text_lines(other_fields_td)

    def get_other_field(pattern, allow_multiple=False, return_label=False):
        results = []
        for line in other_fields_text_lines:
            r = r'^\s*(' + pattern + r')\s+(.*[^\s])\s*$'
            m = re.match(r, line, re.UNICODE)
            if m is not None:
                results.append((m.group(1).strip(':'), m.group(2)))
        if not return_label:
            results = [r[1] for r in results]
        if not allow_multiple:
            if not results:
                return None
            return results[0]
        return results

    def get_adjustments(pattern):
        adjustments = []
        for label, amount_str in get_other_field(
                pattern, allow_multiple=True, return_label=True):
            adjustments.append(
                Adjustment(amount=parse_amount(amount_str), description=label))
        return adjustments

    def get_amounts_in_text(pattern_map):
        amounts = dict()
        for key, label in pattern_map.items():
            amount = parse_amount(get_other_field(label))
            amounts[key] = amount
        return amounts

    items_subtotal = parse_amount(get_other_field(r'Item\(s\) Subtotal:'))
    total_before_tax = parse_amount(get_other_field('Total Before Tax:'))
    tax = get_adjustments('Tax Collected:')
    total_for_this_order = parse_amount(
        get_other_field('Total for this Order:'))
    output_fields = dict()
    output_fields['pretax_adjustments'] = get_adjustments(
        pretax_adjustment_fields_pattern)
    pretax_parts = ([items_subtotal] +
                    [a.amount for a in output_fields['pretax_adjustments']])
    expected_total_before_tax = reduce_amounts(pretax_parts)
    if expected_total_before_tax != total_before_tax:
        errors.append('expected total before tax is %s, but parsed value is %s'
                      % (expected_total_before_tax, total_before_tax))
    output_fields['posttax_adjustments'] = get_adjustments(
        posttax_adjustment_fields_pattern)
    posttax_parts = ([total_before_tax] + [a.amount for a in tax] +
                     [a.amount for a in output_fields['posttax_adjustments']])
    expected_total = reduce_amounts(posttax_parts)
    if expected_total != total_for_this_order:
        errors.append('expected total is %s, but parsed value is %s' %
                      (expected_total, total_for_this_order))

    shipment = Shipment(
        shipped_date=order_date,
        items=items,
        items_subtotal=items_subtotal,
        total_before_tax=total_before_tax,
        tax=tax,
        total=total_for_this_order,
        errors=errors,
        **output_fields)

    order_id_pattern = '^Amazon.com\\s+order number:\\s+(D[0-9-]+)$'

    order_id_td = soup.find(lambda node: node.name == 'td' and re.match(order_id_pattern, node.text.strip()))
    m = re.match(order_id_pattern, order_id_td.text.strip())
    assert m is not None
    order_id = m.group(1)

    payment_table = soup.find(
        lambda node: node.name == 'table' and node.text.strip().startswith('Payment Information')
    )
    credit_card_transactions = parse_credit_card_transactions_from_payments_table(
        payment_table, order_date)

    return Order(
        order_date=order_date,
        order_id=order_id,
        shipments=[shipment],
        credit_card_transactions=credit_card_transactions,
        pretax_adjustments=[],
        posttax_adjustments=output_fields['posttax_adjustments'],
        tax=[],
        errors=[])


def main():
    import argparse
    import sys
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument('-q', '--quiet', default=False, action='store_true')
    ap.add_argument(
        '--json',
        default=False,
        action='store_true',
        help='Output in JSON format.')
    ap.add_argument('paths', nargs='*')

    args = ap.parse_args()
    results = []
    for path in args.paths:
        try:
            result = parse_invoice(path)
            results.append(result)
        except:
            sys.stderr.write('Error reading: %s\n' % path)
            if not args.quiet:
                raise
        if not args.quiet and not args.json:
            print(repr(result))
    if args.json:
        print(json.dumps(to_json(results), indent=4))


if __name__ == '__main__':
    main()
