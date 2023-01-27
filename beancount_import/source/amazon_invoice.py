"""Parses an Amazon.com/.de regular or digital order details HTML file.

Hierarchy of functions for parsing Amazon invoices:

main(...)
    |
    + parse_invoice(...)
    |    |
    |    + parse_digital_order_invoice(...)
    |    |   |
    |    |   + parse_credit_card_transactions_from_payments_table(...)
    |    |   +-> returns Order(..., shipments, ...)
    |    |
    |    + parse_regular_order_invoice(...)
    |        |
    |        + parse_shipments(...)
    |        |   + parse_shipment_payments(...)
    |        |   |   +-> returns Shipment
    |        |   +-> returns List[Shipment]
    |        |
    |        + parse_gift_cards(...)
    |        |   + parse_shipment_payments(...)
    |        |   |   +-> returns Shipment
    |        |   +-> returns List[Shipment]
    |        |
    |        + parse_credit_card_transactions(...)
    |        + parse_credit_card_transactions_from_payments_table(...)
    |        +-> returns Order(..., shipments, ...)
    |
    +-> returns Order
"""
from typing import NamedTuple, Optional, List, Union, Iterable, Dict, Sequence, cast
from abc import ABC, abstractmethod
import collections
import re
import os
import functools
import datetime
import logging

import bs4
import dateutil.parser
import beancount.core.amount
from beancount.core.amount import Amount
from beancount.core.number import D, ZERO, Decimal

from ..amount_parsing import parse_amount, parse_number

logger = logging.getLogger('amazon_invoice')


class Locale_Data(ABC):
    LOCALE: str
    tax_included_in_price: bool
    payee: str
    currency: str  # only used for assumed prices

    # common fields regular and digital orders
    items_ordered: str
    price: str
    items_subtotal: str
    total_before_tax: str
    pretax_adjustment_fields_pattern: str
    posttax_adjustment_fields_pattern: str

    # Payment Table & Credit Card Transactions
    grand_total: str
    credit_card_transactions: str
    credit_card_last_digits: str
    payment_type: List[str]
    payment_information: str

    # regular orders only
    shipment_shipped_pattern: str
    shipment_nonshipped_headers: List[str]
    shipment_quantity: str
    shipment_of: str
    shipment_sales_tax: str
    shipment_total: str
    shipment_seller_profile: str
    shipment_sold_by: str
    shipment_condition: str
    regular_total_order: str
    regular_estimated_tax: str
    regular_order_placed: str
    regular_order_id: str
    gift_card: Optional[str]
    gift_card_to: Optional[str]
    gift_card_amazon_account: Optional[str]

    # digital orders only
    digital_order: str
    digital_order_cancelled: str
    digital_by: str
    digital_sold_by: str
    digital_tax_collected: str
    digital_total_order: str
    digital_order_id: str
    digital_payment_information: str

    @staticmethod
    @abstractmethod
    def parse_amount(amount, assumed_currency=None) -> Amount:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def parse_date(date_str) -> datetime.date:
        raise NotImplementedError


class Locale_en_US(Locale_Data):
    """Language and region specific settings for parsing amazon.com invoices
    """
    LOCALE='en_US'
    tax_included_in_price=False
    payee='Amazon.com'
    currency='USD'  # only used for assumed prices
    
    # common fields regular and digital orders
    items_ordered='Items Ordered'
    price='Price'
    items_subtotal=r'Item\(s\) Subtotal:'
    total_before_tax='Total Before Tax:'
    pretax_adjustment_fields_pattern=('(?:' + '|'.join([
        'Shipping & Handling',
        'Free Shipping',
        'Free delivery',
        'Pantry delivery',
        'Promotion(?:s| Applied)',
        'Lightning Deal',
        'Your Coupon Savings', 
        '[0-9]+% off savings',
        'Subscribe & Save',
        '[0-9]+ Audible Credit Applied',
        '.*[0-9]+% Off.*',
        'Courtesy Credit',
        'Extra Savings',
        '(?:.*) Discount',
        'Gift[ -]Wrap',
    ]) + ') *:')
    posttax_adjustment_fields_pattern=r'Gift Card Amount:|Rewards Points:|Tip [(]optional[)]:|Recycle Fee \$X'

    # Payment Table & Credit Card Transactions
    grand_total=r'\n\s*Grand Total:\s+(.*)\n'
    credit_card_transactions='Credit Card transactions'
    credit_card_last_digits=r'^([^:]+) ending in ([0-9]+):\s+([^:]+):$'
    payment_type=[
        # only first matching regex is used!
        r'\n\s*([^\s|][^|\n]*[^|\s])\s+\|\s+Last (?:4 )?digits:\s+([0-9]{4})\n',
        r'\n\s*(.+)\s+ending in\s+([0-9]{4})\n'
        ]
    payment_information='^Payment information$'

    # regular orders only
    shipment_shipped_pattern='^Shipped on ([^\\n]+)$'
    shipment_nonshipped_headers=[
        'Service completed',
        'Preparing for Shipment',
        'Not Yet Shipped',
        'Shipping now'
        # unknown shipment statuses will be ignored
        # transaction total will not match
        ]
    shipment_quantity=r'^\s*(?:(?P<quantity>[0-9]+)|(?P<weight1>[0-9.]+\s+(?:lb|kg))|(?:(?P<quantityIgnore>[0-9.]+) [(](?P<weight2>[^)]+)[)]))\s+of:'
    shipment_of='of:'
    shipment_sales_tax='Sales Tax:'
    shipment_total='Total for This Shipment:'
    shipment_seller_profile=' (seller profile)'
    shipment_sold_by=r'(?P<description>.*)\n\s*(?:Sold|Provided) by:? (?P<sold_by>[^\n]+)'
    shipment_condition=r'\n.*\n\s*Condition: (?P<condition>[^\n]+)'
    regular_total_order='Grand Total:'
    regular_estimated_tax = 'Estimated tax to be collected:'
    regular_order_placed=r'(?:Subscribe and Save )?Order Placed:\s+([^\s]+ \d+, \d{4})'
    regular_order_id=r'.*Order ([0-9\-]+)'

    # digital orders only
    digital_order='Digital Order: (.*)'
    digital_order_cancelled='Order Canceled'
    digital_by='By'
    digital_sold_by=r'Sold\s+By'
    digital_tax_collected='Tax Collected:'
    digital_total_order='Total for this Order:'
    digital_order_id='^Amazon.com\\s+order number:\\s+(D[0-9-]+)$'
    digital_payment_information='Payment Information'

    @staticmethod
    def parse_amount(amount, assumed_currency=None) -> Amount:
        return parse_amount(amount, assumed_currency=assumed_currency)

    @staticmethod
    def parse_date(date_str) -> datetime.date:
        return dateutil.parser.parse(date_str).date()


class Locale_de_DE(Locale_Data):
    """Language and region specific settings for parsing amazon.de invoices
    """
    LOCALE='de_DE'
    tax_included_in_price=True  # no separate tax transactions
    payee='Amazon.de'
    currency='EUR'  # only used for assumed prices

    # common fields regular and digital orders
    items_ordered='Bestellte Artikel|Erhalten|Versendet|Amazon-Konto erfolgreich aufgeladen' # Erhalten|Versendet for gift cards
    price='Preis|Betrag'
    items_subtotal='Zwischensumme:'
    total_before_tax='Summe ohne MwSt.:'
    # most of translations still missing ...
    pretax_adjustment_fields_pattern=('(?:' + '|'.join([
        'Verpackung & Versand',
        # 'Free Shipping', 'Free delivery', 'Pantry delivery',
        # 'Promotion(?:s| Applied)', 'Lightning Deal',
        # 'Your Coupon Savings', '[0-9]+% off savings',
        # 'Subscribe & Save', '[0-9]+ Audible Credit Applied',
        # '.*[0-9]+% Off.*', 'Courtesy Credit',
        # 'Extra Savings', '(?:.*) Discount', 'Gift[ -]Wrap',
    ]) + ') *:')
    # most adjustments in DE are posttax:
    posttax_adjustment_fields_pattern='Gutschein eingelöst:|Geschenkgutschein\(e\):'
    
    # Payment Table & Credit Card Transactions
    grand_total=r'\n\s*(?:Gesamtsumme|Endsumme):\s+(.*)\n' # regular: Gesamtsumme, digital: Endsumme
    credit_card_transactions='Kreditkarten-Transaktionen'
    credit_card_last_digits=r'^([^:]+) mit den Endziffern ([0-9]+):\s+([^:]+):$'
    payment_type=[
        # only first matching regex is used!
        r'\n\s*([^\s|][^|\n]*[^|\s])\s+\|\s+Die letzten (?:4 )?Ziffern:\s*([0-9]{3,4})', # 3 digits for Bankeinzug
        r'\n\s*(.+)\s+mit den Endziffern\s+([0-9]{4})\n'
        ]
    payment_information='^Zahlungsdaten$'

    # regular orders only
    shipment_shipped_pattern='^versandt am ([^\\n]+)$'
    shipment_nonshipped_headers=[
        'Versand wird vorbereitet',
        'Versand in Kürze',
        # additional cases missing?
        # unknown shipment statuses will be ignored
        # transaction total will not match
    ]
    shipment_quantity=r'^\s*(?:(?P<quantity>[0-9]+)|(?P<weight1>[0-9.]+\s+(?:lb|kg))|(?:(?P<quantityIgnore>[0-9.]+) [(](?P<weight2>[^)]+)[)]))\s+Exemplar\(e\)\svon:'
    shipment_of='Exemplar(e) von:'
    shipment_sales_tax='Anzurechnende MwSt.:' # not sure (only old invoices)
    shipment_total='Gesamtsumme:'
    shipment_seller_profile=' (Mitgliedsprofil)'
    shipment_sold_by=r'(?P<description>.*)\n\s*(?:Verkauf) durch:? (?P<sold_by>[^\n]+)'
    shipment_condition=r'\n.*\n\s*Zustand: (?P<condition>[^\n]+)'
    regular_total_order='Gesamtsumme:'
    regular_estimated_tax='Anzurechnende MwSt.:'
    regular_order_placed=r'(?:Getätigte Spar-Abo-Bestellung|Bestellung aufgegeben am):\s+(\d+\. [^\s]+ \d{4})'
    regular_order_id=r'.*Bestellung ([0-9\-]+)'
    gift_card='Geschenkgutscheine'
    gift_card_to=r'^(?P<type>Geschenkgutschein)[\w\s-]*:\s*(?P<sent_to>[\w@._-]*)$'
    gift_card_amazon_account=r'^[\w\s-]*(?P<type>Amazon-Konto)[\w\s-]*(?P<sent_to>aufgeladen)[\w\s-]*$'

    # digital orders only
    digital_order_cancelled='Order Canceled'
    digital_order='Digitale Bestellung: (.*)'
    digital_by='Von'
    digital_sold_by=r'Verkauft von'
    digital_tax_collected='MwSt:'
    digital_total_order='Endsumme:'
    digital_order_id='^Amazon.de\\s+Bestellnummer:\\s+(D[0-9-]+)$'
    digital_payment_information='Zahlungsinformation'

    @staticmethod
    def _format_number_str(value: str) -> str:
        # 12.345,67 EUR -> 12345.67 EUR
        thousands_sep = '.'
        decimal_sep = ','
        return value.replace(thousands_sep, '').replace(decimal_sep, '.')

    @staticmethod
    def parse_amount(amount: str, assumed_currency=None) -> Amount:
        if amount is None:
            return None
        else:
            return parse_amount(
                Locale_de_DE._format_number_str(amount),
                assumed_currency=assumed_currency)

    class _parserinfo(dateutil.parser.parserinfo):
        MONTHS=[
            ('Jan', 'Januar'), ('Feb', 'Februar'), ('Mär', 'März'),
            ('Apr', 'April'), ('Mai', 'Mai'), ('Jun', 'Juni'),
            ('Jul', 'Juli'), ('Aug', 'August'), ('Sep', 'September'),
            ('Okt', 'Oktober'), ('Nov', 'November'), ('Dez', 'Dezember')
            ]
    
    @staticmethod
    def parse_date(date_str) -> datetime.date:
        return dateutil.parser.parse(date_str, parserinfo=Locale_de_DE._parserinfo(dayfirst=True)).date()


LOCALES = {x.LOCALE: x for x in [Locale_en_US, Locale_de_DE]}

Errors = List[str]
Adjustment = NamedTuple('Adjustment', [
    ('description', str),
    ('amount', Amount),
])
Item = NamedTuple('Item', [
    ('quantity', Decimal),
    ('description', str),
    ('sold_by', Optional[str]),
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
    ('shipped_date', Optional[datetime.date]),
    ('items', List[Union[Item, DigitalItem]]),
    ('items_subtotal', Amount),
    ('pretax_adjustments', List[Adjustment]),
    ('total_before_tax', Amount),
    ('posttax_adjustments', List[Adjustment]),
    ('tax', List[Adjustment]),
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
    ('shipments', List[Shipment]),
    ('credit_card_transactions', List[CreditCardTransaction]),
    ('pretax_adjustments', List[Adjustment]),
    ('tax', Amount),
    ('posttax_adjustments', List[Adjustment]),
    ('errors', Errors),
])

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
    """Add two amounts, amounts with value `None` are ignored.
    """
    if a is None:
        return b
    if b is None:
        return a
    return beancount.core.amount.add(a, b)


def reduce_amounts(amounts: Iterable[Amount]) -> Optional[Amount]:
    """Reduce iterable of amounts to sum by applying `add_amount`.
    """
    return functools.reduce(add_amount, amounts, None)


def get_field_in_table(table, pattern, allow_multiple=False,
                       return_label=False):
    def predicate(node):
        return node.name == 'td' and re.fullmatch(pattern, node.text.strip(),
                                                  re.I) is not None

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


def get_adjustments_in_table(
    table, pattern, assumed_currency=None, locale=Locale_en_US) -> List[Adjustment]:
    """ Parse price adjustments in shipping or payment tables. Returns list of adjustments.
    """
    adjustments = []
    for label, amount_str in get_field_in_table(
            table, pattern, allow_multiple=True, return_label=True):
        adjustments.append(
            Adjustment(amount=locale.parse_amount(amount_str, assumed_currency), 
                    description=label))
    return adjustments


def reduce_adjustments(adjustments: Sequence[Adjustment]) -> List[Adjustment]:
    """ Takes list of adjustments and reduces duplicates by summing up the amounts.
    """
    # create dict like {adjustment: [amount1, amount2, ...]}
    all_adjustments = collections.OrderedDict()  # type: Dict[str, List[Amount]]
    for adjustment in adjustments:
        all_adjustments.setdefault(adjustment.description,
                                   []).append(adjustment.amount)
    # sum over amounts and convert back to list of Adjustment
    return [
        Adjustment(k, reduce_amounts(v)) for k, v in all_adjustments.items()
    ]


def is_items_ordered_header(node, locale=Locale_en_US) -> bool:
    """
    Identify Header of Items Ordered table (within shipment table)
    """
    if node.name != 'tr':
        return False
    tds = node('td')
    if len(tds) < 2:
        return False
    m1 = re.match(locale.items_ordered, tds[0].text.strip())
    m2 = re.match(locale.price, tds[1].text.strip())
    return(m1 is not None and m2 is not None)


def parse_shipments(soup, locale=Locale_en_US) -> List[Shipment]:
    """
    Parses Shipment Table Part of HTML document (1st Table)
    """
    def is_shipment_header_table(node):
        if node.name != 'table':
            return False
        text = node.text.strip()
        m = re.match(locale.shipment_shipped_pattern, text)
        # return True for both shipped and nonshipped table headers
        return m is not None or text in locale.shipment_nonshipped_headers

    header_tables = soup.find_all(is_shipment_header_table)

    if header_tables is []:
        # no shipment tables
        # e.g. if only gift cards in order
        logger.debug('no shipment table found')
        return []

    shipments = []  # type: List[Shipment]
    errors = []  # type: Errors

    for header_table in header_tables:
        logger.debug('extracting shipped date...')
        text = header_table.text.strip()
        shipped_date = None
        if text not in locale.shipment_nonshipped_headers:
            # extract shipped date if order already shipped
            m = re.match(locale.shipment_shipped_pattern, text)
            assert m is not None
            shipped_date = locale.parse_date(m.group(1))

        logger.debug('parsing shipment items...')
        items = []  # type: List[Item]

        shipment_table = header_table.find_parent('table')
        items_ordered_header = shipment_table.find(
            lambda node: is_items_ordered_header(node, locale))
        item_rows = items_ordered_header.find_next_siblings('tr')

        for item_row in item_rows:
            tds = item_row('td')
            description_node = tds[0]
            price_node = tds[1]
            price = price_node.text.strip()

            if price is None:
                price = Amount(D(0), locale.currency)
            else:
                price = locale.parse_amount(price)

            # 1 of: 365 Everyday Value, Potato Yellow Bag Organic, 48 Ounce
            # 2 (1.04 lb) of: Broccoli Crowns Conventional, 1 Each
            # 2.07 lb of: Pork Sausage Link Italian Mild Step 1

            m = re.match(locale.shipment_quantity, description_node.text, re.UNICODE|re.DOTALL)
            
            quantity = None
            if m is not None:
                # Amazon will say you got, e.g. 2 broccoli crowns at $1.69/lb - but then this code multiplies the 2 by the price listed
                # on the invoice, which is the total price in this case (but the per-unit price in other cases) - so if there's a quantity
                # and a weight, ignore the quantity and treat it as 1
                # alternately, capture the weight and the per-unit price and multiply out

                # 'quantity' group: integer, no weight units, no decimals
                quantity = m.group("quantity")
                # set silently to 1 if other regex groups match
                if quantity is None:
                    quantity = 1
            else:
                # regex did not match at all -> log warning
                quantity = 1
                errors.append("Unable to extract quantity, using 1: %s" % description_node.text)

            quantity = D(quantity)

            text = description_node.text.split(locale.shipment_of, 1)[1]

            m = re.match(locale.shipment_sold_by + locale.shipment_condition,
                         text, re.UNICODE | re.DOTALL)
            if m is None:
                m = re.match(locale.shipment_sold_by, text, re.UNICODE | re.DOTALL)
            if m is None:
                errors.append("Could not extract item from row {}".format(text))
                raise Exception("Could not extract item from row", text)
            
            description = re.sub(r'\s+', ' ', m.group('description').strip())
            sold_by = re.sub(r'\s+', ' ', m.group('sold_by').strip())
            try:
                condition = re.sub(r'\s+', ' ', m.group('condition').strip())
            except IndexError:
                condition = None
            suffix = locale.shipment_seller_profile
            if sold_by.endswith(suffix):
                sold_by = sold_by[:-len(suffix)]
            items.append(
                Item(
                    quantity=quantity,
                    description=description,
                    sold_by=sold_by,
                    condition=condition,
                    price=price,
                ))
        
        shipments.append(parse_shipment_payments(
            shipment_table,
            items,
            errors,
            shipped_date=shipped_date,
            locale=locale
        ))

    return shipments

def parse_gift_cards(soup, locale=Locale_en_US) -> List[Shipment]:
    """
    Parses Gift Card Table Part of HTML document (1st Table)
    """
    def is_gift_card_header_table(node):
        if node.name != 'table':
            return False
        text = node.text.strip()
        m = re.match(locale.gift_card, text)
        if m is not None:
            # check if a matching subtable exists
            sub_table = node.find_all(is_gift_card_header_table)
            if sub_table == []:
                # only match if it is the innermost table
                return True
        return False

    header_tables = soup.find_all(is_gift_card_header_table)

    if header_tables is []:
        # if no gift cards in order
        logger.debug('no gift card table found')
        return []

    shipments = []  # type: List[Shipment]
    errors = []  # type: Errors

    for header_table in header_tables:
        logger.debug('parsing gift card items...')
        items = []  # type: List[Item]

        shipment_table = header_table.find_parent('table')
        items_ordered_header = shipment_table.find(
            lambda node: is_items_ordered_header(node, locale))
        item_rows = [items_ordered_header]

        for item_row in item_rows:
            tds = item_row('td')
            description_node = tds[0]
            price_node = tds[1]
            price = price_node.text.strip()
            price = price.split('\n')[1]

            if price is None:
                price = Amount(D(0), locale.currency)
            else:
                price = locale.parse_amount(price)

            m = re.search(locale.gift_card_to, description_node.text.strip(), re.MULTILINE|re.UNICODE)
            if m is None:
                # if no match is found
                # check if Amazon account has been charged up
                m = re.search(locale.gift_card_amazon_account, description_node.text.strip(), re.MULTILINE|re.UNICODE)
            if m is None:
                errors.append('Failed to extract item description')
                description=''
            else:
                description = m.group('type').strip() + ' ' + m.group('sent_to').strip()

            items.append(
                Item(
                    quantity=D(1),
                    description=description,
                    sold_by=None,
                    condition=None,
                    price=price,
                ))

        shipments.append(parse_shipment_payments(
            shipment_table,
            items,
            errors,
            shipped_date=None,
            locale=locale
        ))

    return shipments


def parse_shipment_payments(
        shipment_table,
        items, errors,
        shipped_date=None,
        locale=Locale_en_US) -> Shipment:
    """ Parse payment information of single shipments and gift card orders.
    """
    logger.debug('parsing shipment amounts...')
    # consistency check: shipment subtotal against sum of item prices
    items_subtotal = locale.parse_amount(
        get_field_in_table(shipment_table, locale.items_subtotal))

    expected_items_subtotal = reduce_amounts(
        beancount.core.amount.mul(x.price, D(x.quantity)) for x in items)
    if (items_subtotal is not None and
        expected_items_subtotal != items_subtotal):
        errors.append(
            'expected items subtotal is %r, but parsed value is %r' %
            (expected_items_subtotal, items_subtotal))

    # parse pre- and posttax adjustments for shipment
    output_fields = dict()
    output_fields['pretax_adjustments'] = get_adjustments_in_table(
        shipment_table, locale.pretax_adjustment_fields_pattern, locale=locale)
    output_fields['posttax_adjustments'] = get_adjustments_in_table(
        shipment_table, locale.posttax_adjustment_fields_pattern, locale=locale)
    # compare total before tax
    pretax_parts = [items_subtotal or expected_items_subtotal] + [
        a.amount for a in output_fields['pretax_adjustments']
    ]
    expected_total_before_tax = reduce_amounts(pretax_parts)
    total_before_tax = locale.parse_amount(
        get_field_in_table(shipment_table, locale.total_before_tax))
    if total_before_tax is None:
        total_before_tax = expected_total_before_tax
    elif expected_total_before_tax != total_before_tax:
        errors.append(
            'expected total before tax is %s, but parsed value is %s' %
            (expected_total_before_tax, total_before_tax))

    sales_tax = get_adjustments_in_table(shipment_table, locale.shipment_sales_tax, locale=locale)

    if locale.tax_included_in_price:
        # tax is already inlcuded in item prices
        # do not add additional Adjustment for taxes
        sales_tax = []
    
    # compare total
    posttax_parts = (
        [total_before_tax] + [a.amount for a in sales_tax] +
        [a.amount for a in output_fields['posttax_adjustments']])
    expected_total = reduce_amounts(posttax_parts)
    total = locale.parse_amount(
        get_field_in_table(shipment_table, locale.shipment_total))
    if total is None:
        total = expected_total
    elif expected_total != total:
        errors.append('expected total is %s, but parsed value is %s' %
                        (expected_total, total))

    logger.debug('...finshed parsing shipment')
    return Shipment(
            shipped_date=shipped_date,
            items=items,
            items_subtotal=items_subtotal,
            total_before_tax=total_before_tax,
            tax=sales_tax,
            total=total,
            errors=errors,
            **output_fields)


def parse_credit_card_transactions_from_payments_table(
        payment_table,
        order_date: datetime.date,
        locale=Locale_en_US) -> List[CreditCardTransaction]:
    """ Parse payment information from payments table.
    Only type and last digits are given, no amount (assuming grand total).
    Other payment methods than credit card are possible:
    - Direct Debit (DE: Bankeinzug)
    """
    payment_text = '\n'.join(payment_table.strings)
    m = re.search(locale.grand_total, payment_text)
    assert m is not None
    grand_total = locale.parse_amount(m.group(1).strip())

    for regex in locale.payment_type:
        m = re.search(regex, payment_text)
        if m is not None:
            # only take first matching regex, discard others!
            break

    if m is None:
        return []

    credit_card_transactions = [
        CreditCardTransaction(
            date=order_date,
            amount=grand_total,
            card_description=m.group(1).strip(),
            card_ending_in=m.group(2).strip(),
        )
    ]
    return credit_card_transactions


def parse_credit_card_transactions(soup, locale=Locale_en_US) -> List[CreditCardTransaction]:
    """ Parse Credit Card Transactions from bottom sub-table of payments table.
    Transactions are listed with type, 4 digits, transaction date and amount.
    """
    def is_header_node(node):
        return node.name == 'td' and node.text.strip(
        ) == locale.credit_card_transactions

    header_node = soup.find(is_header_node)
    if header_node is None:
        return []
    sibling = header_node.find_next_sibling('td')
    rows = sibling.find_all('tr')
    transactions = []  # type: List[CreditCardTransaction]
    for row in rows:
        if not row.text.strip():
            continue
        tds = row('td')
        description = tds[0].text.strip()
        amount_text = tds[1].text.strip()
        m = re.match(locale.credit_card_last_digits, description,
                    re.UNICODE)
        assert m is not None
        transactions.append(
            CreditCardTransaction(
                date=locale.parse_date(m.group(3)),
                card_description=m.group(1),
                card_ending_in=m.group(2),
                amount=locale.parse_amount(amount_text),
            ))
    return transactions


def parse_invoice(path: str, locale=Locale_en_US) -> Optional[Order]:
    """ 1st method to call, distinguish between regular and digital invoice.
    """
    if os.path.basename(path).startswith('D'):
        logger.debug('identified as digital invoice')
        return parse_digital_order_invoice(path, locale=locale)
    logger.debug('identified as regular invoice')
    return parse_regular_order_invoice(path, locale=locale)


def parse_regular_order_invoice(path: str, locale=Locale_en_US) -> Order:
    """ Parse regular order type invoice (HTML document)
    1. parse all shipment tables with individual items
    2. parse payment table
    3. sanity check totals extracted from item prices and payment table
    """
    errors = []  # type: Errors
    with open(path, 'rb') as f:
        soup = bs4.BeautifulSoup(f.read(), 'lxml')
    
    # -----------------
    # Order ID & Order placed date
    # -----------------
    logger.debug('parsing order id and order placed date...')
    title = soup.find('title').text.strip()
    m = re.fullmatch(locale.regular_order_id, title.strip())
    assert m is not None
    order_id=m.group(1)

    def is_order_placed_node(node):
        m = re.fullmatch(locale.regular_order_placed, node.text.strip())
        return m is not None

    node = soup.find(is_order_placed_node)
    m = re.fullmatch(locale.regular_order_placed, node.text.strip())
    assert m is not None
    order_date = locale.parse_date(m.group(1))

    # ----------------------
    # Shipments & Gift Cards
    # ----------------------
    logger.debug('parsing shipments...')
    shipments = parse_shipments(soup, locale=locale)
    if hasattr(locale, 'gift_card'):
        shipments += parse_gift_cards(soup, locale=locale)
    if len(shipments) == 0:
        # no shipment or gift card tables found
        msg = ('Identified regular order invoice but no items were found '
               + '(neither shipments nor gift cards). This may be a new type. '
               + 'Consider opening an issue at jbms/beancount-import on github.')
        logger.warning(msg)
        errors.append(msg)
        # do not throw exception, continue parsing the payment table
    logger.debug('finished parsing shipments')

    # -------------------------------------------
    # Payment Table: Pre- and Posttax Adjustments
    # -------------------------------------------
    # Aim: Parse all pre- and posttax adjustments
    #      consistency check grand total against sum of item costs
    logger.debug('parsing payment table...')
    payment_table_header = soup.find(
        lambda node: node.name == 'table' and re.match(
            locale.payment_information, node.text.strip()))

    payment_table = payment_table_header.find_parent('table')

    logger.debug('parsing pretax adjustments...')
    output_fields = dict()  # type: Dict[str, List[Adjustment]]
    output_fields['pretax_adjustments'] = get_adjustments_in_table(
        payment_table, locale.pretax_adjustment_fields_pattern, locale=locale)
    
    # older invoices put pre-tax amounts on a per-shipment basis
    # new invoices only put pre-tax amounts on the overall payments section
    # detect which this is
    
    # payment table pretax adjustments
    pretax_amount = reduce_amounts(
        a.amount for a in output_fields['pretax_adjustments'])
    
    shipments_pretax_amount = None
    if any(s.pretax_adjustments for s in shipments):
        # sum over all shipment pretax amounts
        shipments_pretax_amount = reduce_amounts(a.amount
            for shipment in shipments
            for a in shipment.pretax_adjustments)            

        if shipments_pretax_amount != pretax_amount:
            errors.append(
                'expected total pretax adjustment to be %s, but parsed total is %s'
                % (shipments_pretax_amount, pretax_amount))

    logger.debug('parsing posttax adjustments...')
    # parse first to get an idea of the working currency
    grand_total = locale.parse_amount(
        get_field_in_table(payment_table, locale.regular_total_order))

    payment_adjustments = collections.OrderedDict()  # type: Dict[str, Amount]
    payments_total_adjustments = []  # type: List[Amount]
    shipments_total_adjustments = []  # type: List[Amount]

    def resolve_posttax_adjustments() -> List[Adjustment]:
        """ Extract and compare posttax adjustments
        from shipment and payment tables.
        Returns list of reduced Adjustments.
        """
        # get reduced form of adjustments from payment table
        payment_adjustments.update(
            reduce_adjustments(
                get_adjustments_in_table(payment_table,
                                        locale.posttax_adjustment_fields_pattern,
                                        assumed_currency=grand_total.currency,
                                        locale=locale)))
        # adjustments from all shipments, reduced
        all_shipments_adjustments = collections.OrderedDict(
            reduce_adjustments(
                sum((x.posttax_adjustments for x in shipments), [])))
        
        # initialize dict with all adjustment keys, values not used
        # dict ensures that keys are unique
        all_keys = collections.OrderedDict(payment_adjustments.items())
        all_keys.update(all_shipments_adjustments.items())
        
        # combine shipment and payment adjustments
        # make sure that shipment adjustments match payment adjustments
        all_adjustments = collections.OrderedDict()  # type: Dict[str, Amount]
        for key in all_keys:
            payment_amount = payment_adjustments.get(key)
            shipments_amount = all_shipments_adjustments.get(key)
            amount = payment_amount
            if payment_amount is None and shipments_amount is not None:
                # Amazon sometimes doesn't include adjustments in the Payments table
                amount = shipments_amount
                payments_total_adjustments.append(amount)
            elif payment_amount is not None and shipments_amount is None:
                # Amazon sometimes doesn't include these adjustments in the Shipment table
                shipments_total_adjustments.append(amount)
            elif payment_amount != shipments_amount:
                # Both tables include adjustment with same label, but amount does not match
                errors.append(
                    'expected total %r to be %s, but parsed total is %s' %
                    (key, shipments_amount, payment_amount))
            all_adjustments[key] = amount
        return [Adjustment(k, v) for k, v in all_adjustments.items()]

    output_fields['posttax_adjustments'] = resolve_posttax_adjustments()

    logger.debug('consistency check taxes...')
    # tax from payment table
    tax = locale.parse_amount(
        get_field_in_table(payment_table, locale.regular_estimated_tax))

    # tax from shipment tables
    expected_tax = reduce_amounts(
        a.amount for shipment in shipments for a in shipment.tax)
    if expected_tax is None:
        # tax not given on shipment level
        if not locale.tax_included_in_price:
            # add tax to adjustments if not already included in item prices
            shipments_total_adjustments.append(tax)
    elif expected_tax != tax:
        errors.append(
            'expected tax is %s, but parsed value is %s' % (expected_tax, tax))

    if locale.tax_included_in_price:
        # tax is already inlcuded in item prices
        # do not add additional transaction for taxes
        tax = None

    logger.debug('consistency check grand total...')
    payments_total_adjustment = reduce_amounts(payments_total_adjustments)
    shipments_total_adjustment = reduce_amounts(shipments_total_adjustments)

    expected_total = add_amount(shipments_total_adjustment,
                                reduce_amounts(x.total for x in shipments))

    # if no shipments pre-tax section, then the expected total isn't accounting
    # for the pre-tax adjustments yet since they are only in the grand total section
    if shipments_pretax_amount is None:
        expected_total = add_amount(expected_total, pretax_amount)

    adjusted_grand_total = add_amount(payments_total_adjustment, grand_total)
    if expected_total != adjusted_grand_total:
        errors.append('expected grand total is %s, but parsed value is %s' %
                    (expected_total, adjusted_grand_total))

    # ---------------------------------------
    # Payment Table: Credit Card Transactions
    # ---------------------------------------
    logger.debug('parsing credit card transactions...')
    credit_card_transactions = parse_credit_card_transactions(soup, locale=locale)
    if not credit_card_transactions:
        # no explicit credit card transaction table
        logger.debug('no credit card transactions table given, falling back to payments table')
        credit_card_transactions = parse_credit_card_transactions_from_payments_table(
            payment_table, order_date, locale=locale)

    if credit_card_transactions:
        total_payments = reduce_amounts(
            x.amount for x in credit_card_transactions)
    else:
        logger.debug('no payment transactions found, assumig grand total as total payment amount')
        total_payments = grand_total
    if total_payments != adjusted_grand_total:
        errors.append('total payment amount is %s, but grand total is %s' %
                      (total_payments, adjusted_grand_total))

    logger.debug('...finished parsing regular invoice.')
    return Order(
        order_date=order_date,
        order_id=order_id,
        shipments=shipments,
        credit_card_transactions=credit_card_transactions,
        tax=tax,
        errors=sum((shipment.errors
                    for shipment in shipments), cast(Errors, [])) + errors,
        **output_fields)


def get_text_lines(parent_node):
    """ Format nodes into list of strings
    """
    text_lines = ['']
    for node in parent_node.children:
        if isinstance(node, bs4.NavigableString):
            text_lines[-1] += str(node)
        elif node.name == 'br':
            text_lines.append('')
        else:
            text_lines[-1] += node.text
    return text_lines


def parse_digital_order_invoice(path: str, locale=Locale_en_US) -> Optional[Order]:
    """ Parse digital order type invoice (HTML document)
    1. parse all digital items tables
    2. parse amounts
    3. parse payment table
    """
    errors = []  # type: Errors
    with open(path, 'rb') as f:
        soup = bs4.BeautifulSoup(f.read(), 'lxml')

    logger.debug('check if order has been cancelled...')
    def is_cancelled_order(node):
        return node.text.strip() == locale.digital_order_cancelled

    if soup.find(is_cancelled_order):
        return None

    # --------------------------------------------------
    # Find Digital Order Header, parse date and order ID
    # --------------------------------------------------
    logger.debug('parsing header...')
    def is_digital_order_row(node):
        if node.name != 'tr':
            return False
        m = re.match(locale.digital_order, node.text.strip())
        if m is None:
            return False
        try:
            locale.parse_date(m.group(1))
            return True
        except:
            return False

    digital_order_header = soup.find(is_digital_order_row)
    digital_order_table = digital_order_header.find_parent('table')
    m = re.match(locale.digital_order, digital_order_header.text.strip())
    if m is None:
        msg = ('Identified digital order invoice but no digital orders were found.')
        logger.warning(msg)
        errors.append(msg)
        # throw exception since there is no other possibility to get order_date
        assert m is not None
    order_date = locale.parse_date(m.group(1))

    order_id_td = soup.find(
        lambda node: node.name == 'td' and
        re.match(locale.digital_order_id, node.text.strip())
        )
    m = re.match(locale.digital_order_id, order_id_td.text.strip())
    assert m is not None
    order_id = m.group(1)

    # -----------
    # Parse Items
    # -----------
    logger.debug('parsing items...')
    items_ordered_header = digital_order_table.find(
        lambda node: is_items_ordered_header(node, locale))
    item_rows = items_ordered_header.find_next_siblings('tr')
    
    items = []  # Sequence[DigitalItem]
    other_fields_td = None

    for item_row in item_rows:
        tds = item_row('td')
        if len(tds) != 2:
            # payment information on order level (not payment table)
            # differently formatted, take first column only
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

        by = get_label_value(locale.digital_by)
        sold_by = get_label_value(locale.digital_sold_by)

        items.append(
            DigitalItem(
                description=description,
                by=by,
                sold_by=sold_by,
                url=url,
                price=locale.parse_amount(price),
            ))

    other_fields_text_lines = get_text_lines(other_fields_td)

    # -------------------------------------------
    # Parse Amounts, Pre- and Posttax Adjustments
    # -------------------------------------------
    logger.debug('parsing amounts...')
    def get_other_field(pattern, allow_multiple=False, return_label=False):
        """ Look for pattern in other_fields_text_lines
        """
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
                Adjustment(amount=locale.parse_amount(amount_str), description=label))
        return adjustments

    def get_amounts_in_text(pattern_map):
        amounts = dict()
        for key, label in pattern_map.items():
            amount = locale.parse_amount(get_other_field(label))
            amounts[key] = amount
        return amounts
    
    items_subtotal = locale.parse_amount(
        get_other_field(locale.items_subtotal))
    total_before_tax = locale.parse_amount(
        get_other_field(locale.total_before_tax))
    tax = get_adjustments(locale.digital_tax_collected)
    total_for_this_order = locale.parse_amount(
        get_other_field(locale.digital_total_order))
    
    logger.debug('parsing pretax adjustments...')
    output_fields = dict()
    output_fields['pretax_adjustments'] = get_adjustments(
        locale.pretax_adjustment_fields_pattern)
    pretax_parts = ([items_subtotal] +
                    [a.amount for a in output_fields['pretax_adjustments']])
    expected_total_before_tax = reduce_amounts(pretax_parts)
    if expected_total_before_tax != total_before_tax:
        errors.append('expected total before tax is %s, but parsed value is %s'
                    % (expected_total_before_tax, total_before_tax))
    
    logger.debug('parsing posttax adjustments...')
    output_fields['posttax_adjustments'] = get_adjustments(
        locale.posttax_adjustment_fields_pattern)
    posttax_parts = ([total_before_tax] + [a.amount for a in tax] +
                     [a.amount for a in output_fields['posttax_adjustments']])
    expected_total = reduce_amounts(posttax_parts)

    if expected_total != total_for_this_order:
        errors.append('expected total is %s, but parsed value is %s' %
                      (expected_total, total_for_this_order))

    if locale.tax_included_in_price:
        # tax is already inlcuded in item prices
        # do not add additional transaction for taxes
        tax = []

    shipment = Shipment(
        shipped_date=order_date,
        items=cast(List[Union[Item, DigitalItem]], items),
        items_subtotal=items_subtotal,
        total_before_tax=total_before_tax,
        tax=tax,
        total=total_for_this_order,
        errors=errors,
        **output_fields)

    # -------------
    # Payment Table
    # -------------
    logger.debug('parsing payment information...')
    payment_table = soup.find(
        lambda node: node.name == 'table' and
        node.text.strip().startswith(locale.digital_payment_information)
        )
    credit_card_transactions = parse_credit_card_transactions_from_payments_table(
        payment_table, order_date, locale=locale)

    logger.debug('...finished parsing digital invoice.')

    return Order(
        order_date=order_date,
        order_id=order_id,
        shipments=[shipment],
        credit_card_transactions=credit_card_transactions,
        pretax_adjustments=[],
        posttax_adjustments=output_fields['posttax_adjustments'],
        # tax given on "shipment level"
        # for digital orders tax is always given on shipment level
        # therefore tax on order level is irrelevant
        tax=None,
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
    ap.add_argument(
        '--locale', default='en_US', help='Local Amazon settings, defaults to en_US')
    ap.add_argument('paths', nargs='*')

    args = ap.parse_args()
    locale = LOCALES[args.locale]
    results = []
    for path in args.paths:
        try:
            result = parse_invoice(path, locale=locale)
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
