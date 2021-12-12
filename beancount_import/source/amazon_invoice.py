"""Parses an Amazon.com/.de regular or digital order details HTML file."""

from typing import NamedTuple, Optional, List, Union, Iterable, Dict, Sequence, cast
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

logger = logging.getLogger('amazon')


class Locale_EN():
    LOCALE = 'EN'
    tax_included_in_price = False
    shipped_pattern = '^Shipped on ([^\\n]+)$'
    nonshipped_headers = {
        'Service completed',
        'Preparing for Shipment',
        'Not Yet Shipped',
        'Shipping now'
    }
    items_ordered = 'Items Ordered'
    price = 'Price'
    currency = 'USD'
    of = 'of:'
    seller_profile = ' (seller profile)'
    items_subtotal_regex = r'Item\(s\) Subtotal:'
    total_before_tax_regex = 'Total Before Tax:'
    sales_tax_shipment = 'Sales Tax:'
    total_shipment = 'Total for This Shipment:'

    pattern_without_condition = r'(?P<description>.*)\n\s*(?:Sold|Provided) by:? (?P<sold_by>[^\n]+)'
    pattern_with_condition = pattern_without_condition + r'\n.*\n\s*Condition: (?P<condition>[^\n]+)'

    # Payment Table & Credit Card Transactions
    grand_total_regex = r'\n\s*Grand Total:\s+(.*)\n'
    credit_card_transactions = 'Credit Card transactions'
    last_digits_regex = r'^([^:]+) ending in ([0-9]+):\s+([^:]+):$'
    payment_type_regexes = [
        # only first matching regex is used!
        r'\n\s*([^\s|][^|\n]*[^|\s])\s+\|\s+Last (?:4 )?digits:\s+([0-9]{4})\n',
        r'\n\s*(.+)\s+ending in\s+([0-9]{4})\n'
        ]
    payment_information = '^Payment information$'
    grand_total = 'Grand Total:'

    # Page Header
    order_placed_regex = r'(?:Subscribe and Save )?Order Placed:\s+([^\s]+ \d+, \d{4})'
    order_id_regular = r'.*Order ([0-9\-]+)'

    # digital invoice
    order_cancelled = 'Order Canceled'
    digital_order = 'Digital Order: (.*)'
    by = 'By'
    sold_by = r'Sold\s+By'
    tax_collected_digital = 'Tax Collected:'
    estimated_tax = 'Estimated tax to be collected:'
    total_order_digital = 'Total for this Order:'
    order_id_digital = '^Amazon.com\\s+order number:\\s+(D[0-9-]+)$'
    payment_information_digital = 'Payment Information'
    
    pretax_adjustment_fields_pattern = ('(?:' + '|'.join([
        'Shipping & Handling', # Verpackung & Versand:
        'Free Shipping',
        'Free delivery',
        'Pantry delivery',
        'Promotion(?:s| Applied)', # Gutschein eingelöst:
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
    posttax_adjustment_fields_pattern = r'Gift Card Amount:|Rewards Points:|Tip [(]optional[)]:|Recycle Fee \$X'

    @staticmethod
    def parse_amount(amount, assumed_currency=None) -> Amount:
        return parse_amount(amount, assumed_currency=assumed_currency)

    @staticmethod
    def parse_date(date_str) -> str:
        return dateutil.parser.parse(date_str).date()


class Locale_DE():
    """Language and region specific settings for parsing amazon.de invoices
    """
    LOCALE = 'DE'
    tax_included_in_price = True  # no separate tax transactions
    shipped_pattern = '^versandt am ([^\\n]+)$'
    nonshipped_headers = { # Translations missing
        'Service completed',
        'Preparing for Shipment',
        'Not Yet Shipped',
        'Shipping now'
    }
    items_ordered = 'Bestellte Artikel'
    price = 'Preis'
    currency = 'EUR'
    of = 'Exemplar(e) von:'
    seller_profile = ' (Mitgliedsprofil)'
    items_subtotal_regex = 'Zwischensumme:'
    total_before_tax_regex = 'Summe ohne MwSt.:'
    sales_tax_shipment = 'Anzurechnende MwSt.:' # not sure (only old invoices)
    total_shipment = 'Gesamtsumme:'

    pattern_without_condition = r'(?P<description>.*)\n\s*(?:Verkauf|Provided) durch:? (?P<sold_by>[^\n]+)'
    # Provided by: Translation missing
    pattern_with_condition = pattern_without_condition + r'\n.*\n\s*Zustand: (?P<condition>[^\n]+)'
 
    # Payment Table & Credit Card Transactions
    grand_total_regex = r'\n\s*(?:Gesamtsumme|Endsumme):\s+(.*)\n' # regular: Gesamtsumme, digital: Endsumme
    credit_card_transactions = 'Kreditkarten-Transaktionen'
    last_digits_regex = r'^([^:]+) mit den Endziffern ([0-9]+):\s+([^:]+):$'
    payment_type_regexes = [
        # only first matching regex is used!
        r'\n\s*([^\s|][^|\n]*[^|\s])\s+\|\s+Die letzten (?:4 )?Ziffern:\s+([0-9]{3,4})\n', # 3 digits for Bankeinzug
        r'\n\s*(.+)\s+mit den Endziffern\s+([0-9]{4})\n'
        ]
    payment_information = '^Zahlungsdaten$'
    grand_total = 'Gesamtsumme:'

    # Page Header
    order_placed_regex = r'(?:Subscribe and Save )?Bestellung aufgegeben am:\s+(\d+\. [^\s]+ \d{4})'
    # Translation missing: Subscribe and Save -> Sparabo??
    order_id_regular = r'.*Bestellung ([0-9\-]+)'

    # digital invoice
    order_cancelled = 'Order Canceled'
    digital_order = 'Digitale Bestellung: (.*)'
    by = 'Von'
    sold_by = r'Verkauft von'
    tax_collected_digital = 'MwSt:'
    estimated_tax = 'Anzurechnende MwSt.:'
    total_order_digital = 'Endsumme:'
    order_id_digital = '^Amazon.de\\s+Bestellnummer:\\s+(D[0-9-]+)$'
    payment_information_digital = 'Zahlungsinformation'

    # most of translations still missing ...
    pretax_adjustment_fields_pattern = ('(?:' + '|'.join([
        'Verpackung & Versand',
        'Free Shipping',
        'Free delivery',
        'Pantry delivery',
        'Gutschein eingelöst', # english version not removed yet
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
    posttax_adjustment_fields_pattern = r'Gift Card Amount:|Rewards Points:|Tip [(]optional[)]:|Recycle Fee \$X'


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
                Locale_DE._format_number_str(amount),
                assumed_currency=assumed_currency)

    class _parserinfo(dateutil.parser.parserinfo):
        MONTHS=[
            ('Jan', 'Januar'), ('Feb', 'Februar'), ('Mär', 'März'),
            ('Apr', 'April'), ('Mai', 'Mai'), ('Jun', 'Juni'),
            ('Jul', 'Juli'), ('Aug', 'August'), ('Sep', 'September'),
            ('Okt', 'Oktober'), ('Nov', 'November'), ('Dez', 'Dezember')
            ]
    
    @staticmethod
    def parse_date(date_str) -> str:
        return dateutil.parser.parse(date_str, parserinfo=Locale_DE._parserinfo(dayfirst=True)).date()


LOCALE = {x.LOCALE : x for x in [Locale_EN, Locale_DE]}

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
    ('shipped_date', Optional[datetime.date]),
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


class AmazonInvoice():
    def __init__(self, locale='EN'):
        self.locale = LOCALE[locale]

    @staticmethod
    def add_amount(a: Optional[Amount], b: Optional[Amount]) -> Optional[Amount]:
        """Add two amounts, amounts with value `None` are ignored.
        """
        if a is None:
            return b
        if b is None:
            return a
        return beancount.core.amount.add(a, b)

    @staticmethod
    def reduce_amounts(amounts: Iterable[Amount]) -> Optional[Amount]:
        """Reduce iterable of amounts to sum by applying `add_amount`.
        """
        return functools.reduce(AmazonInvoice.add_amount, amounts, None)

    @staticmethod
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

    def get_adjustments_in_table(self, table, pattern, assumed_currency=None):
        adjustments = []
        for label, amount_str in AmazonInvoice.get_field_in_table(
                table, pattern, allow_multiple=True, return_label=True):
            adjustments.append(
                Adjustment(amount=self.locale.parse_amount(amount_str, assumed_currency), 
                        description=label))
        return adjustments

    @staticmethod
    def reduce_adjustments(adjustments: List[Adjustment]) -> List[Adjustment]:
        all_adjustments = collections.OrderedDict()  # type: Dict[str, List[Amount]]
        for adjustment in adjustments:
            all_adjustments.setdefault(adjustment.description,
                                    []).append(adjustment.amount)
        return [
            Adjustment(k, AmazonInvoice.reduce_amounts(v)) for k, v in all_adjustments.items()
        ]


    def parse_shipments(self, soup) -> List[Shipment]:
        """
        Parses Shipment Table Part of HTML document (1st Table)
        """

        # shipped_pattern = '^Shipped on ([^\\n]+)$'
        # # versandt am 27. September 2021
        # # Shipped on February 8, 2016
        # nonshipped_headers = {
        #     'Service completed',
        #     'Preparing for Shipment',
        #     'Not Yet Shipped',
        #     'Shipping now'
        # }

        def is_shipment_header_table(node):
            if node.name != 'table':
                return False
            text = node.text.strip()
            m = re.match(self.locale.shipped_pattern, text)
            return m is not None or text in self.locale.nonshipped_headers

        header_tables = soup.find_all(is_shipment_header_table)

        shipments = []  # type: List[Shipment]
        errors = []  # type: Errors

        for header_table in header_tables:
            text = header_table.text.strip()
            shipped_date = None
            if text not in self.locale.nonshipped_headers:
                m = re.match(self.locale.shipped_pattern, text)
                assert m is not None
                shipped_date = self.locale.parse_date(m.group(1))

            items = []

            shipment_table = header_table.find_parent('table')

            def is_items_ordered_header(node):
                if node.name != 'tr':
                    return False
                tds = node('td')
                if len(tds) < 2:
                    return False
                return (tds[0].text.strip() == self.locale.items_ordered and
                        tds[1].text.strip() == self.locale.price)
                # Items Ordered
                # Bestellte Artikel
                # Price
                # Preis

            items_ordered_header = shipment_table.find(is_items_ordered_header)

            item_rows = items_ordered_header.find_next_siblings('tr')

            logger.info('Parsing Shipment Items')
            for item_row in item_rows:
                tds = item_row('td')
                description_node = tds[0]
                price_node = tds[1]
                price = price_node.text.strip()

                if price is None:
                    price = Amount(D(0), self.locale.currency)
                    # EUR 16,99
                    # $11.87
                else:
                    price = self.locale.parse_amount(price)

                # 1 of: 365 Everyday Value, Potato Yellow Bag Organic, 48 Ounce
                # 2 (1.04 lb) of: Broccoli Crowns Conventional, 1 Each
                # 2.07 lb of: Pork Sausage Link Italian Mild Step 1

                pattern_quantity = r'^\s*(?:(?P<quantity>[0-9]+)|(?P<weight1>[0-9.]+\s+(?:lb|kg))|(?:(?P<quantityIgnore>[0-9.]+) [(](?P<weight2>[^)]+)[)]))\s+of:'
                # ToDo: check if this matches all locales, e.g. 'of' and units
                m = re.match(pattern_quantity, description_node.text, re.UNICODE|re.DOTALL)
                quantity = 1
                if m is not None:
                    # Amazon will say you got, e.g. 2 broccoli crowns at $1.69/lb - but then this code multiplies the 2 by the price listed
                    # on the invoice, which is the total price in this case (but the per-unit price in other cases) - so if there's a quantity
                    # and a weight, ignore the quantity and treat it as 1
                    # alternately, capture the weight and the per-unit price and multiply out
                    quantity = m.group("quantity") # ignore quantity for weight items
                
                if quantity is None:
                    #print("Unable to extract quantity, using 1: %s" % description_node.text)
                    quantity = D(1)
                else:
                    quantity = D(quantity)

                text = description_node.text.split(self.locale.of, 1)[1]
                # Übersetzung fehlt

                m = re.match(self.locale.pattern_with_condition, text, re.UNICODE | re.DOTALL)
                if m is None:
                    m = re.match(self.locale.pattern_without_condition, text, re.UNICODE | re.DOTALL)
                if m is None:
                    raise Exception("Could not extract item from row", text)
                
                description = re.sub(r'\s+', ' ', m.group('description').strip())
                sold_by = re.sub(r'\s+', ' ', m.group('sold_by').strip())
                try:
                    condition = re.sub(r'\s+', ' ', m.group('condition').strip())
                except IndexError:
                    condition = None
                suffix = self.locale.seller_profile
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
            
            logger.info('Parsing Shipment Amounts')
            items_subtotal = self.locale.parse_amount(
                self.get_field_in_table(shipment_table, self.locale.items_subtotal_regex))

            expected_items_subtotal = self.reduce_amounts(
                beancount.core.amount.mul(x.price, D(x.quantity)) for x in items)
            if (items_subtotal is not None and
                expected_items_subtotal != items_subtotal):
                errors.append(
                    'expected items subtotal is %r, but parsed value is %r' %
                    (expected_items_subtotal, items_subtotal))

            output_fields = dict()
            output_fields['pretax_adjustments'] = self.get_adjustments_in_table(
                shipment_table, self.locale.pretax_adjustment_fields_pattern)
            output_fields['posttax_adjustments'] = self.get_adjustments_in_table(
                shipment_table, self.locale.posttax_adjustment_fields_pattern)
            pretax_parts = [items_subtotal or expected_items_subtotal] + [
                a.amount for a in output_fields['pretax_adjustments']
            ]
            total_before_tax = self.locale.parse_amount(
                self.get_field_in_table(shipment_table, self.locale.total_before_tax_regex))
            expected_total_before_tax = self.reduce_amounts(pretax_parts)
            if total_before_tax is None:
                total_before_tax = expected_total_before_tax
            elif expected_total_before_tax != total_before_tax:
                errors.append(
                    'expected total before tax is %s, but parsed value is %s' %
                    (expected_total_before_tax, total_before_tax))

            sales_tax = self.get_adjustments_in_table(shipment_table, self.locale.sales_tax_shipment)
            # Sales Tax:
            # Anzurechnende MwSt.:

            posttax_parts = (
                [total_before_tax] + [a.amount for a in sales_tax] +
                [a.amount for a in output_fields['posttax_adjustments']])
            total = self.locale.parse_amount(
                self.get_field_in_table(shipment_table, self.locale.total_shipment))
                # Total for This Shipment:
                # Gesamtsumme:
            expected_total = self.reduce_amounts(posttax_parts)
            if total is None:
                total = expected_total
            elif expected_total != total:
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
            self,
            payment_table,
            order_date: datetime.date) -> List[CreditCardTransaction]:
        """ Parse payment information from payments table.
        Only type and last digits are given, no amount (assuming grand total).
        Other payment methods than credit card are possible:
        - Direct Debit (DE: Bankeinzug)
        """
        payment_text = '\n'.join(payment_table.strings)
        m = re.search(self.locale.grand_total_regex, payment_text)
        assert m is not None
        grand_total = self.locale.parse_amount(m.group(1).strip())

        for regex in self.locale.payment_type_regexes:
            m = re.search(regex, payment_text)
            if m is not None:
                break

        # m = re.search(self.locale.last_digits_regex1, payment_text)
        # if m is None:
        #     m = re.search(self.locale.last_digits_regex2, payment_text)

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


    def parse_credit_card_transactions(self, soup) -> List[CreditCardTransaction]:
        """ Parse Credit Card Transactions from bottom sub-table of payments table.
        Transactions are listed with type, 4 digits, transaction date and amount.
        """
        def is_header_node(node):
            return node.name == 'td' and node.text.strip(
            ) == self.locale.credit_card_transactions

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
            m = re.match(self.locale.last_digits_regex, description,
                        re.UNICODE)
            assert m is not None
            transactions.append(
                CreditCardTransaction(
                    date=self.locale.parse_date(m.group(3)),
                    card_description=m.group(1),
                    card_ending_in=m.group(2),
                    amount=self.locale.parse_amount(amount_text),
                ))
        return transactions


    def parse_invoice(self, path: str) -> Optional[Order]:
        """ 1st method to call, distinguish between regular and digital invoice.
        """
        if os.path.basename(path).startswith('D'):
            logger.info('identified as digital invoice')
            return self.parse_digital_order_invoice(path)
        logger.info('identified as regular invoice')
        return self.parse_regular_order_invoice(path)


    def parse_regular_order_invoice(self, path: str) -> Order:
        errors = []
        with open(path, 'rb') as f:
            soup = bs4.BeautifulSoup(f.read(), 'lxml')
        logger.info('parsing shipments...')
        shipments = self.parse_shipments(soup)
        logger.info('finished parsing shipments')
        logger.info('parsing payment table...')
        payment_table_header = soup.find(
            lambda node: node.name == 'table' and re.match(
                self.locale.payment_information, node.text.strip()))

        payment_table = payment_table_header.find_parent('table')

        logger.debug('parsing pretax adjustments...')
        output_fields = dict()
        output_fields['pretax_adjustments'] = self.get_adjustments_in_table(
            payment_table, self.locale.pretax_adjustment_fields_pattern)
        payment_adjustments = collections.OrderedDict()  # type: Dict[str, Amount]

        # older invoices put pre-tax amounts on a per-shipment basis
        # new invoices only put pre-tax amounts on the overall payments section
        # detect which this is
        pretax_amount = self.reduce_amounts(
            a.amount for a in output_fields['pretax_adjustments'])
        shipments_pretax_amount = None

        if any(s.pretax_adjustments for s in shipments):
            shipments_pretax_amount = self.reduce_amounts(a.amount
                for shipment in shipments
                for a in shipment.pretax_adjustments)            

            if shipments_pretax_amount != pretax_amount:
                errors.append(
                    'expected total pretax adjustment to be %s, but parsed total is %s'
                    % (shipments_pretax_amount, pretax_amount))


        logger.debug('parsing posttax adjustments...')
        payments_total_adjustments = []
        shipments_total_adjustments = []

        # parse first to get an idea of the working currency
        grand_total = self.locale.parse_amount(
            self.get_field_in_table(payment_table, self.locale.grand_total))

        def resolve_posttax_adjustments():
            payment_adjustments.update(
                self.reduce_adjustments(
                    self.get_adjustments_in_table(payment_table,
                                            self.locale.posttax_adjustment_fields_pattern,
                                            assumed_currency=grand_total.currency)))
            all_shipments_adjustments = collections.OrderedDict(
                self.reduce_adjustments(
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
                    payments_total_adjustments.append(amount)
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

        logger.debug('consistency check taxes...')
        tax = self.locale.parse_amount(
            self.get_field_in_table(payment_table, self.locale.estimated_tax))

        expected_tax = self.reduce_amounts(
            a.amount for shipment in shipments for a in shipment.tax)
        if expected_tax is None:
            # tax not given on shipment level
            if not self.locale.tax_included_in_price:
                # add tax if not already included in item prices
                shipments_total_adjustments.append(tax)
        elif expected_tax != tax:
            errors.append(
                'expected tax is %s, but parsed value is %s' % (expected_tax, tax))

        if self.locale.tax_included_in_price:
            # tax is already inlcuded in item prices
            # do not add additional transaction for taxes
            tax = None

        logger.debug('consistency check grand total...')
        payments_total_adjustment = self.reduce_amounts(payments_total_adjustments)
        shipments_total_adjustment = self.reduce_amounts(shipments_total_adjustments)

        expected_total = self.add_amount(shipments_total_adjustment,
                                    self.reduce_amounts(x.total for x in shipments))

        # if no shipments pre-tax section, then the expected total isn't accounting
        # for the pre-tax adjustments yet since they are only in the grand total section
        if shipments_pretax_amount is None:
            expected_total = self.add_amount(expected_total, pretax_amount)

        adjusted_grand_total = self.add_amount(payments_total_adjustment, grand_total)
        if expected_total != adjusted_grand_total:
            errors.append('expected grand total is %s, but parsed value is %s' %
                        (expected_total, adjusted_grand_total))

        logger.debug('parsing order placed date...')
        def is_order_placed_node(node):
            m = re.fullmatch(self.locale.order_placed_regex, node.text.strip())
            return m is not None
        
        node = soup.find(is_order_placed_node)
        m = re.fullmatch(self.locale.order_placed_regex, node.text.strip())
        assert m is not None
        order_date = self.locale.parse_date(m.group(1))

        logger.debug('parsing credit card transactions...')
        credit_card_transactions = self.parse_credit_card_transactions(soup)
        if not credit_card_transactions:
            logger.debug('no credit card transactions table given, falling back to payments table')
            credit_card_transactions = self.parse_credit_card_transactions_from_payments_table(
                payment_table, order_date)

        if credit_card_transactions:
            total_payments = self.reduce_amounts(
                x.amount for x in credit_card_transactions)
        else:
            logger.info('no payment transactions found, assumig grand total as total payment amount')
            total_payments = grand_total
        if total_payments != adjusted_grand_total:
            errors.append('total payment amount is %s, but grand total is %s' %
                        (total_payments, adjusted_grand_total))

        logger.debug('parsing order ID...')
        title = soup.find('title').text.strip()
        m = re.fullmatch(self.locale.order_id_regular, title.strip())
        assert m is not None

        logger.debug('...finished parsing invoice.')

        return Order(
            order_date=order_date,
            order_id=m.group(1),
            shipments=shipments,
            credit_card_transactions=credit_card_transactions,
            tax=tax,
            errors=sum((shipment.errors
                        for shipment in shipments), cast(Errors, [])) + errors,
            **output_fields)

    @staticmethod
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


    def parse_digital_order_invoice(self, path: str) -> Optional[Order]:
        errors = []
        with open(path, 'rb') as f:
            soup = bs4.BeautifulSoup(f.read(), 'lxml')

        logger.debug('check if order has been cancelled...')
        def is_cancelled_order(node):
            return node.text.strip() == self.locale.order_cancelled

        if soup.find(is_cancelled_order):
            return None

        logger.debug('parsing header...')
        def is_digital_order_row(node):
            if node.name != 'tr':
                return False
            m = re.match(self.locale.digital_order, node.text.strip())
            if m is None:
                return False
            try:
                self.locale.parse_date(m.group(1))
                return True
            except:
                return False

        # Find Digital Order row
        digital_order_header = soup.find(is_digital_order_row)
        digital_order_table = digital_order_header.find_parent('table')
        m = re.match(self.locale.digital_order, digital_order_header.text.strip())
        assert m is not None
        order_date = self.locale.parse_date(m.group(1))

        logger.debug('parsing items...')
        def is_items_ordered_header(node):
            if node.name != 'tr':
                return False
            tds = node('td')
            if len(tds) < 2:
                return False
            return (tds[0].text.strip() == self.locale.items_ordered and
                    tds[1].text.strip() == self.locale.price)

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

            text_lines = self.get_text_lines(description_node)

            def get_label_value(label):
                for line in text_lines:
                    m = re.match(r'^\s*' + label + ': (.*)$', line,
                                re.UNICODE | re.DOTALL)
                    if m is None:
                        continue
                    return m.group(1)

            by = get_label_value(self.locale.by)
            sold_by = get_label_value(self.locale.sold_by)

            items.append(
                DigitalItem(
                    description=description,
                    by=by,
                    sold_by=sold_by,
                    url=url,
                    price=self.locale.parse_amount(price),
                ))

        other_fields_text_lines = self.get_text_lines(other_fields_td)

        logger.debug('parsing amounts...')
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
                    Adjustment(amount=self.locale.parse_amount(amount_str), description=label))
            return adjustments

        def get_amounts_in_text(pattern_map):
            amounts = dict()
            for key, label in pattern_map.items():
                amount = self.locale.parse_amount(get_other_field(label))
                amounts[key] = amount
            return amounts
        
        items_subtotal = self.locale.parse_amount(
            get_other_field(self.locale.items_subtotal_regex))
        total_before_tax = self.locale.parse_amount(
            get_other_field(self.locale.total_before_tax_regex))
        tax = get_adjustments(self.locale.tax_collected_digital)
        total_for_this_order = self.locale.parse_amount(
            get_other_field(self.locale.total_order_digital))
        
        logger.debug('parsing pretax adjustments...')
        output_fields = dict()
        output_fields['pretax_adjustments'] = get_adjustments(
            self.locale.pretax_adjustment_fields_pattern)
        pretax_parts = ([items_subtotal] +
                        [a.amount for a in output_fields['pretax_adjustments']])
        logger.debug(pretax_parts)
        logger.debug(total_before_tax)
        expected_total_before_tax = self.reduce_amounts(pretax_parts)
        if expected_total_before_tax != total_before_tax:
            errors.append('expected total before tax is %s, but parsed value is %s'
                        % (expected_total_before_tax, total_before_tax))
        
        logger.debug('parsing posttax adjustments...')
        output_fields['posttax_adjustments'] = get_adjustments(
            self.locale.posttax_adjustment_fields_pattern)
        posttax_parts = ([total_before_tax] + [a.amount for a in tax] +
                        [a.amount for a in output_fields['posttax_adjustments']])
        expected_total = self.reduce_amounts(posttax_parts)
        
        logger.debug(total_for_this_order)
        if expected_total != total_for_this_order:
            errors.append('expected total is %s, but parsed value is %s' %
                        (expected_total, total_for_this_order))

        if self.locale.tax_included_in_price:
            tax = []

        shipment = Shipment(
            shipped_date=order_date,
            items=items,
            items_subtotal=items_subtotal,
            total_before_tax=total_before_tax,
            tax=tax,
            total=total_for_this_order,
            errors=errors,
            **output_fields)

        order_id_td = soup.find(
            lambda node: node.name == 'td' and
            re.match(self.locale.order_id_digital, node.text.strip())
            )
        m = re.match(self.locale.order_id_digital, order_id_td.text.strip())
        assert m is not None
        order_id = m.group(1)

        logger.debug('parsing payment information...')
        payment_table = soup.find(
            lambda node: node.name == 'table' and
            node.text.strip().startswith(self.locale.payment_information_digital)
            )
        credit_card_transactions = self.parse_credit_card_transactions_from_payments_table(
            payment_table, order_date)

        logger.debug('...finished')

        return Order(
            order_date=order_date,
            order_id=order_id,
            shipments=[shipment],
            credit_card_transactions=credit_card_transactions,
            pretax_adjustments=[],
            posttax_adjustments=output_fields['posttax_adjustments'],
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
    # ToDo: add locale argument
    # ap.add_argument(
    #     '--locale', default='EN', help='Local Amazon settings, defaults to EN')
    ap.add_argument('paths', nargs='*')
    

    args = ap.parse_args()
    amz_inv = AmazonInvoice()
    results = []
    for path in args.paths:
        try:
            result = amz_inv.parse_invoice(path)
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
