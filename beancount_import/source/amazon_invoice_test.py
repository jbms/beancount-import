import collections
import json
import os

import pytest

from . import amazon_invoice

testdata_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'amazon'))


@pytest.mark.parametrize('name', [
    '277-5312419-9119541',
    '781-8429198-6057878',
    '166-7926740-5141621',
    'D56-5204779-4181560',
])
def test_parsing_en_US(name: str):
    source_path = os.path.join(testdata_dir, name + '.html')
    invoice = amazon_invoice.parse_invoice(source_path)
    json_path = os.path.join(testdata_dir, name + '.json')
    expected = json.load(
        open(json_path, 'r'), object_pairs_hook=collections.OrderedDict)
    expected_str = json.dumps(expected, indent=4)
    actual = amazon_invoice.to_json(invoice)
    actual_str = json.dumps(actual, indent=4)
    if expected_str != actual_str:
        print(actual_str)
    assert expected_str == actual_str


@pytest.mark.parametrize('name', [
    '256-0244967-2403944', # regular order
    '393-2608279-9292916', # Spar-Abo, payed with gift card 
    '898-5185906-0096901', # Spar-Abo
    '974-6135682-9358749', # several credit card transactions
    'D22-9220967-2566135', # digital order, audible subscription
    'D60-9825125-4795642', # digital order
    '399-5779972-5007935', # Direct Debit (Bankeinzug)
    '071-4816388-0694813', # gift card amazon
    '075-2225405-7594823', # gift card spotify
    '447-6209054-6766419', # charge up Amazon account
    '588-8509154-9761865', # preparing shipment 
    '142-4912939-2196263', # shipping soon
])
def test_parsing_de_DE(name: str):
    testdata_dir_locale = os.path.join(testdata_dir, 'de_DE')
    source_path = os.path.join(testdata_dir_locale, name + '.html')
    invoice = amazon_invoice.parse_invoice(source_path, locale=amazon_invoice.LOCALES['de_DE']())
    json_path = os.path.join(testdata_dir_locale, name + '.json')
    expected = json.load(
        open(json_path, 'r'), object_pairs_hook=collections.OrderedDict)
    expected_str = json.dumps(expected, indent=4)
    actual = amazon_invoice.to_json(invoice)
    actual_str = json.dumps(actual, indent=4)
    if expected_str != actual_str:
        print(actual_str)
    assert expected_str == actual_str


@pytest.fixture
def modern_invoice_html():
    with open(os.path.join(testdata_dir, 'modern', '111-0000001-0000001.html')) as f:
        return f.read()


def parse_modern_html(tmp_path, html):
    path = tmp_path / '111-0000001-0000001.html'
    path.write_text(html)
    return amazon_invoice.parse_invoice(str(path))


def test_modern_order_items_and_totals(tmp_path, modern_invoice_html):
    import datetime
    from decimal import Decimal
    from beancount.core.amount import Amount

    invoice = parse_modern_html(tmp_path, modern_invoice_html)
    assert invoice.order_id == '111-0000001-0000001'
    assert invoice.order_date == datetime.date(2025, 3, 15)
    shipment, = invoice.shipments
    item, = shipment.items
    assert item.quantity == Decimal(2)
    assert item.price == Amount.from_string('10.25 USD')
    assert item.sold_by == 'Amazon.com'
    assert shipment.shipped_date is None
    assert shipment.items_subtotal == Amount.from_string('20.50 USD')
    assert shipment.total == Amount.from_string('25.07 USD')
    assert invoice.tax == Amount.from_string('2.07 USD')
    # A payment method is not an explicit record of a charge.
    assert invoice.credit_card_transactions == []
    assert invoice.errors == []


def test_modern_multiple_items_adjustments_and_source_postings(tmp_path, modern_invoice_html):
    from bs4 import BeautifulSoup
    from beancount.core.amount import Amount
    from . import amazon

    soup = BeautifulSoup(modern_invoice_html, 'lxml')
    grid = soup.find(attrs={'data-component': 'purchasedItemsRightGrid'})
    second = BeautifulSoup(str(grid), 'lxml').find(attrs={'data-component': 'purchasedItemsRightGrid'})
    second.find(attrs={'data-component': 'itemTitle'}).string = 'Example pencils'
    second.find(attrs={'data-component': 'quantity'}).string = 'Qty: 3'
    second.find(attrs={'data-component': 'unitPrice'}).string = '$2.00'
    grid.parent.append(second)
    summary = soup.find(attrs={'data-component': 'orderSummary'})
    summary.append(BeautifulSoup('''
<div class="od-line-item-row">
 <div class="od-line-item-row-label">Exclusive Promotion Applied:</div>
 <div class="od-line-item-row-content">-$1.00</div>
</div>
<div class="od-line-item-row">
 <div class="od-line-item-row-label">Gift Card Amount:</div>
 <div class="od-line-item-row-content">-$4.00</div>
</div>''', 'lxml'))
    html = str(soup).replace('$20.50', '$26.50').replace('$23.00', '$28.00').replace('$25.07', '$26.07')
    invoice = parse_modern_html(tmp_path, html)
    assert invoice.errors == []
    assert [item.quantity for item in invoice.shipments[0].items] == [2, 3]
    transaction = amazon.make_amazon_transaction(
        invoice, posttax_adjustment_accounts={'Gift Card Amount': 'Assets:Gift-Cards:Amazon'},
        credit_card_accounts={}, amazon_account='example@example.com',
        credit_card_account='Liabilities:Payable:Amazon')
    assert [p.units for p in transaction.postings] == [
        Amount.from_string(value + ' USD')
        for value in ('20.50', '6.00', '2.50', '-1.00', '2.07', '-4.00', '-26.07')]
    assert transaction.postings[-2].account == 'Assets:Gift-Cards:Amazon'
    assert transaction.postings[-1].account == amazon.FIXME_ACCOUNT
    assert sum(p.units.number for p in transaction.postings) == 0


@pytest.mark.parametrize('old,new', [
    ('111-0000001-0000001', 'invalid'),
    ('March 15, 2025', 'March 15'),
    ('data-component="orderDate"', 'data-component="missingDate"'),
    ('data-component="itemTitle"', 'data-component="missingTitle"'),
    ('$10.25', 'not a price'),
    ('<span>2</span>', '<span>0</span>'),
    ('data-component="orderSummary"', 'data-component="missingSummary"'),
    ('Grand Total:', 'Missing Total:'),
])
def test_modern_invalid_required_fields_fail(tmp_path, modern_invoice_html, old, new):
    with pytest.raises((ValueError, ArithmeticError)):
        parse_modern_html(tmp_path, modern_invoice_html.replace(old, new))


def test_modern_inconsistent_totals_are_visible(tmp_path, modern_invoice_html):
    invoice = parse_modern_html(tmp_path, modern_invoice_html.replace('$25.07', '$99.00'))
    assert len(invoice.errors) == 1
    assert 'grand total' in invoice.errors[0]


def test_modern_explicit_card_transaction_is_preserved(tmp_path, modern_invoice_html):
    from beancount.core.amount import Amount

    table = '''<table><tr><td>Credit Card transactions</td><td><table>
<tr><td>Example Visa ending in 0000: March 17, 2025:</td><td>$25.07</td></tr>
</table></td></tr></table>'''
    invoice = parse_modern_html(tmp_path, modern_invoice_html.replace('</body>', table + '</body>'))
    payment, = invoice.credit_card_transactions
    assert payment.card_description == 'Example Visa'
    assert payment.card_ending_in == '0000'
    assert payment.date.day == 17
    assert payment.amount == Amount.from_string('25.07 USD')
    assert invoice.errors == []



def test_modern_order_source_prepare(tmp_path, modern_invoice_html):
    from ..journal_editor import JournalEditor
    from . import amazon, SourceResults

    path = tmp_path / '111-0000001-0000001.html'
    path.write_text(modern_invoice_html)
    journal_path = tmp_path / 'journal.beancount'
    journal_path.write_text('')
    source = amazon.AmazonSource(
        directory=str(tmp_path), amazon_account='example@example.com',
        log_status=lambda message: None)
    results = SourceResults()
    source.prepare(JournalEditor(str(journal_path)), results)
    assert results.messages == []
    imported, = results.pending
    transaction, = imported.entries
    assert transaction.meta[amazon.ORDER_ID_KEY] == '111-0000001-0000001'
    assert sum(p.units.number for p in transaction.postings) == 0
    assert str(transaction.postings[-1].units) == '-25.07 USD'



def test_modern_explicit_payment_requires_full_date(tmp_path, modern_invoice_html):
    table = '''<table><tr><td>Credit Card transactions</td><td><table>
<tr><td>Example Visa ending in 0000: March 17:</td><td>$25.07</td></tr>
</table></td></tr></table>'''
    with pytest.raises(ValueError, match='Invalid card transaction date'):
        parse_modern_html(tmp_path, modern_invoice_html.replace('</body>', table + '</body>'))
