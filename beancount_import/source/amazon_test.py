import os
from typing import Dict, Any

import pytest

from .amazon_invoice_test import testdata_dir
from .source_test import check_source_example
from ..source import SourceSpec

source_spec_without_posttax_accounts = {
    'module': 'beancount_import.source.amazon',
    'directory': testdata_dir,
    'amazon_account': 'name@domain.com',
}

source_spec_with_posttax_accounts = {
    **source_spec_without_posttax_accounts,
    'posttax_adjustment_accounts': {
        'Gift Card Amount': 'Assets:Gift-Cards:Amazon',
        'Rewards Points': 'Income:Amazon:Cashback',
    },
}  # type: Dict[str, Any]

examples = [
    ('test_basic', source_spec_with_posttax_accounts),
    ('test_credit_card_transactions', source_spec_with_posttax_accounts),
    ('test_cleared_and_invalid', source_spec_with_posttax_accounts),
    ('test_prediction', source_spec_without_posttax_accounts),
]


@pytest.mark.parametrize('name,source_spec', examples)
def test_source(name: str, source_spec: SourceSpec):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec=source_spec,
        replacements=[(testdata_dir, '<testdata>')])


@pytest.fixture
def synthetic_card_invoice():
    import datetime
    from decimal import Decimal
    from beancount.core.amount import Amount
    from . import amazon_invoice

    date = datetime.date(2020, 1, 2)
    total = Amount.from_string('12 USD')
    item = amazon_invoice.Item(
        quantity=Decimal(1), description='Example notebook',
        sold_by='Example Seller', condition='New', price=total)
    shipment = amazon_invoice.Shipment(
        shipped_date=date, items=[item], items_subtotal=total,
        pretax_adjustments=[], total_before_tax=total,
        posttax_adjustments=[], tax=[], total=total, errors=[])
    return amazon_invoice.Order(
        order_id='111-0000001-0000001', order_date=date, shipments=[shipment],
        credit_card_transactions=[
            amazon_invoice.CreditCardTransaction(
                date=date, card_description='Example Card', card_ending_in='0000',
                amount=Amount.from_string('5 USD')),
            amazon_invoice.CreditCardTransaction(
                date=date, card_description='Other Example Card', card_ending_in='0001',
                amount=Amount.from_string('7 USD')),
        ], pretax_adjustments=[], tax=Amount.from_string('0 USD'),
        posttax_adjustments=[], errors=[])


@pytest.mark.parametrize('account', [None, 'Liabilities:Payable:Amazon'])
def test_explicit_card_account_overrides_card_mapping(synthetic_card_invoice, account):
    from . import amazon

    transaction = amazon.make_amazon_transaction(
        synthetic_card_invoice, posttax_adjustment_accounts={},
        credit_card_accounts={'0000': 'Liabilities:Visa', '0001': 'Liabilities:Other'},
        amazon_account='example@example.com', credit_card_account=account)
    payments = [posting for posting in transaction.postings
                if amazon.CREDIT_CARD_DESCRIPTION_KEY in (posting.meta or {})]
    assert [p.account for p in payments] == (
        [account, account] if account else ['Liabilities:Visa', 'Liabilities:Other'])
    assert [p.units.number for p in payments] == [-5, -7]
    assert sum(p.units.number for p in transaction.postings) == 0


def test_configured_card_account_reaches_source_payments(
        synthetic_card_invoice, tmp_path, monkeypatch):
    from .. import journal_editor
    from . import amazon
    from . import SourceResults

    invoice_path = tmp_path / (synthetic_card_invoice.order_id + '.html')
    invoice_path.write_text('<html><body>Synthetic parser input</body></html>')
    monkeypatch.setattr(amazon, 'parse_invoice', lambda *args, **kwargs: synthetic_card_invoice)
    journal_path = tmp_path / 'journal.beancount'
    journal_path.write_text('''
2019-01-01 open Liabilities:Visa USD
  credit_card_last_4_digits: "0000"
''')
    journal = journal_editor.JournalEditor(str(journal_path))
    source = amazon.load(dict(directory=str(tmp_path), amazon_account='example@example.com',
                              credit_card_account='Liabilities:Payable:Amazon'),
                         log_status=lambda message: None)
    results = SourceResults()
    source.prepare(journal, results)
    assert results.messages == []
    imported, = results.pending
    transaction, = imported.entries
    payments = [p for p in transaction.postings
                if amazon.CREDIT_CARD_DESCRIPTION_KEY in (p.meta or {})]
    assert len(payments) == 2
    assert all(p.account == 'Liabilities:Payable:Amazon' for p in payments)


def test_explicit_card_account_does_not_redirect_unexplained_remainder(synthetic_card_invoice):
    from . import amazon

    invoice = synthetic_card_invoice._replace(credit_card_transactions=[])
    transaction = amazon.make_amazon_transaction(
        invoice, posttax_adjustment_accounts={}, credit_card_accounts={},
        amazon_account='example@example.com', credit_card_account='Liabilities:Payable:Amazon')
    assert transaction.postings[-1].account == amazon.FIXME_ACCOUNT
