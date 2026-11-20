import json
import os
from pathlib import Path

import pytest

from ..journal_editor import JournalEditor
from . import SourceResults, paypal
from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source',
        'paypal'))

examples = [
    'test_basic',
    'test_matching',
]


@pytest.mark.parametrize('name', examples)
def test_source_en_US(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.paypal',
            'directory': testdata_dir,
            'assets_account': 'Assets:Paypal',
            'fee_account': 'Expenses:Financial:Paypal:Fees',
            'prefix': 'paypal',
        },
        replacements=[(testdata_dir, '<testdata>')])


# locale DE
examples = [
    'test_basic',
    # 'test_matching',
]


@pytest.mark.parametrize('name', examples)
def test_source_de_DE(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, 'de_DE', name),
        source_spec={
            'module': 'beancount_import.source.paypal',
            'directory': os.path.join(testdata_dir, 'de_DE'),
            'assets_account': 'Assets:Paypal',
            'fee_account': 'Expenses:Financial:Paypal:Fees',
            'prefix': 'paypal',
            'locale': 'de_DE'
        },
        replacements=[(testdata_dir, '<testdata>')])


@pytest.mark.parametrize('is_zero_fee', [True, False])
@pytest.mark.parametrize('blank_amount', ['', '   '])
def test_blank_fee_follows_zero_fee_flag(
        tmp_path: Path, is_zero_fee: bool, blank_amount: str):
    fixture = Path(testdata_dir) / '266LYBQVWC7P8PXV0.json'
    original = fixture.read_text(encoding='utf-8')
    (tmp_path / 'valid.json').write_text(original, encoding='utf-8')
    data = json.loads(original)
    data['amount']['feeAmount'] = blank_amount
    data['amount']['isZeroFee'] = is_zero_fee
    blank_path = tmp_path / 'blank.json'
    blank_path.write_text(json.dumps(data), encoding='utf-8')
    journal_path = tmp_path / 'journal.beancount'
    journal_path.touch()
    fee_account = 'Expenses:Financial:Paypal:Fees'
    source = paypal.load({
        'directory': str(tmp_path),
        'assets_account': 'Assets:Paypal',
        'fee_account': fee_account,
        'prefix': 'paypal',
    }, lambda _: None)
    results = SourceResults()

    source.prepare(JournalEditor(str(journal_path)), results)

    transactions = {next(iter(entry.links)): entry
                    for result in results.pending for entry in result.entries}
    if is_zero_fee:
        assert set(transactions) == {'paypal.blank', 'paypal.valid'}
        assert all(posting.account != fee_account
                   for posting in transactions['paypal.blank'].postings)
        assert results.messages == []
    else:
        assert set(transactions) == {'paypal.valid'}
        assert results.messages == [
            ('error', 'PayPal transaction blank has no amount for feeAmount.',
             {'filename': str(blank_path), 'lineno': 1}),
        ]
