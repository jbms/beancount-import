import csv
import os
from pathlib import Path

import pytest

from ..journal_editor import JournalEditor
from . import SourceResults, venmo
from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'venmo'))

examples = [
    'test_basic',
    'test_matching',
    'test_invalid_references',
]


@pytest.mark.parametrize('name', examples)
def test_source(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.venmo',
            'directory': testdata_dir,
            'assets_account': 'Assets:Venmo',
        },
        replacements=[(testdata_dir, '<testdata>')])



@pytest.mark.parametrize('blank_amount', ['', '   '])
def test_blank_transaction_amount_skips_only_that_transaction(
        tmp_path: Path, blank_amount: str):
    csv_path = tmp_path / 'transactions.csv'
    with csv_path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=venmo.transaction_field_names)
        writer.writeheader()
        writer.writerow({
            venmo.CSV_ID_KEY: 'blank',
            venmo.CSV_TYPE_KEY: 'Payment',
            venmo.CSV_DATETIME_KEY: '2026-01-01',
            venmo.CSV_AMOUNT_TOTAL_KEY: blank_amount,
        })
        writer.writerow({
            venmo.CSV_ID_KEY: 'valid',
            venmo.CSV_TYPE_KEY: 'Payment',
            venmo.CSV_DATETIME_KEY: '2026-01-02',
            venmo.CSV_FUNDING_SOURCE_KEY: 'Venmo balance',
            venmo.CSV_AMOUNT_TOTAL_KEY: '-$5.00',
        })
    journal_path = tmp_path / 'journal.beancount'
    journal_path.touch()
    source = venmo.load({
        'directory': str(tmp_path),
        'assets_account': 'Assets:Venmo',
    }, lambda _: None)
    results = SourceResults()

    source.prepare(JournalEditor(str(journal_path)), results)

    assert len(results.pending) == 1
    posting = results.pending[0].entries[0].postings[0]
    assert posting.meta[venmo.VENMO_PAYMENT_KEY] == 'valid'
    assert results.messages == [
        ('error', 'Venmo transaction blank has no total amount.',
         {'filename': str(csv_path), 'lineno': 1}),
    ]
