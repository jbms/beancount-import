import os
from pathlib import Path
from typing import Optional, Tuple

import pytest

from .source_test import check_source_example, load_source
from . import SourceResults
from ..journal_editor import JournalEditor
from beancount.core.data import Transaction
from beancount.core.amount import Amount
from beancount.core.number import MISSING
from beancount.core.position import CostSpec
from beancount.parser import booking
from beangulp.importers.csv import Importer as CSVImporter, Col
from beangulp.importer import ImporterProtocol
from beancount.parser.parser import parse_file

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'generic_importer'))

testdata_csv = os.path.join(testdata_dir, "csv")

examples = [
    'test_basic',
    'test_invalid',
    'test_training_examples'
]

importer = CSVImporter({Col.DATE: 'Date',
                        Col.NARRATION1: 'Description',
                        Col.AMOUNT: 'Amount',
                        },
                       'Assets:Bank',
                       'USD',
                       '"Date","Description","Amount"',
                       )


@pytest.mark.parametrize('name', examples)
def test_source(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.generic_importer_source',
            'directory': testdata_csv,
            'account': 'Assets:Bank',
            'importer': importer,
        },
        replacements=[(testdata_dir, '<testdata>')])


# The existing testcases use CSVImporter, which cannot handle costs.
# This importer instead loads the transaction from a beancount file.
class BeancountLedgerImporter(ImporterProtocol):
    def extract(self, file, existing_entries):
        entries, errors, options_map = parse_file(file.name)
        return entries
    def identify(self, file):
        return True


def test_cost():
    example_dir = os.path.join(testdata_dir, "test_cost")
    check_source_example(
        example_dir=example_dir,
        source_spec={
            'module': 'beancount_import.source.generic_importer_source',
            'directory': os.path.join(example_dir, "input"),
            'account': 'Assets:Bank',
            'importer': BeancountLedgerImporter(),
        },
        replacements=[(testdata_dir, '<testdata>')])


def import_transaction(tmp_path: Path, text: str, journal_text: str = '',
                       importer: Optional[ImporterProtocol] = None
                       ) -> Tuple[Transaction, JournalEditor]:
    journal_path = tmp_path / 'journal.beancount'
    journal_path.write_text(journal_text)
    input_dir = tmp_path / 'input'
    input_dir.mkdir()
    (input_dir / 'statement.beancount').write_text(text)
    editor = JournalEditor(str(journal_path))
    assert editor.errors == []
    source = load_source({
        'module': 'beancount_import.source.generic_importer_source',
        'directory': str(input_dir),
        'account': 'Assets:Bank',
        'importer': importer if importer is not None else BeancountLedgerImporter(),
    })
    results = SourceResults()
    source.prepare(editor, results)
    assert not [message for message in results.messages if message[0] == 'error']
    result, = results.pending
    entry, = result.entries
    assert isinstance(entry, Transaction)
    return entry, editor


def test_import_rounding_within_inferred_tolerance(tmp_path):
    entry, _ = import_transaction(tmp_path, '''2020-01-01 * "Currency exchange"
  Assets:Bank -1.00 USD
  Assets:Savings 3.00 EUR @ 0.3333 USD
''')
    assert [posting.account for posting in entry.postings] == [
        'Assets:Bank', 'Assets:Savings']


@pytest.mark.parametrize('empty_posting', [
    'Assets:Receivable', 'Assets:Receivable USD'])
def test_import_empty_posting_is_balanced_by_booking(tmp_path, empty_posting):
    entry, editor = import_transaction(tmp_path, '''2020-01-01 * "Example loan"
  Assets:Bank -10 USD
  %s
    note: "Preserve this posting"
''' % empty_posting)
    assert [posting.account for posting in entry.postings] == [
        'Assets:Bank', 'Assets:Receivable']
    booked, errors = booking.book([entry], editor.options_map)
    assert errors == []
    assert booked[0].postings[1].units == Amount.from_string('10 USD')
    assert booked[0].postings[1].meta['note'] == 'Preserve this posting'


@pytest.mark.parametrize('currency', ['USD', '*'])
@pytest.mark.parametrize('price,expected_fixme', [
    ('0.333', []), ('0.332', [Amount.from_string('0.004 USD')])])
def test_import_respects_configured_tolerance(tmp_path, currency, price,
                                             expected_fixme):
    entry, _ = import_transaction(tmp_path, '''2020-01-01 * "Currency exchange"
  Assets:Bank -1 USD
  Assets:Savings 3 EUR @ %s USD
''' % price, 'option "inferred_tolerance_default" "%s:0.001"\n' % currency)
    assert [posting.units for posting in entry.postings
            if posting.account == 'Expenses:FIXME'] == expected_fixme


def test_import_balances_only_currencies_outside_tolerance(tmp_path):
    entry, _ = import_transaction(tmp_path, '''2020-01-01 * "Two currencies"
  Assets:Bank -10.00 USD
  Expenses:Food 9 USD
  Assets:Other -1.00 EUR
  Expenses:Food 0.9999 EUR
''')
    assert [posting.units for posting in entry.postings
            if posting.account == 'Expenses:FIXME'] == [
                Amount.from_string('1.00 USD')]


@pytest.mark.parametrize('valuation', ['{10 USD}', '{{10 USD}}', '@ 10 USD'])
def test_import_preserves_stock_weights_and_valuation(tmp_path, valuation):
    entry, _ = import_transaction(tmp_path, '''2020-01-01 * "Stock purchase"
  Assets:Bank -10 USD
  Assets:Broker 1 EXAMPLE %s
    note: "Keep valuation"
''' % valuation)
    assert [posting.account for posting in entry.postings] == [
        'Assets:Bank', 'Assets:Broker']
    assert entry.postings[1].units == Amount.from_string('1 EXAMPLE')
    assert entry.postings[1].meta['note'] == 'Keep valuation'
    if valuation.startswith('{'):
        assert isinstance(entry.postings[1].cost, CostSpec)
    else:
        assert entry.postings[1].price == Amount.from_string('10 USD')


@pytest.mark.parametrize('infer_from_cost,expected_fixme', [
    ('TRUE', []), ('FALSE', [Amount.from_string('0.001 USD')])])
def test_import_respects_cost_tolerance_option(tmp_path, infer_from_cost,
                                             expected_fixme):
    entry, _ = import_transaction(tmp_path, '''2020-01-01 * "Stock purchase"
  Assets:Bank -10 USD
  Assets:Broker 1.111 EXAMPLE {9 USD}
''', 'option "infer_tolerance_from_cost" "%s"\n' % infer_from_cost)
    assert [posting.units for posting in entry.postings
            if posting.account == 'Expenses:FIXME'] == expected_fixme


def test_import_does_not_guess_between_two_empty_postings(tmp_path):
    entry, editor = import_transaction(tmp_path, '''2020-01-01 * "Unallocated loan"
  Assets:Bank -10 USD
  Assets:Receivable
  Expenses:Food
''')
    assert [posting.account for posting in entry.postings] == [
        'Assets:Bank', 'Assets:Receivable', 'Expenses:Food']
    assert entry.postings[1].units is MISSING
    assert entry.postings[2].units is MISSING
    _, errors = booking.book([entry], editor.options_map)
    assert errors


def test_import_empty_posting_can_balance_multiple_currencies(tmp_path):
    entry, editor = import_transaction(tmp_path, '''2020-01-01 * "Two currencies"
  Assets:Bank -10 USD
  Assets:Other -5 EUR
  Expenses:Food
''')
    assert [posting.account for posting in entry.postings] == [
        'Assets:Bank', 'Assets:Other', 'Expenses:Food']
    booked, errors = booking.book([entry], editor.options_map)
    assert errors == []
    assert {posting.units for posting in booked[0].postings
            if posting.account == 'Expenses:Food'} == {
                Amount.from_string('10 USD'), Amount.from_string('5 EUR')}


def test_import_legacy_none_amount_is_balanced_by_booking(tmp_path):
    class NoneAmountImporter(BeancountLedgerImporter):
        def extract(self, file, existing_entries):
            entries = super().extract(file, existing_entries)
            for entry in entries:
                if isinstance(entry, Transaction):
                    entry.postings[:] = [
                        p._replace(units=None) if p.units is MISSING else p
                        for p in entry.postings]
            return entries

    entry, editor = import_transaction(tmp_path, '''2020-01-01 * "Example loan"
  Assets:Bank -10 USD
  Assets:Receivable
''', importer=NoneAmountImporter())
    assert [posting.account for posting in entry.postings] == [
        'Assets:Bank', 'Assets:Receivable']
    booked, errors = booking.book([entry], editor.options_map)
    assert errors == []
    assert booked[0].postings[1].units == Amount.from_string('10 USD')
