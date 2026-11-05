import os
import datetime
from pathlib import Path
from typing import Optional, Tuple

import pytest

from .source_test import check_source_example, load_source
from . import Source, SourceResults
from .. import test_util
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


def test_import_preserves_source_description_and_date(tmp_path):
    entry, _ = import_transaction(tmp_path, '''2020-01-03 * "Groceries"
  Assets:Bank -10 USD
    source_desc: "CARD PAYMENT EXAMPLE SHOP 1234"
    date: 2020-01-02
    note: "Importer metadata"
  Expenses:Food 10 USD
''')
    meta = entry.postings[0].meta
    assert meta['source_desc'] == 'CARD PAYMENT EXAMPLE SHOP 1234'
    assert meta['date'] == datetime.date(2020, 1, 2)
    assert meta['note'] == 'Importer metadata'


def reload_import(tmp_path: Path, entry: Transaction
                  ) -> Tuple[SourceResults, Source]:
    journal_path = tmp_path / 'journal.beancount'
    journal_path.write_text('''2019-01-01 open Assets:Bank
2019-01-01 open Expenses:Food
2019-01-01 open Income:Salary
''' + test_util.format_entries([entry._replace(narration='Edited narration')]))
    editor = JournalEditor(str(journal_path))
    assert editor.errors == []
    source = load_source({
        'module': 'beancount_import.source.generic_importer_source',
        'directory': str(tmp_path / 'input'),
        'account': 'Assets:Bank',
        'importer': BeancountLedgerImporter(),
    })
    results = SourceResults()
    source.prepare(editor, results)
    return results, source


def test_import_preserved_metadata_identifies_accepted_transaction(tmp_path):
    entry, _ = import_transaction(tmp_path, '''2020-01-03 * "Groceries"
  Assets:Bank -10 USD
    source_desc: "CARD PAYMENT EXAMPLE SHOP 1234"
    date: 2020-01-02
  Expenses:Food 10 USD
''')
    results, _ = reload_import(tmp_path, entry)
    assert results.pending == []
    assert results.invalid_references == []


@pytest.mark.parametrize('none_meta', [False, True])
def test_import_annotates_only_all_source_account_postings(tmp_path, none_meta):
    class MetadataImporter(BeancountLedgerImporter):
        def extract(self, file, existing_entries):
            entries = super().extract(file, existing_entries)
            if none_meta:
                for entry in entries:
                    if isinstance(entry, Transaction):
                        entry.postings[:] = [
                            p._replace(meta=None) for p in entry.postings]
            return entries

    entry, _ = import_transaction(tmp_path, '''2020-01-03 * "Salary and bonus"
  Assets:Bank 3000 USD
  Assets:Bank 500 USD
  Income:Salary -3500 USD
''', importer=MetadataImporter())
    assert [(p.meta['source_desc'], p.meta['date'])
            for p in entry.postings[:2]] == [
                ('Salary and bonus', datetime.date(2020, 1, 3)),
                ('Salary and bonus', datetime.date(2020, 1, 3))]
    assert not (entry.postings[2].meta or {}).get('source_desc')
    assert not (entry.postings[2].meta or {}).get('date')
    _, source = reload_import(tmp_path, entry)
    assert [source.is_posting_cleared(p) for p in entry.postings] == [
        True, True, False]


@pytest.mark.parametrize('postings', ['''  Assets:Bank 3000 USD
  Assets:Bank 500 USD
  Income:Salary -3500 USD
''', '''  Assets:Bank 500 USD
  Assets:Bank 500 USD
  Income:Salary -1000 USD
''', '''  Assets:Bank 3000 USD
    source_desc: "EXAMPLE SALARY"
    date: 2020-01-02
  Assets:Bank 500 USD
    source_desc: "EXAMPLE BONUS"
    date: 2020-01-03
  Income:Salary -3500 USD
'''])
def test_import_split_source_postings_are_recognized_after_reload(tmp_path,
                                                                 postings):
    entry, _ = import_transaction(
        tmp_path, '2020-01-03 * "Salary and bonus"\n' + postings)
    results, _ = reload_import(tmp_path, entry)
    assert results.pending == []
    assert results.invalid_references == []


def test_import_overlapping_files_keep_distinct_split_records(tmp_path):
    entry, _ = import_transaction(tmp_path, '''2020-01-03 * "Salary and bonus"
  Assets:Bank 3000 USD
  Assets:Bank 500 USD
  Income:Salary -3500 USD
''')
    (tmp_path / 'input' / 'second.beancount').write_text('''2020-01-03 * "Salary and bonus"
  Assets:Bank 3000 USD
  Assets:Bank 600 USD
  Income:Salary -3600 USD
''')
    results, _ = reload_import(tmp_path, entry)
    pending, = results.pending
    transaction, = pending.entries
    assert [p.units for p in transaction.postings] == [
        Amount.from_string('3000 USD'), Amount.from_string('600 USD'),
        Amount.from_string('-3600 USD')]
    assert results.invalid_references == []


def test_import_overlapping_files_deduplicate_reordered_split_postings(tmp_path):
    entry, _ = import_transaction(tmp_path, '''2020-01-03 * "Salary and bonus"
  Assets:Bank 3000 USD
  Assets:Bank 500 USD
  Income:Salary -3500 USD
''')
    (tmp_path / 'input' / 'second.beancount').write_text('''2020-01-03 * "Salary and bonus"
  Assets:Bank 500 USD
  Assets:Bank 3000 USD
  Income:Salary -3500 USD
''')
    results, _ = reload_import(tmp_path, entry)
    assert results.pending == []
    assert results.invalid_references == []


def test_import_split_records_preserve_repeated_occurrences(tmp_path):
    text = '''2020-01-03 * "Two equal payments"
  Assets:Bank 500 USD
  Assets:Bank 500 USD
  Income:Salary -1000 USD
'''
    entry, _ = import_transaction(tmp_path, text)
    (tmp_path / 'input' / 'statement.beancount').write_text(text + '\n' + text)
    results, _ = reload_import(tmp_path, entry)
    assert len(results.pending) == 1
    assert results.invalid_references == []


def test_import_single_records_can_match_one_combined_journal_transaction(tmp_path):
    text = '''2020-01-03 * "First payment"
  Assets:Bank 3000 USD
    source_desc: "FIRST PAYMENT"
  Income:Salary -3000 USD
'''
    entry, _ = import_transaction(tmp_path, text)
    (tmp_path / 'input' / 'statement.beancount').write_text(text + '''
2020-01-03 * "Second payment"
  Assets:Bank 500 USD
    source_desc: "SECOND PAYMENT"
  Income:Salary -500 USD
''')
    second, _, _ = parse_file(str(tmp_path / 'input' / 'statement.beancount'))
    second_posting = second[1].postings[0]._replace(meta={
        'source_desc': 'SECOND PAYMENT', 'date': datetime.date(2020, 1, 3)})
    entry = entry._replace(postings=[entry.postings[0], second_posting,
        entry.postings[1]._replace(units=Amount.from_string('-3500 USD'))])
    results, _ = reload_import(tmp_path, entry)
    assert results.pending == []
    assert results.invalid_references == []


@pytest.mark.parametrize('inferred_posting', ['Assets:Bank', 'Assets:Bank USD'])
def test_import_inferred_source_leg_is_recognized_after_reload(tmp_path,
                                                              inferred_posting):
    entry, _ = import_transaction(tmp_path, '''2020-01-03 * "Transfer and fee"
  Assets:Bank -10 USD
  %s
  Expenses:Food 1 USD
''' % inferred_posting)
    units = entry.postings[1].units
    assert units is MISSING or units.number is MISSING
    results, _ = reload_import(tmp_path, entry)
    assert results.pending == []
    assert results.invalid_references == []


def test_import_ambiguous_source_amounts_remain_pending(tmp_path):
    entry, _ = import_transaction(tmp_path, '''2020-01-03 * "Unallocated transfer"
  Assets:Bank -10 USD
  Assets:Bank
  Assets:Bank
  Expenses:Food 1 USD
''')
    assert entry.postings[1].units is MISSING
    assert entry.postings[2].units is MISSING
