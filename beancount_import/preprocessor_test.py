import datetime
import gc
import os
import re
from types import SimpleNamespace

import py
import pytest
from beancount.core.data import Transaction

from . import journal_editor, matching, reconcile, reconcile_test, test_util, training
from .preprocessor import BasePreprocessor, TransactionRuleMixin, PreprocessingRule
from .source import ImportResult, Source

testdata_root = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "testdata")
)

venmo_dir = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "testdata", "source", "venmo")
)

rules = [
    # Rule 1: Match tutoring transactions by description
    PreprocessingRule(
        source_name="venmo",
        transaction_narration="Tutoring",
        posting_meta={"venmo_description": "Tutoring"},
        target_account="Expenses:Tutoring",
        action="record",
    ),
    # Rule 2: Match rent payments by amount and description
    PreprocessingRule(
        transaction_narration="Rent",
        transaction_flag="*",
        posting_account=re.compile(r"Assets:Venmo"),
        posting_amount=re.compile(r"1[0-9]{3}\.[0-9]{2} USD"),  # Matches 1xxx.xx USD
        posting_meta={"venmo_description": "Rent"},
        target_account="Income:Rent",
        action="record",
    ),
    # Rule 3: Ignore small food-related transactions
    PreprocessingRule(
        transaction_narration=re.compile(r"^(?!Transfer$).*$"),
        posting_meta={
            "venmo_description": re.compile(r"(Bagels|Local Eats|Distant Treats)")
        },
        posting_amount=re.compile(
            r"-?\d{1,2}\.\d{2} USD"
        ),  # Matches small dollar amounts
        action="ignore",
    ),
    # Rule 4: Modify utility payments
    PreprocessingRule(
        transaction_narration=re.compile(r"^(?!Transfer$).*$"),
        posting_meta={"venmo_description": "Utilities", "venmo_type": "Charge"},
        target_account="Expenses:Utilities",
        action="modify",
    ),
    # Rule 5: Handle security deposits
    PreprocessingRule(
        transaction_narration=re.compile(r"^(?!Transfer$).*$"),
        posting_meta={"venmo_description": "Security deposit"},
        target_account="Liabilities:Security-Deposits",
        action="record",
    ),
    # Rule 6: Match standard transfers to checking
    PreprocessingRule(
        posting_meta={
            "venmo_type": "Standard Transfer",
            "venmo_account_description": re.compile(r".*\d{4}"),
        },
        target_account="Assets:Checking",
        action="record",
    ),
]


class RuleBasedPreprocessor(TransactionRuleMixin, BasePreprocessor):
    def preprocess(self) -> None:
        """Apply the preprocessing rules then call parent's preprocess."""
        self.apply_rules(rules)


def test_rule_preprocessing(tmpdir: py.path.local):
    tester = reconcile_test.ReconcileGoldenTester(
        golden_directory=os.path.join(
            testdata_root, "preprocessor", "test_preprocessor"
        ),
        temp_dir=str(tmpdir),
        options=dict(
            # Keep snapshots independent of decision-tree tie breaking.
            classifier='nearest_neighbor',
            preprocessor=RuleBasedPreprocessor,
            data_sources=[
                {
                    "module": "beancount_import.source.venmo",
                    "directory": venmo_dir,
                    "assets_account": "Assets:Venmo",
                },
            ],
        ),
    )
    tester.snapshot()


# State tests use a real journal editor and posting index, with invented entries.

class SyntheticSource(Source):
    name = 'synthetic'


def pending_entries(text):
    entries = test_util.parse(text)
    return reconcile.make_pending_entry(
        ImportResult(date=entries[0].date, entries=entries, info=None),
        SyntheticSource(log_status=lambda message: None))


@pytest.fixture
def processor(tmp_path):
    (tmp_path / 'journal.beancount').write_text('''
include "other.beancount"
2019-01-01 open Assets:Checking USD
2019-01-01 open Expenses:FIXME USD
2019-01-01 open Expenses:Groceries USD
''')
    (tmp_path / 'other.beancount').write_text('')
    (tmp_path / 'ignore.beancount').write_text('')
    loaded = reconcile.LoadedReconciler.__new__(reconcile.LoadedReconciler)
    loaded.editor = journal_editor.JournalEditor(
        str(tmp_path / 'journal.beancount'), str(tmp_path / 'ignore.beancount'))
    loaded.reconciler = SimpleNamespace(
        ignore_path=str(tmp_path / 'ignore.beancount'),
        entry_file_selector=lambda entry: str(tmp_path / 'journal.beancount'))
    loaded.posting_db = matching.PostingDatabase(
        fuzzy_match_days=0, fuzzy_match_amount=0,
        is_cleared=lambda posting: False, metadata_keys=frozenset())
    loaded.pending_data = []
    loaded.pending_transaction_ids = set()
    loaded._pending_entry_originals = {}
    loaded.uncleared_postings = []
    loaded.cleared_dates = {}
    loaded.account_source_map = {}
    return BasePreprocessor(loaded)


def add_pending(processor, entry):
    processor.loaded_reconciler.pending_data.append(entry)
    for transaction in entry.entries:
        if isinstance(transaction, Transaction):
            processor.loaded_reconciler.pending_transaction_ids.add(id(transaction))
            processor.loaded_reconciler.posting_db.add_transaction(transaction)


def index_ids(processor):
    return {id(posting.entry) for date in (datetime.date(2020, 1, 1),
                                         datetime.date(2020, 1, 2))
            for posting in processor.posting_db.get_date_currency_postings(
                (date, 'USD'))}


def synthetic_pending():
    return pending_entries('''
2020-01-01 * "First"
  Assets:Checking -10 USD
  Expenses:FIXME 10 USD
''')


def test_pending_data_tracks_reconciler_replacement(processor):
    entry = synthetic_pending()
    add_pending(processor, entry)
    processor.loaded_reconciler.pending_data = []
    assert processor.pending_data is processor.loaded_reconciler.pending_data


def test_record_multiple_directives_once_and_index_persisted_entries(processor, tmp_path):
    entry = pending_entries('''
2020-01-01 * "First"
  Assets:Checking -10 USD
  Expenses:Groceries 10 USD
2020-01-02 * "Second"
  Assets:Checking -20 USD
  Expenses:Groceries 20 USD
''')
    add_pending(processor, entry)
    processor.loaded_reconciler.reconciler.entry_file_selector = lambda e: str(
        tmp_path / ('other.beancount' if getattr(e, 'narration', '') == 'Second'
                    else 'journal.beancount'))
    processor.record_pending_entry(entry, entry)
    persisted = [e for e in processor.editor.entries if isinstance(e, Transaction)]
    assert len(persisted) == 2
    assert processor.pending_data == []
    assert processor.pending_transaction_ids == set()
    assert index_ids(processor) == {id(e) for e in persisted}
    reloaded = journal_editor.JournalEditor(processor.editor.journal_path)
    assert len([e for e in reloaded.entries if isinstance(e, Transaction)]) == 2


def test_record_opens_missing_accounts(processor):
    entry = pending_entries('''
2020-01-01 * "First"
  Assets:Checking -10 USD
  Expenses:New 10 USD
''')
    add_pending(processor, entry)
    processor.record_pending_entry(entry, entry)
    assert 'Expenses:New' in processor.editor.accounts


@pytest.mark.parametrize('operation', ['record', 'ignore'])
def test_failed_write_preserves_pending_state(processor, monkeypatch, operation):
    entry = synthetic_pending()
    add_pending(processor, entry)
    before_ids = index_ids(processor)

    def fail(stage):
        raise OSError('synthetic write failure')

    monkeypatch.setattr(journal_editor.StagedChanges, 'apply', fail)
    with pytest.raises(OSError, match='synthetic write failure'):
        if operation == 'record':
            processor.record_pending_entry(entry, entry)
        else:
            processor.ignore_pending_entry(entry)
    assert processor.pending_data == [entry]
    assert processor.pending_transaction_ids == {id(entry.entries[0])}
    assert index_ids(processor) == before_ids


def test_ignore_removes_pending_index_and_persists_once(processor):
    entry = synthetic_pending()
    add_pending(processor, entry)
    processor.ignore_pending_entry(entry)
    assert processor.pending_data == []
    assert processor.pending_transaction_ids == set()
    assert index_ids(processor) == set()
    assert len([e for e in processor.editor.ignored_entries
                if isinstance(e, Transaction)]) == 1


def test_modify_refreshes_pending_display_and_index(processor):
    entry = synthetic_pending()
    add_pending(processor, entry)
    transaction = entry.entries[0]
    changed = transaction._replace(postings=[
        transaction.postings[0],
        transaction.postings[1]._replace(account='Expenses:Groceries')])
    processor.modify_pending_entry(entry, entry._replace(entries=[changed]))
    modified, = processor.pending_data
    assert modified.id != entry.id
    assert 'Expenses:Groceries' in modified.formatted
    assert processor.pending_transaction_ids == {id(changed)}
    assert index_ids(processor) == {id(changed)}


@pytest.mark.parametrize('reverse', [False, True])
def test_rules_are_posting_order_independent(processor, reverse):
    class Rules(TransactionRuleMixin, BasePreprocessor):
        pass

    entry = synthetic_pending()
    if reverse:
        transaction = entry.entries[0]
        entry = entry._replace(entries=[transaction._replace(
            postings=list(reversed(transaction.postings)))])
    add_pending(processor, entry)
    rules = Rules(processor.loaded_reconciler)
    rules.apply_rules([PreprocessingRule(target_account='Expenses:Groceries',
                                        action='modify')])
    accounts = [p.account for p in rules.pending_data[0].entries[0].postings]
    expected = ['Assets:Checking', 'Expenses:Groceries']
    assert accounts == (list(reversed(expected)) if reverse else expected)


@pytest.mark.parametrize('postings', ['', '  Assets:Checking -10 USD\n',
    '  Assets:Checking -10 USD\n  Expenses:FIXME 5 USD\n  Expenses:FIXME 5 USD\n'])
def test_rules_skip_non_simple_transactions(processor, postings):
    class Rules(TransactionRuleMixin, BasePreprocessor):
        pass

    entry = synthetic_pending()
    transaction = entry.entries[0]._replace(postings=[])
    if postings:
        transaction, = test_util.parse('2020-01-01 * "First"\n' + postings)
    entry = entry._replace(entries=[transaction])
    assert not Rules(processor.loaded_reconciler).is_simple_pending_entry(entry)


def test_pending_data_assignment_updates_reconciler(processor):
    entry = synthetic_pending()
    processor.pending_data = [entry]
    assert processor.loaded_reconciler.pending_data == [entry]


def test_invalid_action_is_rejected():
    with pytest.raises(ValueError, match='action'):
        PreprocessingRule(action='recrod')


@pytest.mark.parametrize('operation', ['record', 'ignore', 'modify'])
def test_journal_backed_pending_entry(processor, operation):
    raw = synthetic_pending()
    stage = processor.editor.stage_changes()
    stage.add_entry(raw.entries[0], processor.editor.journal_path)
    original, = stage.apply().new_entries
    entry = reconcile.make_pending_entry(
        ImportResult(date=original.date, entries=[original], info=None), None)
    processor.pending_data.append(entry)
    processor.posting_db.add_transaction(original)
    changed = original._replace(postings=[original.postings[0],
        original.postings[1]._replace(account='Expenses:Groceries')])
    new_entry = entry._replace(entries=[changed])
    if operation == 'record':
        processor.record_pending_entry(entry, new_entry)
        persisted = [e for e in processor.editor.entries if isinstance(e, Transaction)]
        assert len(persisted) == 1
        assert persisted[0].postings[1].account == 'Expenses:Groceries'
        assert index_ids(processor) == {id(persisted[0])}
    elif operation == 'ignore':
        processor.ignore_pending_entry(entry)
        assert not any(isinstance(e, Transaction) for e in processor.editor.entries)
        assert len(processor.editor.ignored_entries) == 1
        assert index_ids(processor) == set()
    else:
        loaded = processor.loaded_reconciler
        loaded.classifier = None
        loaded._use_nearest_neighbor = False
        loaded.sources = []
        loaded.training_examples = training.TrainingExamples()
        loaded._feature_extractor = training.FeatureExtractor(
            account_source_map={}, sources=[], skip_accounts=set())
        processor.modify_pending_entry(entry, new_entry)
        assert index_ids(processor) == {id(changed)}
        candidates = loaded._make_candidates_from_import_result(processor.pending_data[0])
        candidate = next(c for c in candidates.candidates
                         if any(t is original for t in c.used_transactions))
        loaded.accept_candidate(candidate)
        persisted = [e for e in processor.editor.entries if isinstance(e, Transaction)]
        assert len(persisted) == 1
        assert persisted[0].postings[1].account == 'Expenses:Groceries'
    assert processor.pending_data == []


@pytest.mark.parametrize('classifier', reconcile.CLASSIFIERS)
def test_callback_actions_survive_reload(tmp_path, classifier):
    journal = tmp_path / 'journal.beancount'
    ignored = tmp_path / 'ignore.beancount'
    csv_path = tmp_path / 'transactions.csv'
    journal.write_text('''
2019-01-01 open Assets:Checking USD
  mint_id: "Synthetic Checking"
''')
    ignored.write_text('')
    csv_path.write_text(
        'Date,Description,Original Description,Amount,Transaction Type,Category,Account Name,Labels,Notes\n'
        '01/01/2020,Rule Record,RULE RECORD,10.00,debit,,Synthetic Checking,,\n'
        '01/02/2020,Rule Ignore,RULE IGNORE,20.00,debit,,Synthetic Checking,,\n'
        '01/03/2020,Rule Modify,RULE MODIFY,30.00,debit,,Synthetic Checking,,\n'
        '01/04/2020,Unmatched,UNMATCHED,40.00,debit,,Synthetic Checking,,\n')

    class Rules(TransactionRuleMixin, BasePreprocessor):
        def preprocess(self):
            self.apply_rules([
                PreprocessingRule(posting_meta={'source_desc': 'RULE RECORD'},
                                  target_account='Expenses:Groceries'),
                PreprocessingRule(posting_meta={'source_desc': 'RULE IGNORE'},
                                  action='ignore'),
                PreprocessingRule(posting_meta={'source_desc': 'RULE MODIFY'},
                                  target_account='Expenses:Groceries', action='modify'),
            ])

    options = dict(
        default_output=str(journal), transaction_output_map=[],
        open_account_output_map=[], balance_account_output_map=[], price_output=None,
        fuzzy_match_days=5, fuzzy_match_amount=0, account_pattern=None,
        ignore_account_for_classification_pattern=training.DEFAULT_IGNORE_ACCOUNT_FOR_CLASSIFICATION_PATTERN, classifier_cache=None,
        classifier=classifier, preprocessor=Rules,
        data_sources=[dict(module='beancount_import.source.mint', filename=str(csv_path))])
    reconciler = reconcile.Reconciler(str(journal), lambda message: None,
                                     str(ignored), options)
    try:
        for iteration in range(2):
            loaded = reconciler.loaded_future.result(timeout=30)
            assert len(loaded.pending_data) == 2
            assert len([e for e in loaded.editor.entries if isinstance(e, Transaction)]) == 1
            assert len([e for e in loaded.editor.ignored_entries
                        if isinstance(e, Transaction)]) == 1
            modified = next(e for e in loaded.pending_data
                            if 'RULE MODIFY' in e.formatted)
            assert 'Expenses:Groceries' in modified.formatted
            assert len(loaded.training_examples.training_examples) > 0
            if iteration == 0:
                reconciler.reload_journal()
    finally:
        gc.unfreeze()


def test_find_pending_entries_can_be_consumed_while_recording(processor):
    first = synthetic_pending()
    second = pending_entries('''
2020-01-02 * "Second"
  Assets:Checking -20 USD
  Expenses:Groceries 20 USD
''')
    add_pending(processor, first)
    add_pending(processor, second)
    for entry in processor.find_pending_entries(lambda entry: True):
        processor.record_pending_entry(entry, entry)
    assert processor.pending_data == []


@pytest.fixture
def mint_journal_pending(tmp_path):
    journal = tmp_path / 'journal.beancount'
    ignored = tmp_path / 'ignore.beancount'
    csv_path = tmp_path / 'transactions.csv'
    journal.write_text('''
2019-01-01 open Assets:Checking USD
  mint_id: "Synthetic Checking"
2019-01-01 open Expenses:FIXME USD
2020-01-01 * "RULE MODIFY"
  Assets:Checking -10 USD
    date: 2020-01-01
    source_desc: "RULE MODIFY"
  Expenses:FIXME 10 USD
''')
    ignored.write_text('')
    csv_path.write_text(
        'Date,Description,Original Description,Amount,Transaction Type,Category,Account Name,Labels,Notes\n'
        '01/01/2020,Rule Modify,RULE MODIFY,10.00,debit,,Synthetic Checking,,\n')
    options = dict(
        default_output=str(journal), transaction_output_map=[],
        open_account_output_map=[], balance_account_output_map=[], price_output=None,
        fuzzy_match_days=5, fuzzy_match_amount=0, account_pattern=None,
        ignore_account_for_classification_pattern=training.DEFAULT_IGNORE_ACCOUNT_FOR_CLASSIFICATION_PATTERN,
        classifier_cache=None,
        data_sources=[dict(module='beancount_import.source.mint', filename=str(csv_path))])
    reconciler = reconcile.Reconciler(str(journal), lambda message: None,
                                     str(ignored), options)
    try:
        loaded = reconciler.loaded_future.result(timeout=30)
        assert len(loaded.pending_data) == 1
        original = loaded.pending_data[0].entries[0]
        assert loaded.is_posting_cleared(original.postings[0])
        yield reconciler, BasePreprocessor(loaded), original
    finally:
        gc.unfreeze()


def modify_account(processor, account):
    entry, = processor.pending_data
    transaction = entry.entries[0]
    changed = transaction._replace(postings=[transaction.postings[0],
        transaction.postings[1]._replace(account=account)])
    processor.modify_pending_entry(entry, entry._replace(entries=[changed]))


@pytest.mark.parametrize('operation', ['accept', 'accept_ignored', 'record', 'ignore'])
def test_modified_cleared_journal_entry_keeps_provenance(mint_journal_pending, operation):
    reconciler, processor, original = mint_journal_pending
    modify_account(processor, 'Expenses:Groceries')
    # A second modification must still refer to the first journal original.
    modify_account(processor, 'Expenses:Household')
    if operation.startswith('accept'):
        loaded = processor.loaded_reconciler
        candidates, _, _ = loaded.get_next_candidates()
        for candidate in candidates.candidates:
            assert any(t is original for t in candidate.used_transactions)
        loaded.accept_candidate(candidates.candidates[0], ignore=operation == 'accept_ignored')
    elif operation == 'record':
        entry, = processor.pending_data
        processor.record_pending_entry(entry, entry)
    else:
        processor.ignore_pending_entry(processor.pending_data[0])
    assert processor.pending_data == []
    assert processor.pending_transaction_ids == set()
    assert id(original) not in index_ids(processor)
    reconciler.reload_journal()
    reloaded = reconciler.loaded_future.result(timeout=30)
    assert reloaded.pending_data == []
    assert reloaded.invalid_references == []
    ignored = operation in ('ignore', 'accept_ignored')
    journal_transactions = [e for e in reloaded.editor.entries if isinstance(e, Transaction)]
    ignored_transactions = [e for e in reloaded.editor.ignored_entries if isinstance(e, Transaction)]
    assert len(journal_transactions) == (0 if ignored else 1)
    assert len(ignored_transactions) == (1 if ignored else 0)
    persisted, = ignored_transactions if ignored else journal_transactions
    assert persisted.postings[1].account == 'Expenses:Household'
    assert persisted.postings[0].meta['source_desc'] == 'RULE MODIFY'
    assert persisted.postings[0].meta['date'] == original.postings[0].meta['date']


def test_noop_modify_keeps_journal_provenance(mint_journal_pending):
    reconciler, processor, original = mint_journal_pending
    entry, = processor.pending_data
    processor.modify_pending_entry(entry, entry)
    loaded = processor.loaded_reconciler
    candidates, _, _ = loaded.get_next_candidates()
    loaded.accept_candidate(candidates.candidates[0])
    persisted = [e for e in loaded.editor.entries if isinstance(e, Transaction)]
    assert len(persisted) == 1
    assert processor.pending_data == []
    assert processor.pending_transaction_ids == set()


@pytest.mark.parametrize('operation', ['record', 'ignore'])
def test_failed_write_retains_modified_journal_provenance(
        mint_journal_pending, monkeypatch, operation):
    reconciler, processor, original = mint_journal_pending
    modify_account(processor, 'Expenses:Groceries')
    entry, = processor.pending_data
    before_ids = index_ids(processor)

    def fail(stage):
        raise OSError('synthetic write failure')

    with monkeypatch.context() as patch:
        patch.setattr(journal_editor.StagedChanges, 'apply', fail)
        with pytest.raises(OSError, match='synthetic write failure'):
            if operation == 'record':
                processor.record_pending_entry(entry, entry)
            else:
                processor.ignore_pending_entry(entry)
    assert processor.pending_data == [entry]
    assert index_ids(processor) == before_ids
    # Retrying after the failed write must still consume the journal original.
    if operation == 'record':
        processor.record_pending_entry(entry, entry)
    else:
        processor.ignore_pending_entry(entry)
    reconciler.reload_journal()
    loaded = reconciler.loaded_future.result(timeout=30)
    assert loaded.pending_data == []
    assert loaded.invalid_references == []


def add_balance_to_modified_entry(processor):
    entry, = processor.pending_data
    balance, = test_util.parse('2020-01-02 balance Assets:Checking -10 USD')
    processor.modify_pending_entry(entry, entry._replace(entries=[*entry.entries, balance]))
    return processor.pending_data[0]


def test_accept_modified_journal_entry_with_multiple_directives(mint_journal_pending):
    reconciler, processor, original = mint_journal_pending
    modify_account(processor, 'Expenses:Groceries')
    add_balance_to_modified_entry(processor)
    loaded = processor.loaded_reconciler
    candidates, _, _ = loaded.get_next_candidates()
    loaded.accept_candidate(candidates.candidates[0])
    assert processor.pending_data == []
    assert processor.pending_transaction_ids == set()
    assert loaded._pending_entry_originals == {}
    reconciler.reload_journal()
    reloaded = reconciler.loaded_future.result(timeout=30)
    assert reloaded.pending_data == []
    assert reloaded.invalid_references == []
    assert len([e for e in reloaded.editor.entries if isinstance(e, Transaction)]) == 1


def test_matching_cannot_consume_part_of_modified_journal_entry(mint_journal_pending):
    reconciler, processor, original = mint_journal_pending
    modify_account(processor, 'Expenses:Groceries')
    grouped = add_balance_to_modified_entry(processor)
    imported = synthetic_pending()
    add_pending(processor, imported)
    candidates = processor.loaded_reconciler._make_candidates_from_import_result(imported)
    assert all(not any(t is grouped.entries[0] for t in candidate.used_transactions)
               for candidate in candidates.candidates)


def test_accept_cleans_consumed_transaction_from_middle_of_pending_group(processor):
    loaded = processor.loaded_reconciler
    loaded.classifier = None
    loaded._use_nearest_neighbor = False
    loaded.sources = []
    loaded.training_examples = training.TrainingExamples()
    loaded._feature_extractor = training.FeatureExtractor(
        account_source_map={}, sources=[], skip_accounts=set())
    grouped = pending_entries('''
2020-01-01 * "First"
  Assets:Checking -10 USD
  Expenses:FIXME 10 USD
2020-01-02 * "Second"
  Assets:Checking -20 USD
  Expenses:FIXME 20 USD
''')
    imported = pending_entries('''
2020-01-02 * "Second"
  Assets:Checking -20 USD
  Expenses:Groceries 20 USD
''')
    add_pending(processor, grouped)
    add_pending(processor, imported)
    second = grouped.entries[1]
    candidates = loaded._make_candidates_from_import_result(imported)
    matched = next(candidate for candidate in candidates.candidates
                   if any(entry is second for entry in candidate.used_transactions))
    loaded.accept_candidate(matched)
    assert id(second) not in processor.pending_transaction_ids
    assert id(second) not in index_ids(processor)
