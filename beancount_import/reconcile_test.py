from typing import Dict, Any, Optional, Tuple, List, Union
import collections
import io
import os
import json
import shutil

import py
import pytest
from beancount.core.data import Directive, Open, Posting, Transaction
from beancount.core.number import MISSING

from . import journal_editor
from . import reconcile
from . import test_util
from . import training

testdata_root = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..', 'testdata'))

mint_data_path = os.path.realpath(
    os.path.join(testdata_root, 'source', 'mint', 'mint.csv'))


def _encode_pending_entries(pending_list: List[reconcile.PendingEntry]) -> str:
    out = io.StringIO()
    for pending in pending_list:
        out.write(';; source: %s\n' % (pending.source.name
                                       if pending.source else 'fixme', ))
        out.write(';; date: %s\n' % pending.date.strftime('%Y-%m-%d'))
        out.write(';; info: %s\n\n' % json.dumps(pending.info, sort_keys=True))
        for entry in pending.entries:
            out.write(test_util.format_entries([entry]).strip() + '\n\n')
    return out.getvalue().strip() + '\n'


def _encode_candidates(candidates: Optional[reconcile.Candidates]) -> str:
    parts = []
    if candidates is None:
        candidate_list = []  # type: List[reconcile.Candidate]
    else:
        candidate_list = candidates.candidates
    for candidate in candidate_list:
        textual_diff = candidate.staged_changes_with_unique_account_names.get_textual_diff(
        )
        for substitution in (candidate.substituted_accounts or []):
            textual_diff = textual_diff.replace(
                substitution.unique_name, '[%d]%s' %
                (substitution.group_number, substitution.account_name))
        parts.append(textual_diff)
    return '\n\n'.join(parts)


def _add_invalid_reference_and_uncleared_metadata(
        loaded_reconciler: reconcile.LoadedReconciler) -> Dict[str, str]:
    id_to_metadata = {}  # type: Dict[int, List[Tuple[str, Any]]]
    for i, (source, ref) in enumerate(loaded_reconciler.invalid_references):
        invalid_pair = ('invalid%d' % (i, ),
                        '%s: %d extra' % (source.name, ref.num_extras))
        for transaction, posting in ref.transaction_posting_pairs:
            if posting is None:
                id_to_metadata.setdefault(id(transaction),
                                          []).append(invalid_pair)
            else:
                id_to_metadata.setdefault(id(posting), []).append(invalid_pair)

    for transaction, posting in loaded_reconciler.uncleared_postings:
        id_to_metadata.setdefault(id(posting), []).append(('uncleared', True))

    def _adjust_meta(
            directive: Union[Directive, Posting]) -> Union[Directive, Posting]:
        meta = collections.OrderedDict(
            sorted((key, value)
                   for key, value in (directive.meta or {}).items()
                   if not key.startswith('invalid') and key != 'uncleared'))
        meta.update(id_to_metadata.get(id(directive), []))
        if dict(meta) == directive.meta:
            return directive
        return directive._replace(meta=meta)

    def _adjust_entry(entry: Directive) -> Directive:
        if not isinstance(entry, Transaction): return entry
        new_entry = _adjust_meta(entry)
        modified = new_entry is not entry
        new_postings = []
        for posting in entry.postings:
            new_posting = _adjust_meta(posting)
            if new_posting is not posting:
                modified = True
            new_postings.append(new_posting)
        new_entry = new_entry._replace(postings=new_postings)
        if modified:
            return new_entry
        return entry

    editor = loaded_reconciler.editor

    stage = editor.stage_changes()
    for entry in editor.all_entries:
        new_entry = _adjust_entry(entry)
        if new_entry is not entry:
            stage.change_entry(entry, new_entry)
    return {
        filename: result.new_contents
        for filename, result in editor.get_file_change_results(
            stage.get_diff().change_sets).items()
    }


class ReconcileGoldenTester:
    def __init__(self,
                 golden_directory: str,
                 temp_dir: str,
                 options: Dict[str, Any],
                 replacements: List[Tuple[str, str]] = [],
                 write: Optional[bool] = None):
        self.golden_directory = golden_directory
        self.temp_dir = temp_dir
        self.replacements = [
            (os.path.realpath(temp_dir), '<journal-dir>'),
            (testdata_root, '<testdata>'),
            *replacements,
        ]
        self.write = write
        self.snapshot_number = 1
        initial_snapshot_name = '0'
        initial = os.path.join(golden_directory, initial_snapshot_name)
        for name in os.listdir(initial):
            if name in ['errors.json', 'pending.beancount', 'candidates.diff']:
                continue
            shutil.copyfile(
                os.path.join(initial, name), os.path.join(temp_dir, name))
        journal_path = os.path.join(temp_dir, 'journal.beancount')
        self.reconciler = reconcile.Reconciler(
            journal_path=journal_path,
            ignore_path=os.path.join(temp_dir, 'ignore.beancount'),
            log_status=print,
            options=dict(
                transaction_output_map=[],
                price_output=None,
                open_account_output_map=[],
                default_output=journal_path,
                balance_account_output_map=[],
                fuzzy_match_days=5,
                fuzzy_match_amount=0,
                account_pattern=None,
                ignore_account_for_classification_pattern=training.
                DEFAULT_IGNORE_ACCOUNT_FOR_CLASSIFICATION_PATTERN,
                classifier_cache=None,
                **options),
        )
        self.skip_ids = collections.Counter()  # type: Dict[str, int]
        self.next_candidates = None  # type: Optional[reconcile.Candidates]
        self._update_candidates()
        test_util.check_golden_contents(
            path=os.path.join(initial, 'errors.json'),
            expected_contents=json.dumps(
                self.loaded_reconciler.errors, indent='  ', sort_keys=True),
            replacements=self.replacements,
            write=self.write,
        )
        self._check_state(initial_snapshot_name)

    def _update_candidates(self):
        self.next_candidates, index, self.skip_ids = self.loaded_reconciler.get_next_candidates(
            self.skip_ids)

    def skip(self, index: int):
        self.skip_ids = self.loaded_reconciler.get_skip_ids_by_index(index)
        self._update_candidates()

    def accept_candidate(self, candidate_index: int, ignore=False):
        next_candidates = self.next_candidates
        assert next_candidates is not None
        self.loaded_reconciler.accept_candidate(
            next_candidates.candidates[candidate_index], ignore=ignore)
        self._update_candidates()

    def change_candidate(self, candidate_index: int, changes: Dict[str, Any]):
        next_candidates = self.next_candidates
        assert next_candidates is not None
        next_candidates.change_transaction(candidate_index, changes)

    @property
    def loaded_reconciler(self):
        return self.reconciler.loaded_future.result()

    def _check_state(self, snapshot_name: str):
        loaded_reconciler = self.loaded_reconciler
        golden = os.path.join(self.golden_directory, snapshot_name)
        test_util.check_golden_contents(
            path=os.path.join(golden, 'pending.beancount'),
            expected_contents=_encode_pending_entries(
                loaded_reconciler.pending_data),
            replacements=self.replacements,
            write=self.write,
        )
        test_util.check_golden_contents(
            path=os.path.join(golden, 'candidates.diff'),
            expected_contents=_encode_candidates(self.next_candidates),
            replacements=self.replacements,
            write=self.write,
        )
        for filename, expected_contents in _add_invalid_reference_and_uncleared_metadata(
                loaded_reconciler).items():
            test_util.check_golden_contents(
                path=filename,
                expected_contents=expected_contents,
                replacements=self.replacements,
                write=self.write,
            )

    def snapshot(self):
        snapshot_name = '%d' % self.snapshot_number
        self.snapshot_number += 1
        golden = os.path.join(self.golden_directory, snapshot_name)
        self._check_state(snapshot_name)
        for name in os.listdir(self.temp_dir):
            with open(
                    os.path.join(self.temp_dir, name),
                    'r',
                    encoding='utf-8',
                    newline='\n') as f:
                contents = f.read()
            test_util.check_golden_contents(
                path=os.path.join(golden, name),
                expected_contents=contents,
                replacements=self.replacements,
                write=self.write,
            )


def test_basic(tmpdir: py.path.local):
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile', 'test_basic'),
        temp_dir=str(tmpdir),
        options=dict(
            data_sources=[
                {
                    'module': 'beancount_import.source.mint',
                    'filename': mint_data_path,
                },
            ],
        ),
    )

    tester.accept_candidate(0)
    tester.snapshot()
    tester.change_candidate(0, dict(accounts=['Expenses:Coffee'], ))
    tester.accept_candidate(0)
    tester.snapshot()


def test_basic_nearest_neighbor(tmpdir: py.path.local):
    cache_path = str(tmpdir.join('classifier_cache'))
    journal_dir = tmpdir.mkdir('journal')
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile',
                                      'test_basic_nearest_neighbor'),
        temp_dir=str(journal_dir),
        options=dict(
            data_sources=[
                {
                    'module': 'beancount_import.source.mint',
                    'filename': mint_data_path,
                },
            ],
            classifier='nearest_neighbor',
        ),
    )
    tester.reconciler.options['classifier_cache'] = cache_path

    tester.accept_candidate(0)
    # Unlike the decision tree (see test_basic), the accepted transaction
    # informs the next prediction without retraining.
    tester.snapshot()
    tester.change_candidate(0, dict(accounts=['Expenses:Coffee'], ))
    tester.accept_candidate(0)
    tester.snapshot()

    # Retraining writes the encoder cache; reloading reuses the encoder.
    tester.reconciler.retrain()
    loaded = tester.loaded_reconciler
    assert os.path.exists(cache_path)
    encoder = loaded.classifier.encoder
    assert encoder is not None
    tester.reconciler.reload_journal()
    reloaded = tester.loaded_reconciler
    assert reloaded is not loaded
    assert reloaded.classifier.encoder is encoder
    assert len(reloaded.classifier) == len(loaded.classifier)


def test_ignore(tmpdir: py.path.local):
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile',
                                      'test_ignore'),
        temp_dir=str(tmpdir),
        options=dict(
            data_sources=[
                {
                    'module': 'beancount_import.source.mint',
                    'filename': mint_data_path,
                },
            ],
        ),
    )

    tester.accept_candidate(1, ignore=True)
    tester.snapshot()
    tester.accept_candidate(0, ignore=True)
    tester.snapshot()
    tester.accept_candidate(0, ignore=True)
    tester.snapshot()

@pytest.mark.parametrize('testdata_subdir', ['test_ofx_basic', 'test_ofx_ignore_balance', 'test_ofx_ignore_price'])
def test_ofx_basic(tmpdir: py.path.local, testdata_subdir: str):
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile',
                                      testdata_subdir),
        temp_dir=str(tmpdir),
        options=dict(
            data_sources=[
                {
                    'module':
                    'beancount_import.source.ofx',
                    'ofx_filenames': [
                        os.path.join(testdata_root, 'source', 'ofx',
                                     'vanguard_roth_ira.ofx')
                    ],
                },
            ],
        ),
    )

def test_ofx_matching(tmpdir: py.path.local):
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile',
                                      'test_ofx_matching'),
        temp_dir=str(tmpdir),
        options=dict(
            data_sources=[
                {
                    'module':
                    'beancount_import.source.ofx',
                    'ofx_filenames': [
                        os.path.join(testdata_root, 'source', 'ofx',
                                     'vanguard_roth_ira.ofx')
                    ],
                },
            ],
        ),
    )


def test_ofx_cleared(tmpdir: py.path.local):
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile',
                                      'test_ofx_cleared'),
        temp_dir=str(tmpdir),
        options=dict(
            data_sources=[
                {
                    'module':
                    'beancount_import.source.ofx',
                    'ofx_filenames': [
                        os.path.join(testdata_root, 'source', 'ofx',
                                     'vanguard_roth_ira.ofx')
                    ],
                },
            ],
        ),
    )

def test_amazon_large_matching(tmpdir: py.path.local):
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile',
                                      'test_amazon_large'),
        temp_dir=str(tmpdir),
        options=dict(
            data_sources=[
                {
                    'module': 'beancount_import.source.amazon',
                    'directory':
                        os.path.join(testdata_root, 'source', 'amazon_large'),
                    'amazon_account': 'a@example.com',
                },
            ],
        ),
    )


def test_reload_collects_previous_load(tmpdir: py.path.local):
    # Loading freezes the GC (see LoadedReconciler); a reload must make
    # cyclic garbage from before the previous freeze collectable again.
    import gc
    import weakref

    class Node:
        def __init__(self) -> None:
            self.cycle = self

    node = Node()
    ref = weakref.ref(node)

    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile', 'test_basic'),
        temp_dir=str(tmpdir),
        options=dict(data_sources=[{
            'module': 'beancount_import.source.mint',
            'filename': mint_data_path,
        }]),
    )
    try:
        del node
        gc.collect()
        assert ref() is not None  # frozen by the load

        tester.reconciler.reload_journal()
        tester.reconciler.loaded_future.result()
        assert ref() is None
    finally:
        # Don't leave later tests with a frozen heap.
        gc.unfreeze()


@pytest.mark.parametrize('missing_currency', [MISSING, None])
def test_stage_missing_accounts_with_unspecified_currency(tmpdir, missing_currency):
    journal_path = str(tmpdir.join('journal.beancount'))
    tmpdir.join('journal.beancount').write('')
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    transaction, = test_util.parse("""
        2016-01-01 * "Synthetic transaction"
          Assets:New  -10 USD
          Expenses:New  10 USD
          Expenses:New  20 EUR
    """)
    posting = transaction.postings[-1]
    transaction = transaction._replace(postings=transaction.postings + [
        posting._replace(units=posting.units._replace(currency=missing_currency)),
        posting._replace(account='Expenses:Unspecified',
                         units=posting.units._replace(currency=missing_currency)),
    ])
    stage.add_entry(transaction, journal_path)

    reconcile.stage_missing_accounts(stage, lambda entry: journal_path)

    opens = {entry.account: entry for entry in stage.get_all_new_entries()
             if isinstance(entry, Open)}
    assert opens['Assets:New'].currencies == ['USD']
    assert opens['Expenses:New'].currencies == ['EUR', 'USD']
    assert opens['Expenses:Unspecified'].currencies == []
    assert all(entry.date == transaction.date for entry in opens.values())
    assert stage.get_missing_accounts() == []


@pytest.mark.parametrize('classifier', reconcile.CLASSIFIERS)
def test_without_classifier(tmpdir: py.path.local, classifier: str):
    tester = ReconcileGoldenTester(
        golden_directory=os.path.join(testdata_root, 'reconcile', 'test_basic'),
        temp_dir=str(tmpdir),
        options=dict(
            classifier=classifier, build_classifier=False,
            data_sources=[dict(module='beancount_import.source.mint',
                               filename=mint_data_path)],
        ),
    )
    loaded = tester.loaded_reconciler
    assert loaded.classifier is None
    assert loaded.training_examples.training_examples == []
    assert loaded.retrain() is loaded
    num_pending = loaded.num_pending
    tester.accept_candidate(0)
    assert loaded.num_pending < num_pending
    assert loaded.training_examples.training_examples == []
    tester.reconciler.reload_journal()
    assert tester.loaded_reconciler.classifier is None
    assert tester.loaded_reconciler.training_examples.training_examples == []
