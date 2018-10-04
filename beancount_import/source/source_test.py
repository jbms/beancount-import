from typing import List, Optional, Union, Tuple, Dict

import pytest
import py

import beancount.parser.parser
import beancount.parser.printer
from beancount.core.data import Posting, Transaction, Meta, ALL_DIRECTIVES

from ..journal_editor import JournalEditor
from . import load_source as _load_source, ImportResult, SourceResults, SourceSpec, InvalidSourceReference
from .. import training
from .. import test_util

Directive = Union[ALL_DIRECTIVES]
Entries = List[Directive]


def log_status(x):
    print(x)


def load_source(source_spec: SourceSpec):
    return _load_source(source_spec, log_status=log_status)


def import_result(
        info: dict,
        entries: str,
        unknown_account_prediction_inputs: Optional[List[
            training.PredictionInput]] = None
) -> Tuple[ImportResult, Optional[List[training.PredictionInput]]]:
    parsed_entries = [
        test_util.normalize_entry(entry) for entry in test_util.parse(entries)
    ]
    return ImportResult(
        date=parsed_entries[0].date, entries=parsed_entries,
        info=info), unknown_account_prediction_inputs


InvalidReferenceSpec = Tuple[int, List[Tuple[str, Optional[str]]]]


def _get_invalid_references(entries: Entries, specs: List[InvalidReferenceSpec]
                            ) -> List[InvalidSourceReference]:
    transactions_by_id = dict()  # type: Dict[str, Transaction]
    postings_by_id = dict()  # type: Dict[Tuple[str, str], Posting]
    for entry in entries:
        if not isinstance(entry, Transaction): continue
        if not entry.meta: continue
        invalid_id = entry.meta.get('invalid_id')
        if invalid_id is None: continue
        transactions_by_id[invalid_id] = entry
        for posting in entry.postings:
            if not posting.meta: continue
            posting_id = posting.meta.get('invalid_id')
            if posting_id is None: continue
            postings_by_id[(invalid_id, posting_id)] = posting
    return [
        InvalidSourceReference(
            num_extras,
            [(transactions_by_id.get(transaction_id),
              postings_by_id.get((transaction_id, posting_id))
              if posting_id is not None else None)
             for transaction_id, posting_id in transaction_posting_pairs])
        for num_extras, transaction_posting_pairs in specs
    ]


def format_pending_entry(p: ImportResult):
    return '''
            import_result(
                info=%r,
                entries=r"""
%s
                """,
            )''' % (p.info, test_util.format_entries(p.entries,
                                                     indent=16).rstrip())


def format_pending_entries(pending: List[ImportResult]):
    return '        [' + ',\n'.join(format_pending_entry(p)
                                    for p in pending) + '\n        ]'


def normalize_import_result(pending: ImportResult) -> ImportResult:
    return pending._replace(
        entries=[test_util.normalize_entry(entry) for entry in pending.entries])


def normalize_import_results(pending: List[ImportResult]) -> List[ImportResult]:
    return [normalize_import_result(p) for p in pending]


def check_source(tmpdir,
                 source_spec: SourceSpec,
                 pending: List[Tuple[ImportResult, Optional[List[
                     training.PredictionInput]]]],
                 journal_contents='',
                 invalid_references: List[InvalidReferenceSpec] = [],
                 accounts=frozenset(),
                 training_examples: Optional[List[Tuple[
                     training.PredictionInput, str]]] = None):
    journal_path = tmpdir.join('journal.beancount')
    journal_path.write(journal_contents)
    journal_editor = JournalEditor(str(journal_path))
    if journal_editor.errors:
        assert journal_editor.errors == []
    source = load_source(source_spec)
    results = SourceResults()
    source.prepare(journal_editor, results)
    results.pending.sort(key=lambda x: x.date)
    actual_pending = normalize_import_results(results.pending)
    pending_entries = [entry for entry, _ in pending]
    if actual_pending != pending_entries:
        print(format_pending_entries(results.pending))
    expected_invalid_references = _get_invalid_references(
        journal_editor.entries, invalid_references)
    assert actual_pending == pending_entries
    assert results.accounts == set(accounts)
    assert sorted(
        results.invalid_references) == sorted(expected_invalid_references)

    for entry in journal_editor.entries:
        if not isinstance(entry, Transaction): continue
        for posting in entry.postings:
            if posting.account in results.accounts:
                is_cleared = source.is_posting_cleared(posting)
                assert is_cleared == posting.meta.get('cleared', False)

    account_source_map = {account: source for account in results.accounts}
    sources = [source]
    extractor = training.FeatureExtractor(
        sources=sources, account_source_map=account_source_map)

    if training_examples is not None:
        actual_training_examples = training.MockTrainingExamples()
        extractor.extract_examples(journal_editor.entries,
                                   actual_training_examples)
        if actual_training_examples.examples != training_examples:
            print(actual_training_examples.examples)
        assert actual_training_examples.examples == training_examples

    for entry, expected_input in pending:
        if expected_input is not None:
            assert len(entry.entries) == 1
            assert extractor.extract_unknown_account_group_features(
                entry.entries[0]) == expected_input
