"""Opt-in preprocessing of pending imports before account classification.

Pass a callable accepting a LoadedReconciler as the ``preprocessor`` option.
BasePreprocessor supplies helpers for recording, ignoring and modifying pending
entries. Combine it with TransactionRuleMixin for simple two-posting rules.
"""

from enum import Enum
from typing import Dict, Union, Pattern, List, Optional, Iterator, Any, cast, Callable
from dataclasses import dataclass

from beancount.core.data import Transaction, Posting

from .reconcile import (LoadedReconciler, PendingEntry, make_pending_entry,
                        stage_missing_accounts)
from .source import ImportResult
from .journal_editor import StagedChanges
from .matching import FIXME_ACCOUNT, is_unknown_account


class BasePreprocessor:
    def __init__(self, loaded_reconciler: LoadedReconciler):
        self.loaded_reconciler = loaded_reconciler
        self.posting_db = loaded_reconciler.posting_db
        self.pending_transaction_ids = loaded_reconciler.pending_transaction_ids
        self.editor = loaded_reconciler.editor
        self.preprocess()

    @property
    def pending_data(self) -> List[PendingEntry]:
        """Always access the reconciler's current pending list."""
        return self.loaded_reconciler.pending_data

    @pending_data.setter
    def pending_data(self, entries: List[PendingEntry]) -> None:
        self.loaded_reconciler.pending_data = entries

    def _remove_pending_entry(self, entry: PendingEntry) -> None:
        """Remove an entry from the pending list and matching index."""
        for directive in entry.entries:
            if isinstance(directive, Transaction):
                self.posting_db.remove_transaction(directive)
                self.pending_transaction_ids.discard(id(directive))
        removed_ids = {id(directive) for directive in entry.entries}
        loaded = self.loaded_reconciler
        loaded.uncleared_postings = [
            pair for pair in loaded.uncleared_postings
            if id(pair[0]) not in removed_ids]
        self.pending_data.remove(entry)

    def _stage_entry_removal(self, stage: StagedChanges, entry: PendingEntry) -> None:
        # Pending FIXME entries can also originate in the journal itself.
        for directive in entry.entries:
            if id(directive) in self.pending_transaction_ids:
                continue
            if any(directive is existing for existing in self.editor.entries):
                stage.remove_entry(directive)

    def ignore_pending_entry(self, entry: PendingEntry) -> None:
        """Move a pending entry to the ignore journal after writing succeeds."""
        self.pending_data.index(entry)  # Reject stale entries before writing.
        ignore_path = self.loaded_reconciler.reconciler.ignore_path
        if ignore_path is None:
            raise RuntimeError('An ignore journal is required to ignore entries')
        stage = self.editor.stage_changes()
        self._stage_entry_removal(stage, entry)
        for directive in entry.entries:
            stage.add_entry(directive, ignore_path)
        stage.apply()
        self._remove_pending_entry(entry)

    def record_pending_entry(
        self, old_entry: PendingEntry, new_entry: PendingEntry
    ) -> None:
        """Write all directives once, then update pending and matching state."""
        self.pending_data.index(old_entry)
        stage = self.editor.stage_changes()
        self._stage_entry_removal(stage, old_entry)
        select_file = self.loaded_reconciler.reconciler.entry_file_selector
        for directive in new_entry.entries:
            stage.add_entry(directive, select_file(directive))
        stage_missing_accounts(stage, select_file)
        result = stage.apply()
        self._remove_pending_entry(old_entry)
        # Booking supplies canonical entries with real filenames/line numbers.
        for directive in result.new_entries:
            if isinstance(directive, Transaction):
                self.posting_db.add_transaction(directive)
        loaded = self.loaded_reconciler
        loaded._add_uncleared_postings_from(result.new_entries)
        loaded.uncleared_postings.sort(key=lambda pair: pair[0].date)

    def modify_pending_entry(
        self, old_entry: PendingEntry, new_entry: PendingEntry
    ) -> None:
        """Replace a pending entry without writing it to the journal."""
        idx = self.pending_data.index(old_entry)
        refreshed = make_pending_entry(
            ImportResult(date=new_entry.date, entries=new_entry.entries,
                         info=new_entry.info), new_entry.source)
        for directive in old_entry.entries:
            if (isinstance(directive, Transaction)
                    and id(directive) in self.pending_transaction_ids):
                self.posting_db.remove_transaction(directive)
                self.pending_transaction_ids.remove(id(directive))
        # A journal-backed original stays indexed so the replacement can be
        # matched to it and accepted as an edit instead of an extra import.
        for directive in refreshed.entries:
            if isinstance(directive, Transaction):
                self.posting_db.add_transaction(directive)
                self.pending_transaction_ids.add(id(directive))
        self.pending_data[idx] = refreshed

    def get_transaction_postings(self, entry: PendingEntry) -> list[Posting]:
        """gets all postings from transactions in a pending entry."""
        postings = []
        for directive in entry.entries:
            if isinstance(directive, Transaction):
                postings.extend(directive.postings)
        return postings

    def get_fixme_postings(self, entry: PendingEntry) -> list[Posting]:
        """gets all postings with fixme accounts from a pending entry."""
        return [
            p
            for p in self.get_transaction_postings(entry)
            if is_unknown_account(p.account)
        ]

    def get_non_fixme_postings(self, entry: PendingEntry) -> list[Posting]:
        """gets all postings with non-fixme accounts from a pending entry."""
        return [
            p
            for p in self.get_transaction_postings(entry)
            if not is_unknown_account(p.account)
        ]

    def find_pending_entries(
        self, predicate: Callable[[PendingEntry], bool]
    ) -> Iterator[PendingEntry]:
        """Find matching entries in a snapshot safe to record or ignore."""
        return filter(predicate, self.pending_data.copy())

    def preprocess(self) -> None:
        """override this method in subclasses to implement preprocessing logic."""
        pass


class CriteriaType(Enum):
    """Datastructures Preprocessinrules will match against"""
    STRING = "string"
    DICT = "dict"
    LIST = "list"


CRITERIA_CONFIG = {
    # String criteria
    'source_name': ('source', 'name', CriteriaType.STRING),
    'posting_account': ('posting', 'account', CriteriaType.STRING),
    'posting_amount': ('posting', 'units', CriteriaType.STRING),
    'transaction_narration': ('transaction', 'narration', CriteriaType.STRING),
    'transaction_payee': ('transaction', 'payee', CriteriaType.STRING),
    'transaction_flag': ('transaction', 'flag', CriteriaType.STRING),
    'posting_flag': ('posting', 'flag', CriteriaType.STRING),
    # Dict-like criteria
    'posting_meta': ('posting', 'meta', CriteriaType.DICT),
    'transaction_meta': ('transaction', 'meta', CriteriaType.DICT),
    # List-like criteria
    'transaction_tags': ('transaction', 'tags', CriteriaType.LIST),
    'transaction_links': ('transaction', 'links', CriteriaType.LIST)
}


@dataclass
class PreprocessingRule:
    """rule for matching imported postings and determining target account for fixme posting."""
    # matching criteria for the imported posting
    # String criteria
    source_name: Optional[Union[str, Pattern]] = None
    posting_account: Optional[Union[str, Pattern]] = None
    posting_amount: Optional[Union[str, Pattern]] = None
    transaction_narration: Optional[Union[str, Pattern]] = None
    transaction_payee: Optional[Union[str, Pattern]] = None
    transaction_flag: Optional[Union[str, Pattern]] = None
    posting_flag: Optional[Union[str, Pattern]] = None
    # Dict-like criteria
    posting_meta: Optional[dict[str, Union[str, Pattern]]] = None
    transaction_meta: Optional[dict[str, Union[str, Pattern]]] = None
    # List-like criteria
    transaction_links: Optional[Union[str, Pattern, list[Union[str, Pattern]]]] = None
    transaction_tags: Optional[Union[str, Pattern, list[Union[str, Pattern]]]] = None
    # account to replace fixme posting with when rule matches
    target_account: str = FIXME_ACCOUNT
    action: str = "record"  # 'record', 'modify', or 'ignore'

    def __post_init__(self) -> None:
        if self.action not in ('record', 'modify', 'ignore'):
            raise ValueError('Unknown preprocessing action: %r' % self.action)


class TransactionRuleMixin:
    """mixin class that adds rule-based preprocessing capabilities for simple
    pendingentries containing just one transaction with one imported posting
    and one fixme posting needing updating"""

    def is_simple_pending_entry(self, entry: PendingEntry) -> bool:
        if len(entry.entries) != 1 or not isinstance(entry.entries[0], Transaction):
            return False
        postings = entry.entries[0].postings
        return (len(postings) == 2 and
                sum(is_unknown_account(p.account) for p in postings) == 1)

    def _matches_pattern(self, value: Any, pattern: Union[str, Pattern]) -> bool:
        """Helper to match string against literal or regex pattern."""
        if value is None:
            return False
        if isinstance(pattern, str):
            return str(value) == pattern
        return bool(pattern.match(str(value)))

    def _matches_string_criteria(self, value: Any, pattern: Union[str, Pattern]) -> bool:
        """Match a string criteria."""
        return self._matches_pattern(value, pattern)

    def _matches_dict_criteria(self, meta: Optional[Dict], criteria: Dict[str, Union[str, Pattern]]) -> bool:
        """Match dictionary criteria (metadata)."""
        if meta is None:
            return False
        return all(
            key in meta and self._matches_pattern(str(meta[key]), pattern)
            for key, pattern in criteria.items()
        )

    def _matches_list_criteria(self, values: Optional[Any], pattern: Union[str, Pattern, List[Union[str, Pattern]]]) -> bool:
        """Match list criteria (tags/links)."""
        if values is None:
            return False
        if isinstance(pattern, (str, Pattern)):
            return any(self._matches_pattern(value, pattern) for value in values)
        return all(
            any(self._matches_pattern(value, p) for value in values)
            for p in pattern
        )

    def _get_object_value(self, obj: Any, attr: str) -> Any:
        """Safely get attribute value from an object."""
        return getattr(obj, attr, None)

    def _matches_criteria(self, entry: PendingEntry, posting: Posting, 
                         transaction: Transaction, rule: PreprocessingRule) -> bool:
        """Check if entry matches all specified criteria in the rule."""
        for criteria_name, criteria_value in vars(rule).items():
            if criteria_value is None or criteria_name == 'target_account' or criteria_name == 'action':
                continue

            if criteria_name not in CRITERIA_CONFIG:
                continue

            obj_type, attr, criteria_type = CRITERIA_CONFIG[criteria_name]
            # Get the object to check (transaction, posting, or source)
            if obj_type == 'transaction':
                obj = transaction
            elif obj_type == 'posting':
                obj = posting
            elif obj_type == 'source':
                obj = entry.source
            else:
                continue
            # Get the value to check
            value = self._get_object_value(obj, attr)
            # Match based on criteria type
            if criteria_type == CriteriaType.STRING:
                if not self._matches_string_criteria(value, criteria_value):
                    return False
            elif criteria_type == CriteriaType.DICT:
                if not self._matches_dict_criteria(value, criteria_value):
                    return False
            elif criteria_type == CriteriaType.LIST:
                if not self._matches_list_criteria(value, criteria_value):
                    return False
        return True

    def apply_rules(self, rules: List[PreprocessingRule]) -> None:
        """Apply preprocessing rules to pending entries."""
        # Callers combine this mixin with BasePreprocessor.
        preprocessor = cast(BasePreprocessor, self)
        entries = preprocessor.pending_data.copy()
        for entry in entries:
            if not self.is_simple_pending_entry(entry):
                continue

            transaction = entry.entries[0]
            imported_posting = next(
                p for p in transaction.postings if not is_unknown_account(p.account))

            for rule in rules:
                if self._matches_criteria(entry, imported_posting, transaction, rule):
                    if rule.action == "ignore":
                        preprocessor.ignore_pending_entry(entry)
                        break

                    new_postings = [
                        p._replace(account=rule.target_account)
                        if is_unknown_account(p.account) else p
                        for p in transaction.postings
                    ]
                    new_transaction = transaction._replace(postings=new_postings)
                    new_entry = entry._replace(entries=(new_transaction,))

                    if rule.action == "modify":
                        preprocessor.modify_pending_entry(entry, new_entry)
                        break
                    else:
                        preprocessor.record_pending_entry(entry, new_entry)
                        break
