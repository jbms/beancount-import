"""
helper classes for deterministic, rule-based preprocssing of imported
transactions, allowing one to, e.g. avoid needing to confirm repetetive
transactions within the web-ui.  whatever object is used for the preprocessing
should be passed to the webserver using the preprocessor key.

beancount_import.webserver.main(
    ...
    preprocessor=mypreprocessor
    )

that preprocessor takes one agrument--a reference to the loadedreconciler, and
can then directly modify the loaded reconciler program state. e.g. modifying
the list of pending entries found at loadedreconciler.pending_data or writing
to the ledger using the loadedreconciler.editor methods.

to simplify managing loadedreconciler state, a basepreprocessor class is
provided below which your custom preprocessor can inheret from and simplifies
ignoring, recording and modifying a pending entry while still affording a high
degree of flexibility.

and for simple pending entries having just one transaction which has one
imported posting and one unmatched fixme account where only the fixme account
needs updating, a transactionrulemixin is provided that you can inherit from
and specify matching rules in a dictionary-like fashion.

"""

from enum import Enum
from typing import Dict, Union, Pattern, List, Optional, Iterator, Any
from dataclasses import dataclass

from beancount.core.data import Transaction, Posting

from .reconcile import LoadedReconciler, PendingEntry
from .matching import FIXME_ACCOUNT, is_unknown_account


class BasePreprocessor:
    def __init__(self, loaded_reconciler: LoadedReconciler):
        self.loaded_reconciler = loaded_reconciler
        self.pending_data = loaded_reconciler.pending_data
        self.posting_db = loaded_reconciler.posting_db
        self.pending_transaction_ids = loaded_reconciler.pending_transaction_ids
        self.editor = loaded_reconciler.editor
        self.preprocess()

    def ignore_pending_entry(self, entry: PendingEntry) -> None:
        """ignores a pending entry by writing it to ignore_path and removing
        from tracking."""
        # remove from posting_db and pending_transaction_ids
        for directive in entry.entries:
            if isinstance(directive, Transaction):
                self.posting_db.remove_transaction(directive)
                self.pending_transaction_ids.remove(id(directive))
        # write to ignore file
        stage = self.editor.stage_changes()
        for directive in entry.entries:
            stage.add_entry(directive, self.loaded_reconciler.reconciler.ignore_path)
        stage.apply()
        # remove from pending_data
        self.pending_data.remove(entry)

    def record_pending_entry(
        self, old_entry: PendingEntry, new_entry: PendingEntry
    ) -> None:
        """records a pending entry and removes it from tracking."""
        for directive in old_entry.entries:
            if isinstance(directive, Transaction):
                self.posting_db.remove_transaction(directive)
                self.pending_transaction_ids.remove(id(directive))
        self.pending_data.remove(old_entry)
        # write to journal
        stage = self.editor.stage_changes()
        for directive in new_entry.entries:
            output_filename = self.loaded_reconciler.reconciler.entry_file_selector(
                directive
            )
            stage.add_entry(directive, output_filename)
            stage.apply()
            if isinstance(directive, Transaction):
                self.posting_db.add_transaction(directive)

    def modify_pending_entry(
        self, old_entry: PendingEntry, new_entry: PendingEntry
    ) -> None:
        """replaces an existing pending entry with a modified version."""
        # remove old entry from tracking
        for directive in old_entry.entries:
            if isinstance(directive, Transaction):
                self.posting_db.remove_transaction(directive)
                self.pending_transaction_ids.remove(id(directive))
        # add new entry to tracking
        for directive in new_entry.entries:
            if isinstance(directive, Transaction):
                self.posting_db.add_transaction(directive)
                self.pending_transaction_ids.add(id(directive))
        # replace in pending_data
        idx = self.pending_data.index(old_entry)
        self.pending_data[idx] = new_entry

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

    def find_pending_entries(self, predicate) -> Iterator[PendingEntry]:
        """finds all pending entries matching a predicate function."""
        return filter(predicate, self.pending_data)

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


class TransactionRuleMixin:
    """mixin class that adds rule-based preprocessing capabilities for simple
    pendingentries containing just one transaction with one imported posting
    and one fixme posting needing updating"""

    def is_simple_pending_entry(self, entry: PendingEntry) -> bool:
        # ensure the pending entry is a single transaction with one imported
        # posting followed by one auto-generated, matching FIXME postng
        return (
            isinstance(entry.entries[0], Transaction)
            and (len(entry.entries) == 1)
            and not is_unknown_account(entry.entries[0].postings[0].account)
            and is_unknown_account(entry.entries[0].postings[1].account)
        )

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
        entries = self.pending_data.copy()
        for entry in entries:
            if not self.is_simple_pending_entry(entry):
                continue

            transaction = entry.entries[0]
            imported_posting = transaction.postings[0]
            fixme_posting = transaction.postings[1]

            for rule in rules:
                if self._matches_criteria(entry, imported_posting, transaction, rule):
                    if rule.action == "ignore":
                        self.ignore_pending_entry(entry)
                        break

                    new_postings = [
                        imported_posting,
                        fixme_posting._replace(account=rule.target_account),
                    ]
                    new_transaction = transaction._replace(postings=new_postings)
                    new_entry = entry._replace(entries=(new_transaction,))

                    if rule.action == "modify":
                        self.modify_pending_entry(entry, new_entry)
                        break
                    else:
                        self.record_pending_entry(entry, new_entry)
                        break
