"""Facilities for defining sources based on non-unique description text."""

import datetime
import collections
from typing import Iterable, Tuple, Dict, TypeVar, Callable, List, AbstractSet, Union, Optional

from beancount.core.data import Transaction, Posting, Open, Directive, CostSpec, Meta
from beancount.core.number import ZERO, MISSING

from ..posting_date import POSTING_DATE_KEY
from . import Source, SourceResults, ImportResult, InvalidSourceReference
from ..training import TrainingExamples
from ..unbook import unbook_postings, group_postings_by_meta

SOURCE_DESC_KEYS = ['source_desc'] + ['source_desc%d' % x for x in range(1, 3)]

MATCHED_METADATA_KEYS = frozenset(SOURCE_DESC_KEYS)


def get_posting_source_descs(
        posting: Posting) -> Iterable[Tuple[str, datetime.date]]:
    posting_date = posting.meta.get(POSTING_DATE_KEY, None)

    # To handle duplicate downloaded entries (hopefully rare), we allow multiple source_desc values per posting.
    for sd_key in SOURCE_DESC_KEYS:
        source_desc = posting.meta.get(sd_key, None)
        if source_desc is not None:
            if posting_date is None:
                raise RuntimeError(
                    'Posting date is missing on entry: %r' % (posting, ))
            yield (source_desc, posting_date)


def get_account_mapping(accounts: Dict[str, Open], metadata_key: str
                        ) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Populates the bidirectional mappings id_to_account and
    account_to_id based on the metadata_key metadata field.
    """

    id_to_account = {}  # type: Dict[str, str]
    account_to_id = {}  # type: Dict[str, str]

    for entry in accounts.values():
        account_id = entry.meta.get(metadata_key, None)
        if account_id is None:
            continue
        if not isinstance(account_id, str):
            raise RuntimeError(
                'Invalid %s (not string): %r' % (metadata_key, account_id))
        old_id = account_to_id.get(entry.account, None)
        old_account = id_to_account.get(account_id, None)
        if old_id is not None and old_id != account_id:
            raise RuntimeError('Duplicate mappings for account %r: %r' %
                               (entry.account,
                                [old_id, account_id]))
        if old_account is not None and old_account != entry.account:
            raise RuntimeError('Duplicate mappings for %s %r: %r' %
                               (metadata_key, account_id,
                                [old_account, entry.account]))
        account_to_id[entry.account] = account_id
        id_to_account[account_id] = entry.account
    return account_to_id, id_to_account


RawEntry = TypeVar('RawEntry')
RawEntryKey = TypeVar('RawEntryKey')


def get_pending_and_invalid_entries(
        raw_entries: Iterable[RawEntry], journal_entries: Iterable[Directive],
        account_set: AbstractSet[str], get_key_from_posting: Callable[[
            Transaction, Posting, List[Posting], str, datetime.date
        ], RawEntryKey],
        get_key_from_raw_entry: Callable[[RawEntry], RawEntryKey],
        make_import_result: Callable[[RawEntry], ImportResult],
        results: SourceResults,
        get_keys_from_raw_entry: Optional[Callable[[RawEntry],
                                                  Iterable[RawEntryKey]]] = None
        ) -> None:
    """Match source references, emitting one pending result per unmatched record.

    A record may supply several keys. Each occurrence consumes one matching
    journal reference; if any is missing, the record is offered once.
    get_keys_from_raw_entry overrides the single-key callback when provided.
    """
    matched_postings = dict(
    )  # type: Dict[RawEntryKey, List[Tuple[Transaction, Posting]]]

    for entry in journal_entries:
        if not isinstance(entry, Transaction):
            continue
        for postings in group_postings_by_meta(entry.postings):
            posting = unbook_postings(postings)
            if posting.meta is None:
                continue
            if posting.account not in account_set:
                continue
            for source_desc, posting_date in get_posting_source_descs(posting):
                key = get_key_from_posting(entry, posting, postings,
                                           source_desc, posting_date)
                if key is None:
                    continue
                matched_postings.setdefault(key, []).append((entry, posting))

    matched_postings_counter = collections.Counter(
    )  # type: Dict[RawEntryKey, int]
    for key, entry_posting_pairs in matched_postings.items():
        matched_postings_counter[key] += len(entry_posting_pairs)

    pending_entries = []  # type: List[Tuple[RawEntry, Dict[RawEntryKey, int]]]
    for raw_entry in raw_entries:
        keys = (get_keys_from_raw_entry(raw_entry)
                if get_keys_from_raw_entry is not None
                else [get_key_from_raw_entry(raw_entry)])
        key_counts: Dict[RawEntryKey, int] = collections.Counter(keys)
        if all(matched_postings_counter[key] >= count
               for key, count in key_counts.items()):
            for key, count in key_counts.items():
                matched_postings_counter[key] -= count
        else:
            pending_entries.append((raw_entry, key_counts))

    # Match complete records before consuming references for partial records.
    # Otherwise a new overlapping record can steal an accepted record's match.
    for raw_entry, key_counts in pending_entries:
        for key, count in key_counts.items():
            matched_postings_counter[key] = max(
                0, matched_postings_counter[key] - count)
        results.add_pending_entry(make_import_result(raw_entry))

    for key, entry_posting_pairs in matched_postings.items():
        extra = matched_postings_counter[key]
        if extra:
            results.add_invalid_reference(
                InvalidSourceReference(extra, entry_posting_pairs))

    results.add_accounts(account_set)


class DescriptionBasedSource(Source):
    def get_example_key_value_pairs(self, transaction: Transaction,
                                    posting: Posting):
        result = {}
        if posting.meta is not None:
            meta = posting.meta
            key = SOURCE_DESC_KEYS[0]
            value = meta.get(key)
            if value is not None:
                result['desc'] = value
        return result

    def is_posting_cleared(self, posting: Posting):
        if posting.meta is None:
            return False
        if posting.meta.get("merge"):
            return True
        return not MATCHED_METADATA_KEYS.isdisjoint(posting.meta)
