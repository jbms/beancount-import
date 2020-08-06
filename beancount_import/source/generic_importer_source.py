"""This module implements a Source Subclass for wrapping
`beancount.ingest.importer.ImporterProtocol` subclasses importers.
The importers are considered athoritative of the account they represent.

The Transaction.narration set by each importer is copied to Posting.meta[source_desc]
This helps in predicting postings for similar transaction while allowing the
user to change the Transaction description and payee from UI
(see readme.md for more on source_desc)
This `source_desc` meta is also used for check cleared postings and should not be
changed manually

Author: Sufiyan Adhikari(github.com/dumbPy)
"""

import os
import hashlib
from glob import glob
from typing import List
from collections import defaultdict
import itertools
import datetime

from beancount.core.data import Transaction, Posting,  Directive
from beancount.core import data
from beancount.core.amount import Amount
from beancount.ingest.importer import ImporterProtocol
from beancount.core.compare import hash_entry
from beancount.ingest.cache import get_file
from beancount.ingest.similar import find_similar_entries, SimilarityComparator

from ..matching import FIXME_ACCOUNT, SimpleInventory
from . import ImportResult, Source, SourceResults, InvalidSourceReference, AssociatedData
from ..journal_editor import JournalEditor


class ImporterSource(Source):
    def __init__(self,
                 directory: str,
                 account: str,
                 importer: ImporterProtocol,
                 account_name:str = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = os.path.expanduser(directory)
        self.importer = importer
        self.account = account
        self.account_name = account_name if account_name else self.name

        self._comparator = SimilarityComparator()

        # get _FileMemo object for each file
        files = [get_file(f) for f in
                 glob(os.path.join(directory, '**', '*'), recursive=True)
        ]
        # filter the valid files for this importer
        self.files = [f for f in files if self.importer.identify(f)]

    @property
    def name(self):
        return self.importer.name()

    def prepare(self, journal: 'JournalEditor', results: SourceResults) -> None:
        results.add_account(self.account)
        entries = defaultdict(list)
        for f in self.files:
            f_entries = self.importer.extract(f)
            # collect  all entries in current statement, grouped by hash
            hashed_entries = defaultdict(list)
            for entry in f_entries:
                hash_ = self._hash_entry(entry, frozenset(['filename','lineno']))
                hashed_entries[hash_].append(entry)
            # deduplicate across statements
            for hash_ in hashed_entries:
                # skip the existing entries from other statements. add remaining
                n = len(entries[hash_])
                entries[hash_].extend(hashed_entries[hash_][n:])

        uncleared_entries = defaultdict(list)
        for hash_ in entries:
            # number of matching cleared entries in journal
            n = len(similar_entries_in_journal(entries[hash_][0],
                                               journal.entries,
                                               self.comparator))
            # If journal has n cleared entries for this hash, pick remaining
            for entry in entries[hash_][n:]:
                # add importer name as sorce description to source postings
                self._add_description(entry)
                # balance amount
                self.balance_amounts(entry)
                uncleared_entries[hash_].append(entry)

        results.add_pending_entries(
            [ImportResult(entry.date, [entry], None)
                for entry in itertools.chain.from_iterable(uncleared_entries.values())
            ]
        )

    def comparator(self, entry1, entry2):
        """Returns if the two entries are similar and 2nd entry is cleared.
        The first entry is from new_entries and 2nd is from journal
        """
        return self._comparator(entry1, entry2) \
               and self.is_entry_cleared(entry2) \
               and entry1.narration == entry2.postings[0].meta['source_desc']

    def _add_description(self, entry: Transaction):
        if not isinstance(entry, Transaction): return None
        postings = entry.postings #type: ignore
        to_mutate = []
        for i, posting in enumerate(postings):
            if isinstance(posting.meta, dict): posting.meta["source_desc"] = entry.narration
            else: to_mutate.append(i)
        for i in to_mutate:
            p = postings.pop(i)
            p = Posting(p.account, p.units, p.cost, p.price, p.flag, {"source_desc":entry.narration})
            postings.insert(i, p)

    @staticmethod
    def balance_amounts(txn:Transaction)-> None:
        """Add FIXME account for the remaing amount to balance accounts"""
        inventory = SimpleInventory()
        for posting in txn.postings:
            inventory += posting.units
        for currency in inventory:
            txn.postings.append(
                Posting(
                    account=FIXME_ACCOUNT,
                    units=Amount(currency=currency, number=-inventory[currency]),
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                ))

    @staticmethod
    def _hash_entry(entry:Directive, exclude_meta_keys=frozenset()) -> str:
        """Similar to beancount.core.compare.hash_entry but can skip selective meta fields
        the meta fields to be used for hashing should be in Transaction's meta, not Posting's meta
        """
        if not isinstance(entry, Transaction): return hash_entry(entry)
        h = hashlib.md5()
        h.update(hash_entry(entry, exclude_meta=True).encode())
        for key in entry.meta:
            if key in exclude_meta_keys: continue
            h.update(str(entry.meta[key]).encode())
        return h.hexdigest()

    def is_entry_cleared(self, entry: Transaction) -> bool:
        """If an entry has a cleared posting, it is considered cleared"""
        for posting in entry.postings:
            if self.is_posting_cleared(posting): return True
        return False

    def is_posting_cleared(self, posting: Posting) -> bool:
        """Given than this source is athoritative of the account of a particular posting,
        return if that posting is cleared.
        Each Individual Importer can either implement it if required or else
        all postings which have `source_desc` meta key are considered cleared
        """
        if getattr(self.importer, 'is_posting_cleared', None):
            return self.importer.is_posting_cleared(posting)
        if isinstance(posting.meta, dict) and "source_desc" in posting.meta:
            return True
        return False

def similar_entries_in_journal(entry:Transaction, source_entries:List[Directive],
                               comparator=None, window_days=2) -> List[Transaction]:
    """Given a hashed entry, find the similar entries in the journal
    This is a rewrite of beancount.ingest.similar.find_similar_entries
    to get all possible matches for a single new entry
    """
    window_head = datetime.timedelta(days=window_days)
    window_tail = datetime.timedelta(days=window_days + 1)

    if comparator is None:
        comparator = SimilarityComparator()

    # Look at existing entries at a nearby date.
    duplicates = []
    for source_entry in data.filter_txns(
            data.iter_entry_dates(source_entries,
                                  entry.date - window_head,
                                  entry.date + window_tail)):
        if comparator(entry, source_entry):
            duplicates.append(source_entry)
    return duplicates

def load(spec, log_status):
    return ImporterSource(log_status=log_status, **spec)