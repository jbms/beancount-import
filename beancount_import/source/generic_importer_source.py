"""This module implements a Source Subclass for wrapping
`beancount.ingest.importer.ImporterProtocol` subclasses importers.
The importers are considered athoritative of the account they represent.

The Transaction.narration set by each importer is copied to Posting.meta[source_desc]
This helps in predicting postings for similar transaction while allowing the
user to change the Transaction description and payee from UI (see readme.md for more on source_desc)

Author: Sufiyan Adhikari(github.com/dumbPy)
"""

import os
import hashlib
from glob import glob
from typing import List, Tuple

from beancount.core.data import Transaction, Posting,  Directive
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
                 account: str ,
                 importer: ImporterProtocol,
                 account_name:str = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = os.path.expanduser(directory)
        self.importer = importer
        self.account = account
        self.account_name = account_name if account_name else self.name

        self.comparator = SimilarityComparator()

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
        entries = {}
        for f in self.files:
            f_entries = self.importer.extract(f)
            # deduplicate across statements
            hashed_entries = {}
            for entry in f_entries:
                hash_ = self._hash_entry(entry, frozenset(['filename','lineno']))
                # skip the existing entries from other statements
                if hash_ in entries: continue
                # If the entry exists in the journal, skip
                if self._is_existing(journal, entry): continue
                # add importer name as sorce description to source postings
                self._add_description(entry)
                # balance amount
                self.balance_amounts(entry)
                hashed_entries[hash_] = entry
            entries = {**entries, **hashed_entries}

        results.add_pending_entries(
            [ImportResult(entry.date, [entry], None)
                for entry in entries.values()
            ]
        )

    def _is_existing(self, journal: 'JournalEditor', entry: Directive) -> bool:
        """Check if the entry exists in journal and is cleared"""
        matches:List[Tuple[Transaction, Transaction]] = \
            find_similar_entries([entry], journal.entries, self.comparator, 0)
        if not matches: return False
        for posting in matches[0][1].postings:
            if self.is_posting_cleared(posting):
                return True
        return False

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

    def is_posting_cleared(self, posting: Posting) -> bool:
        """Given than this source is athoritative of the accoutn of a particular posting,
        return if that posting is cleared.
        This is an added layer of filter on what postings are used for training classifiers.
        Each Individual Importer can either implement it if required or else
        all postings which have `source_desc` meta key are considered cleared
        """
        if getattr(self.importer, 'is_posting_cleared', None):
            return self.importer.is_posting_cleared(posting)
        if isinstance(posting.meta, dict) and "source_desc" in posting.meta:
            return True
        return False


def load(spec, log_status):
    return ImporterSource(log_status=log_status, **spec)