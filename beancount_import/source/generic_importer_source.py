"""This module implements a Source Subclass for wrapping
`beangulp.importer.Importer` or `beangulp.importer.ImporterProtocol` importers.
The importers are considered athoritative of the account they represent.

Missing source_desc and date metadata on source-account postings default to the
transaction's narration and date. Importer-supplied values are preserved.
This helps in predicting postings for similar transaction while allowing the
user to change the Transaction description and payee from UI
(see readme.md for more on source_desc)
This `source_desc` meta is also used for check cleared postings and should not be
changed manually

Author: Sufiyan Adhikari(github.com/dumbPy)
"""

import os
import datetime
from copy import deepcopy
from glob import glob
from collections import Counter, OrderedDict
import itertools
from typing import Hashable, List, Dict, Union

from beancount.core import interpolate
from beancount.core.data import Balance, Transaction, Posting, Directive, Options
from beancount.core.amount import Amount
from beancount.core.convert import get_weight
from beancount.core.number import MISSING
from beancount.parser import booking
from beangulp.importer import Importer, ImporterProtocol, Adapter
from beangulp.cache import get_file
from beancount.parser.booking_full import convert_costspec_to_cost

from ..matching import FIXME_ACCOUNT, SimpleInventory
from . import ImportResult, SourceResults
from ..journal_editor import JournalEditor, get_partially_booked_entries
from .description_based_source import DescriptionBasedSource, get_pending_and_invalid_entries


class ImporterSource(DescriptionBasedSource):
    def __init__(self,
                 directory: str,
                 account: str,
                 importer: Union[Importer, ImporterProtocol],
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = os.path.expanduser(directory)
        # The beangulp `Importer` interface operates on file paths, whereas the
        # legacy `ImporterProtocol` operates on `_FileMemo` objects. Wrap legacy
        # importers in `Adapter` so the rest of this class can uniformly use the
        # `Importer` interface (and so native `beangulp.Importer` subclasses are
        # supported directly).
        if isinstance(importer, Importer):
            self.importer = importer
        else:
            self.importer = Adapter(importer)
        self.account = account

        # get the absolute path of each file in the directory
        all_files = [os.path.abspath(f) for f in
                     filter(os.path.isfile,
                            glob(os.path.join(directory, '**', '*'), recursive=True))]
        # filter the valid files for this importer
        self.files = [f for f in all_files if self.importer.identify(f)]

    @property
    def name(self) -> str:
        return self.importer.name

    def prepare(self, journal: 'JournalEditor', results: SourceResults) -> None:
        results.add_account(self.account)

        def get_keys(entry: Directive) -> List[Hashable]:
            return self._get_keys_from_imported_entry(entry, journal.options_map)

        def get_posting_key(entry: Transaction, posting: Posting,
                            source_postings: List[Posting], source_desc: str,
                            posting_date: datetime.date) -> Hashable:
            units = posting.units
            if len(source_postings) == 1 and (
                    units is None or units is MISSING or units.number is MISSING):
                inferred = self._get_inferred_source_units(entry,
                                                           journal.options_map)
                units = inferred.get(id(source_postings[0]), units)
            return (posting.account, posting_date, units, source_desc)

        entries = OrderedDict() #type: Dict[Hashable, List[Directive]]
        for f in self.files:
            f_entries = self.importer.extract(f, journal.entries)
            # collect  all entries in current statement, grouped by hash
            hashed_entries = OrderedDict() #type: Dict[Hashable, List[Directive]]
            for entry in f_entries:
                entry_key = frozenset(Counter(get_keys(entry)).items())
                self._add_description(entry)
                hashed_entries.setdefault(entry_key, []).append(entry)
            # deduplicate across statements
            for key_ in hashed_entries:
                # skip the existing entries from other statements. add remaining
                if not key_ in entries:
                    n = 0
                else:
                    n = len(entries[key_])
                entries.setdefault(key_, []).extend(hashed_entries[key_][n:])

        get_pending_and_invalid_entries(
            raw_entries=list(itertools.chain.from_iterable(entries.values())),
            journal_entries=journal.all_entries,
            account_set=set([self.account]),
            get_key_from_posting=get_posting_key,
            get_key_from_raw_entry=lambda entry: get_keys(entry)[0],
            make_import_result=lambda entry: self._make_import_result(
                entry, journal.options_map),
            results=results,
            get_keys_from_raw_entry=get_keys)

    def _add_description(self, entry: Directive) -> None:
        if not isinstance(entry, Transaction):
            return
        for i, posting in enumerate(entry.postings):
            if posting.account != self.account:
                continue
            if posting.meta is None:
                posting = posting._replace(meta={})
                entry.postings[i] = posting
            posting.meta.setdefault("source_desc", entry.narration)
            posting.meta.setdefault("date", entry.date)

    def _get_inferred_source_units(self, entry: Transaction,
                                   options_map: Options) -> Dict[int, Amount]:
        indices = [i for i, p in enumerate(entry.postings)
                   if p.account == self.account and
                   (p.units is None or p.units is MISSING or
                    p.units.number is MISSING)]
        if not indices:
            return {}
        # Infer identity without changing the import or the journal objects used
        # by invalid-reference diagnostics. Keep the original posting structure.
        entry_copy = deepcopy(entry)
        entry_copy.postings[:] = [
            p._replace(meta={}) if p.meta is None else p
            for p in entry_copy.postings]
        booked, errors = booking.book([entry_copy], options_map)
        if errors:
            return {}
        for i in indices:
            entry_copy.postings[i] = entry_copy.postings[i]._replace(units=MISSING)
        partial, = get_partially_booked_entries([entry_copy], booked)
        return {id(entry.postings[i]): partial.postings[i].units
                for i in indices
                if partial.postings[i].units is not None and
                partial.postings[i].units is not MISSING and
                partial.postings[i].units.number is not MISSING}

    def _get_keys_from_imported_entry(self, entry: Directive,
                                     options_map: Options) -> List[Hashable]:
        if isinstance(entry, Balance):
            return [(entry.account, entry.date, entry.amount)]
        if not isinstance(entry, Transaction):
            raise ValueError("currently, ImporterSource only supports Transaction and Balance Directive. Got entry {}".format(entry))
        postings = [p for p in entry.postings if p.account == self.account]
        if not postings:
            raise ValueError("entry {} has no postings for account: {}".format(entry, self.account))
        inferred = self._get_inferred_source_units(entry, options_map)
        return [(self.account,
                 (p.meta or {}).get('date', entry.date),
                 inferred.get(id(p), p.units),
                 (p.meta or {}).get('source_desc', entry.narration))
                for p in postings]

    def _make_import_result(self, imported_entry: Directive,
                            options_map: Options) -> ImportResult:
        if isinstance(imported_entry, Transaction):
            balance_amounts(imported_entry, options_map)
        result = ImportResult(
            date=imported_entry.date, info=get_info(imported_entry), entries=[imported_entry])
        # delete filename since it is used by beancount-import to determine if the
        # entry is from journal.
        imported_entry.meta.pop('filename')
        return result


def get_info(raw_entry: Directive) -> dict:
    if raw_entry.meta["filename"].endswith(".beancount"):
        ftype = "text/plain"
    else:
        ftype = get_file(raw_entry.meta['filename']).mimetype()
    return dict(
        type=ftype,
        filename=raw_entry.meta['filename'],
        line=raw_entry.meta['lineno'],
    )

def balance_amounts(txn: Transaction, options_map: Options) -> None:
    """Add FIXME postings for residuals outside the journal's tolerances."""
    if any(posting.units is None or posting.units is MISSING or
           posting.units.number is MISSING for posting in txn.postings):
        # Let Beancount interpolate incomplete postings when the entry is booked.
        return
    inventory = SimpleInventory()
    for posting in txn.postings:
        inventory += get_weight(convert_costspec_to_cost(posting))
    tolerances = interpolate.infer_tolerances(txn.postings, options_map)
    for currency, number in inventory.items():
        if abs(number) <= tolerances[currency]:
            continue
        txn.postings.append(
            Posting(
                account=FIXME_ACCOUNT,
                units=Amount(currency=currency, number=-number),
                cost=None,
                price=None,
                flag=None,
                meta={},
            ))


def load(spec, log_status):
    return ImporterSource(log_status=log_status, **spec)
