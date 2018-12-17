"""Base class for sources with a transaction link that specifies a unique id."""

from typing import Dict, List, Any, Optional, FrozenSet, Union, Set

from beancount.core.data import Open, Transaction, Posting, Amount, Pad, Balance, Entries, Directive
from . import ImportResult, SourceResults, Source, InvalidSourceReference, AssociatedData


class LinkBasedSource:
    def __init__(self, link_prefix: str, **kwargs):
        super().__init__(**kwargs)  # type: ignore
        self.link_prefix = link_prefix

    def get_entries_with_link(
            self,
            all_entries: Entries,
            valid_links: Union[Set[str], FrozenSet[str]],
            results: SourceResults,
    ) -> Dict[str, List[Transaction]]:
        link_prefix = self.link_prefix
        seen_entries = dict()  # type: Dict[str, Entries]
        for entry in all_entries:
            if not isinstance(entry, Transaction): continue
            for link in entry.links:
                if not link.startswith(link_prefix): continue
                txn_id = link[len(link_prefix):]
                seen_entries.setdefault(txn_id, []).append(entry)
        for txn_id, entries in seen_entries.items():
            expected_count = 1 if txn_id in valid_links else 0
            if len(entries) == expected_count: continue
            results.add_invalid_reference(
                InvalidSourceReference(
                    num_extras=len(entries) - expected_count,
                    transaction_posting_pairs=[(t, None) for t in entries]))
        return seen_entries

    def get_associated_data_for_link(
            self, entry_id: str) -> Optional[List[AssociatedData]]:
        return None

    def get_associated_data(self,
                            entry: Directive) -> Optional[List[AssociatedData]]:
        if not isinstance(entry, Transaction): return None
        link_prefix = self.link_prefix
        associated_data = []  # type: List[AssociatedData]
        for link in entry.links:
            if link.startswith(link_prefix):
                txn_id = link[len(link_prefix):]
                cur_results = self.get_associated_data_for_link(txn_id)
                if cur_results is not None:
                    for x in cur_results:
                        x.link = link
                    associated_data.extend(cur_results)
        return associated_data
