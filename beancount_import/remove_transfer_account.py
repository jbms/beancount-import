"""Interactive tool for removing an account that was used for pending transfers."""

from typing import List, Set, Tuple, NamedTuple, Union, Dict
import argparse
import datetime

from beancount.core.data import Transaction, Posting, Directive, Entries
from beancount.core.number import MISSING, ZERO
from beancount.core.amount import Amount
import beancount.parser.printer

from . import journal_editor
from . import matching
from .posting_date import get_posting_date

PendingEntry = NamedTuple('PendingEntry', [
    ('date', datetime.date),
    ('transaction', Transaction),
    ('posting', Posting),
])


def format_transaction(transaction: Transaction) -> str:
    printer = beancount.parser.printer.EntryPrinter()
    return printer(transaction)


CHANGE_TYPE_INDICATOR = {0: ' ', -1: '-', 1: '+'}


def get_matchable_posting_for_merge(
        x: PendingEntry) -> matching.MatchablePosting:
    if x.posting.units.number < ZERO:
        if len(x.transaction.postings) == 2:
            for p in x.transaction.postings:
                if id(p) != id(x.posting):
                    posting = p
                    break
        else:
            posting = Posting(
                account=x.posting.account,
                units=-x.posting.units,
                cost=None,
                price=None,
                flag=None,
                meta=None)
        return matching.MatchablePosting(
            posting=posting,
            weight=matching.get_posting_weight(posting),
            source_postings=[
                p for p in x.transaction.postings if id(p) != id(x.posting)
            ],
        )
    return matching.MatchablePosting(
        posting=x.posting,
        weight=matching.get_posting_weight(x.posting),
        source_postings=[x.posting],
    )


def delete_posting_account(pending_entry: PendingEntry) -> PendingEntry:
    meta = pending_entry.posting.meta.copy()
    for k in list(meta.keys()):
        if k.startswith('ofx_'):
            del meta[k]
    new_posting = pending_entry.posting._replace(
        account=matching.FIXME_ACCOUNT, meta=meta)
    new_transaction = pending_entry.transaction._replace(postings=[
        new_posting if id(p) == id(pending_entry.posting) else p
        for p in pending_entry.transaction.postings
    ])
    return pending_entry._replace(
        posting=new_posting, transaction=new_transaction)


def process(journal_path: str,
            account: str,
            max_day_offset: int = 30):
    editor = journal_editor.JournalEditor(journal_path)

    postings_by_units = dict(
    )  # type: Dict[Amount, List[PendingEntry]]

    pending_entries = []  # type: List[PendingEntry]

    def add_directive(entry: Directive):
        if not isinstance(entry, Transaction): return
        for posting in entry.postings:
            if posting.account != account: continue
            if posting.units is MISSING or posting.units is None: continue
            date = get_posting_date(entry, posting)
            pending_entry = PendingEntry(date, entry, posting)
            pending_entries.append(pending_entry)
            postings_by_units.setdefault(posting.units,
                                         []).append(pending_entry)

    removed_transactions = set()  # type: Set[int]
    removed_postings = set()  # type: Set[int]

    def remove_directive(entry: Directive):
        if not isinstance(entry, Transaction): return
        removed_transactions.add(id(entry))
        for posting in entry.postings:
            if posting.account != account: continue
            removed_postings.add(id(posting))

    for entry in editor.entries:
        add_directive(entry)

    pending_entries.sort(key=lambda x: x.date)

    for pending in pending_entries:
        if id(pending.transaction) in removed_transactions: continue
        if id(pending.posting) in removed_postings: continue
        possible = [
            x for x in postings_by_units.get(-pending.posting.units, [])
            if (id(x.transaction) not in removed_transactions and
                id(x.posting) not in removed_postings and
                abs(x.date - pending.date).days < max_day_offset)
        ]
        possible.sort(key=lambda x: abs(x.date - pending.date))

        candidates = []  # type: List[journal_editor.StagedChanges]
        for x in possible:
            stage = editor.stage_changes()
            a = pending
            b = x
            if (len(b.transaction.postings) > len(a.transaction.postings) or
                (len(b.transaction.postings) == len(a.transaction.postings) and
                 len(b.transaction.narration) > len(a.transaction.narration))):
                a, b = b, a
            a_modified = delete_posting_account(a)
            b_modified = delete_posting_account(b)

            removals = []
            for x in a_modified, b_modified:
                if x.posting.units.number < ZERO:
                    removals.append(
                        matching.MatchablePosting(
                            x.posting,
                            weight=matching.get_posting_weight(x.posting),
                            source_postings=[x.posting]))

            merged_transaction = matching.combine_transactions_using_match_set(
                (a_modified.transaction, b_modified.transaction),
                match_set=matching.PostingMatchSet(
                    matches=[
                        (get_matchable_posting_for_merge(a_modified),
                         get_matchable_posting_for_merge(b_modified)),
                    ],
                    removals=removals),
                is_cleared=lambda x: False,
            )
            merged_transaction = matching.normalize_transaction(
                merged_transaction)
            stage.change_entry(a.transaction, merged_transaction)
            stage.remove_entry(b.transaction)
            candidates.append(stage)

        print('TRANSACTION')
        print(format_transaction(pending.transaction))
        print()

        print('Found %d matches' % len(candidates))

        for i, candidate in enumerate(candidates):
            print('\nCANDIATE %d' % (i + 1))
            for change_type, line in candidate.get_combined_changes():
                print('  %s%s' % (CHANGE_TYPE_INDICATOR[change_type], line))

        print('\n')
        default_choice = min(1, len(candidates))
        raw_choice = input('Choose candidates [%d]: ' % default_choice)
        raw_choice = raw_choice.strip()
        if len(raw_choice) == 0:
            choice = default_choice
        else:
            choice = int(choice)
        if choice == 0:
            continue
        candidate = candidates[choice - 1]
        _, old_entries, new_entries = candidate.get_diff()
        candidate.apply()
        for entry in old_entries:
            remove_directive(entry)
        for entry in new_entries:
            add_directive(entry)
        pending_entries.sort(key=lambda x: x.date)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('journal', help='Path to beancount journal file.')
    ap.add_argument('account', help='Transfer account to remove.')
    ap.add_argument(
        '--max-day-offset',
        type=int,
        default=30,
        help='Maximum number of days over which matches are permitted.')
    args = ap.parse_args()
    process(
        args.journal,
        args.account,
        max_day_offset=args.max_day_offset)


if __name__ == '__main__':
    main()
