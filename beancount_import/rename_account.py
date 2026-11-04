#!/usr/bin/env python3

import argparse
import sys

from beanquery.query import run_query
from beancount.core.compare import hash_entry
from beancount.core.data import Transaction
from beancount.core.position import Position
from . import journal_editor


def get_matching_postings(entries, options_map, query):
    """Yield ``(entry, matching_postings)`` for a BQL FROM/WHERE expression.

    Beancount v3 moved the query engine out into the standalone ``beanquery``
    package. beanquery's row API does not hand back the original ``Posting``
    objects, so we select the columns that uniquely identify a matched posting
    within its entry -- ``id`` (== ``hash_entry``), ``account``, ``position``,
    ``price`` and ``posting_flag`` -- and map those keys back to the actual
    ``Posting`` objects. Note beanquery's ``flag`` column is the transaction
    flag; the posting flag is exposed separately as ``posting_flag``.

    When the query has no WHERE clause, beanquery emits a row for every posting
    of each matched entry, so all of an entry's postings are returned, matching
    the previous behaviour.

    Limitation: the match key does not include posting metadata, so a WHERE
    clause that distinguishes two otherwise-identical sibling postings (same
    account, units, cost, price and flag) purely by ``meta[...]`` will select
    both -- a contrived case.
    """
    query_text = 'SELECT id, account, position, price, posting_flag ' + query
    _rtypes, rows = run_query(entries, options_map, query_text)
    matched = {(entry_id, account, str(position), str(price), str(flag))
               for entry_id, account, position, price, flag in rows}

    for entry in entries:
        if not isinstance(entry, Transaction):
            continue
        entry_id = hash_entry(entry)
        matching_postings = [
            posting for posting in entry.postings
            if (entry_id, posting.account,
                str(Position(posting.units, posting.cost)),
                str(posting.price), str(posting.flag)) in matched
        ]
        if matching_postings:
            yield (entry, matching_postings)


CHANGE_TYPE_INDICATOR = {0: ' ', -1: '-', 1: '+'}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('journal', help='Path to beancount journal file.')
    ap.add_argument('query', help='Query FROM and/or WHERE expression.  See https://docs.google.com/document/d/1s0GOZMcrKKCLlP29MD7kHO4L88evrwWdIO0p4EwRBE0/view')
    ap.add_argument('-a', '--new-account', help='Replacement account name for matching postings.')
    ap.add_argument('-e', '--eval', help='Python expression that modifies posting.')

    args = ap.parse_args()

    editor = journal_editor.JournalEditor(args.journal)
    stage = editor.stage_changes()
    new_account = args.new_account
    eval_expr = args.eval

    for entry, postings in get_matching_postings(editor.entries, editor.options_map, args.query):
        new_postings = []
        for p in entry.postings:
            if p in postings:
                if new_account:
                    p = p._replace(account=new_account)
                if eval_expr:
                    p = p._replace(meta=p.meta.copy() if p.meta else None)
                    local_vars = {'posting': p}
                    p = eval(eval_expr, globals(), local_vars)
                    #p = local_vars['posting']
            new_postings.append(p)
        stage.change_entry(entry, entry._replace(postings=new_postings))

    change_sets, old_entries, new_entries = stage.get_diff()
    for filename, file_change_sets in change_sets:
        print(filename)
        for line_range, line_changes in file_change_sets:
            for change_type, line in line_changes:
                print('%s%s' % (CHANGE_TYPE_INDICATOR[change_type], line))

    sys.stdout.write('Continue with change? [yes] (control-c to cancel)')
    result = input().lower()

    if result not in ['', 'yes', 'y']:
        sys.exit(1)

    stage.apply()
    sys.exit(0)

if __name__ == '__main__':
    main()
