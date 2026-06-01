#!/usr/bin/env python3

import argparse
import sys

from beanquery.query import run_query
from beancount.core.compare import hash_entry
from . import journal_editor


def get_matching_entries(entries, options_map, query):
    """Return the entries matched by a BQL ``FROM`` and/or ``WHERE`` expression.

    Beancount v3 moved the query engine out into the standalone ``beanquery``
    package, whose internal API differs from the old ``beancount.query`` one.
    We run the query through beanquery's public interface, selecting the
    entry ``id`` (which equals ``beancount.core.compare.hash_entry``), then map
    those ids back to the original directives.

    Scope: beanquery selects over the postings table, so only Transaction
    directives are matchable. A FROM clause targeting non-Transaction
    directives (Balance, Note, Open, Price, ...) matches nothing -- consistent
    with this tool's purpose (deleting transactions), but a behaviour change
    from the pre-v3 implementation, which could also delete those directives.
    """
    query_text = 'SELECT id ' + query
    _rtypes, rows = run_query(entries, options_map, query_text)
    matching_ids = {row[0] for row in rows}
    return [entry for entry in entries if hash_entry(entry) in matching_ids]


CHANGE_TYPE_INDICATOR = {0: ' ', -1: '-', 1: '+'}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('journal', help='Path to beancount journal file.')
    ap.add_argument('query', help='Query FROM and/or WHERE expression matching the transactions to delete (only Transaction directives are matched).  See https://docs.google.com/document/d/1s0GOZMcrKKCLlP29MD7kHO4L88evrwWdIO0p4EwRBE0/view')

    args = ap.parse_args()

    editor = journal_editor.JournalEditor(args.journal)
    stage = editor.stage_changes()

    for entry in get_matching_entries(editor.entries, editor.options_map, args.query):
        stage.remove_entry(entry)

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
