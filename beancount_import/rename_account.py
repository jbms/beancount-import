#!/usr/bin/env python3

import argparse
import sys

from beancount.query import (query_compile, query_env, query_execute, query_parser)
from beancount.core import inventory
from beancount.core.data import Transaction
from beancount.parser import options
from beancount.core import (getters, prices)
from . import journal_editor
import pdb


def get_matching_postings(entries, options_map, query):
    query_text = 'SELECT * ' + query
    parser = query_parser.Parser()
    parsed_query = parser.parse(query_text)
    c_from = None
    if parsed_query.from_clause:
        c_from = query_compile.compile_from(parsed_query.from_clause, query_env.FilterEntriesEnvironment())
    c_where = None
    if parsed_query.where_clause:
        c_where = query_compile.compile_expression(parsed_query.where_clause, query_env.FilterPostingsEnvironment())

    # Figure out if we need to compute balance.
    balance = None
    if c_where and query_execute.uses_balance_column(c_where):
        balance = inventory.Inventory()

    context = query_execute.RowContext()
    context.balance = balance
    

    # Initialize some global properties for use by some of the accessors.
    context.options_map = options_map
    context.account_types = options.get_account_types(options_map)
    context.open_close_map = getters.get_account_open_close(entries)
    #context.commodity_map = getters.get_commodity_map(entries)
    context.price_map = prices.build_price_map(entries) 

    if c_from is not None:
        filtered_entries = query_execute.filter_entries(c_from, entries, options_map)
    else:
        filtered_entries = entries
    for entry in filtered_entries:
        if isinstance(entry, Transaction):
            context.entry = entry
            matching_postings = []
            for posting in entry.postings:
                context.posting = posting
                if c_where is None or c_where(context):
                    matching_postings.append(posting)
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
