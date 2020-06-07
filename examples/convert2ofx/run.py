#!/usr/bin/env python3

import glob
import os
import json
import sys

from beancount_import.source.ofx import convert2ofx

def run_reconcile(extra_args):
    import beancount_import.webserver

    journal_dir = os.path.dirname(__file__)
    data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'icscards')
    force = False

    data_sources = [
        dict(
            module='beancount_import.source.ofx',
            ofx_filenames=(
                convert2ofx('nl-icscards', glob.glob(os.path.join(data_dir, 'icscards.txt')), force)
            ),
            checknum_numeric=lambda ofx_filename: False,
            check_balance=lambda ofx_filename: False,
        ),
    ]

    beancount_import.webserver.main(
        extra_args,
        journal_input=os.path.join(journal_dir, 'journal.beancount'),
        ignored_journal=os.path.join(journal_dir, 'ignored.beancount'),
        default_output=os.path.join(journal_dir, 'transactions.beancount'),
        open_account_output_map=[
            ('.*', os.path.join(journal_dir, 'accounts.beancount')),
        ],
        balance_account_output_map=[
            ('.*', os.path.join(journal_dir, 'accounts.beancount')),
        ],
        price_output=os.path.join(journal_dir, 'prices.beancount'),
        data_sources=data_sources,
    )


if __name__ == '__main__':
    run_reconcile(sys.argv[1:])
