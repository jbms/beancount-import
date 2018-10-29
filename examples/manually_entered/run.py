#!/usr/bin/env python3

import glob
import os
import json
import sys


def run_reconcile(extra_args):
    import beancount_import.webserver

    journal_dir = os.path.dirname(__file__)
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')

    data_sources = [
        dict(
            module='beancount_import.source.mint',
            filename=os.path.join(data_dir, 'mint/mint.csv'),
        ),
        dict(
            module='beancount_import.source.ofx',
            ofx_filenames=[os.path.join(data_dir, 'ofx/checking2.ofx'),
            ],
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
