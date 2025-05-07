#!/usr/bin/env python3

import glob
import os
import json
import sys

from config import my_foobar_bank_importer, my_amex_cc_importer, foobar_email_importer


def run_reconcile(extra_args):
    import beancount_import.webserver

    journal_dir = os.path.dirname(__file__)
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    data_sources = [
        dict(
            module="beancount_import.source.generic_importer_source",
            # imports monthly bank statements
            importer=my_foobar_bank_importer,
            account="Assets:FooBarBank",
            directory=os.path.join(data_dir, "importers"),
        ),
        dict(
            module="beancount_import.source.generic_importer_source",
            # imports individual transactions from email
            importer=foobar_email_importer,
            # this importer just imports transactions from email
            # but does not clear the postings, hence account=None
            # note than the importer just above this one clears the postings
            # imported by this importer
            account=None,
            directory=os.path.join(data_dir, "importers"),
        ),
        dict(
            module="beancount_import.source.generic_importer_source",
            # imports monthly credit card statements
            importer=my_amex_cc_importer,
            account="Liabilities:Amex-Credit-Card",
            directory=os.path.join(data_dir, "importers"),
        ),
    ]

    beancount_import.webserver.main(
        extra_args,
        journal_input=os.path.join(journal_dir, "journal.beancount"),
        ignored_journal=os.path.join(journal_dir, "ignored.beancount"),
        default_output=os.path.join(journal_dir, "transactions.beancount"),
        open_account_output_map=[
            (".*", os.path.join(journal_dir, "accounts.beancount")),
        ],
        balance_account_output_map=[
            (".*", os.path.join(journal_dir, "accounts.beancount")),
        ],
        price_output=os.path.join(journal_dir, "prices.beancount"),
        data_sources=data_sources,
    )


if __name__ == "__main__":
    run_reconcile(sys.argv[1:])
