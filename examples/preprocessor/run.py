#!/usr/bin/env python3
"""Record a fictional grocery payment and leave other imports for review."""

import os
import sys

from beancount_import.preprocessor import (
    BasePreprocessor, PreprocessingRule, TransactionRuleMixin)
import beancount_import.webserver


class Rules(TransactionRuleMixin, BasePreprocessor):
    def preprocess(self) -> None:
        self.apply_rules([
            PreprocessingRule(
                source_name='mint',
                posting_meta={'source_desc': 'EXAMPLE MARKET'},
                target_account='Expenses:Groceries',
                action='record',
            ),
        ])


def run(extra_args):
    directory = os.path.dirname(__file__)
    beancount_import.webserver.main(
        extra_args,
        journal_input=os.path.join(directory, 'journal.beancount'),
        ignored_journal=os.path.join(directory, 'ignored.beancount'),
        default_output=os.path.join(directory, 'journal.beancount'),
        data_sources=[dict(
            module='beancount_import.source.mint',
            filename=os.path.join(directory, 'transactions.csv'))],
        preprocessor=Rules,
    )


if __name__ == '__main__':
    run(sys.argv[1:])
