#!/usr/bin/env python3
"""Handle routine imports with rules; leave everything else for review."""

import os
import re
import sys

from beancount_import.preprocessor import (
    BasePreprocessor, PreprocessingRule, TransactionRuleMixin)
import beancount_import.webserver

# Rules are tried in order and the first match wins. A rule only applies to a
# pending transaction with two postings, one of them `Expenses:FIXME`.
RULES = [
    # record (the default): write to the journal now, with no review.
    PreprocessingRule(
        transaction_narration=re.compile('EXAMPLE MARKET'),
        target_account='Expenses:Groceries',
    ),
    # modify: fill in the account, but keep the entry in the web UI for review.
    PreprocessingRule(
        transaction_narration=re.compile('EXAMPLE CAFE'),
        target_account='Expenses:Dining',
        action='modify',
    ),
    # ignore: move the entry to the ignore journal so it is never offered
    # again. All criteria must match; amounts compare as text, sign included.
    PreprocessingRule(
        source_name='mint',
        transaction_narration=re.compile('CARD VERIFICATION'),
        posting_amount='-1.00 USD',
        action='ignore',
    ),
]


class Rules(TransactionRuleMixin, BasePreprocessor):
    def preprocess(self) -> None:
        self.apply_rules(RULES)


def options(directory):
    return dict(
        journal_input=os.path.join(directory, 'journal.beancount'),
        ignored_journal=os.path.join(directory, 'ignored.beancount'),
        default_output=os.path.join(directory, 'journal.beancount'),
        data_sources=[dict(
            module='beancount_import.source.mint',
            filename=os.path.join(directory, 'transactions.csv'))],
        preprocessor=Rules,
    )


def run(extra_args):
    directory = os.path.dirname(os.path.abspath(__file__))
    beancount_import.webserver.main(extra_args, **options(directory))


if __name__ == '__main__':
    run(sys.argv[1:])
