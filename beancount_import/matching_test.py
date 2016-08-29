#!/usr/bin/env python3

import os
import unittest

import beancount.parser.parser
import beancount.parser.printer
from . import matching
from . import test_util


def add_entries_to_db(posting_db: matching.PostingDatabase, entries):
    for entry in entries:
        posting_db.add_transaction(entry)


testdata_dir = os.path.join(
    os.path.dirname(__file__), '..', 'testdata', 'matching_test')


def load_match_test_data(name, key):
    path = os.path.join(testdata_dir, name, key + '.beancount')
    if os.path.exists(path):
        with open(path, 'r') as f:
            return f.read()
    return ''


def assert_match(pending_candidate: str = '',
                 journal_candidate: str = '',
                 pending: str = '',
                 journal: str = '',
                 matches: str = ''):

    assert (not pending_candidate) != (not journal_candidate)

    candidate_entry, = test_util.parse(pending_candidate or journal_candidate)
    pending_entries = test_util.parse(pending)
    journal_entries = test_util.parse(journal)
    expected_match_entries = test_util.parse(matches)

    for entry in pending_entries:
        del entry.meta['filename']

    if pending_candidate:
        del candidate_entry.meta['filename']

    def is_cleared(posting):
        return posting.meta and posting.meta.get('cleared') == True

    posting_db = matching.PostingDatabase(
        fuzzy_match_days=3,
        is_cleared=is_cleared,
        metadata_keys=frozenset([matching.CHECK_KEY]),
    )
    add_entries_to_db(posting_db, pending_entries)
    add_entries_to_db(posting_db, journal_entries)
    add_entries_to_db(posting_db, [candidate_entry])

    results = [
        txn for txn, used_transactions in matching.get_extended_transactions(
            candidate_entry, posting_db)
    ]
    expected_match_entries = test_util.format_entries(expected_match_entries)
    results = test_util.format_entries(results)
    if results != expected_match_entries:
        print(results)
    assert results == expected_match_entries


def assert_file_match(name):
    assert_match(
        **{
            key: load_match_test_data(name, key)
            for key in ('pending_candidate', 'journal_candidate', 'pending',
                        'journal', 'matches')
        })


def test_cleared_matches_not_cleared():
    # This case corresponds to a transfer between two bank accounts: the transactions created
    # from each bank statement are duplicates and should be matched.
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
          Assets:B 1 USD
            note1: "B"
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            note2: "A"
          Assets:B 1 USD
            cleared: TRUE
            note2: "B"
        """,
        matches="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
            note2: "A"
          Assets:B 1 USD
            cleared: TRUE
            note1: "B"
            note2: "B"
        """)

def test_check_match():
    # This case corresponds to a transfer between two bank accounts: the transactions created
    # from each bank statement are duplicates and should be matched.
    assert_match(
        pending_candidate="""
        2016-03-01 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            check: 5
            note1: "A"
          Assets:B 1 USD
            note1: "B"
        """,
        journal="""
        2016-01-01 * "Wrote check"
          Assets:A  -1 USD
            check: 5
            note2: "A"
          Assets:B 1 USD
            note2: "B"
        """,
        matches="""
        2016-01-01 * "Wrote check"
          Assets:A  -1 USD
            cleared: TRUE
            check: 5
            note1: "A"
            note2: "A"
          Assets:B 1 USD
            note1: "B"
            note2: "B"
        """)


def test_cleared_matches_not_cleared_fuzzy():
    # Same as above, a transfer between two bank accounts, but where the dates don't match
    # perfectly.
    assert_match(
        pending_candidate="""
        2016-01-04 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
          Assets:B 1 USD
            note1: "B"
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            note2: "A"
          Assets:B 1 USD
            cleared: TRUE
            note2: "B"
        """,
        matches="""
        2016-01-04 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
            note2: "A"
          Assets:B 1 USD
            cleared: TRUE
            note1: "B"
            note2: "B"
        """)


def test_fail_cleared_matches_not_cleared_fuzzy():
    # Same as above, a transfer between two bank accounts, but where the date difference exceeds
    # fuzzy_match_days.
    assert_match(
        pending_candidate="""
        2016-01-05 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
          Assets:B 1 USD
            note1: "B"
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            note2: "A"
          Assets:B 1 USD
            cleared: TRUE
            note2: "B"
        """)


def xxtest_fail_not_cleared_matches_not_cleared():
    # This tests that two uncleared postings in two different pending transactions cannot be
    # matched together.
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
          Assets:B 1 USD
            note1: "B"
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            note2: "A"
          Assets:B 1 USD
            note2: "B"
        """)


def test_not_cleared_journal_matches_not_cleared_pending():
    # This case corresponds to entering a purchase manually in the journal, then reconciling it
    # with a transaction produced from a bank statement.  The transactions are duplicates and
    # should be merged.
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
          Expenses:B 1 USD
            note1: "B"
        """,
        journal="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            note2: "A"
          Expenses:B 1 USD
            note2: "B"
        """,
        matches="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
            note1: "A"
            note2: "A"
          Expenses:B 1 USD
            note1: "B"
            note2: "B"
        """)


def test_unknown_matches_negated_unknown():
    # This corresponds to two partial transactions.
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Income:A  -1 USD
            note1: "A"
          Income:B  -9 USD
            note1: "B"
          Expenses:FIXME 10 USD
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:A  5 USD
            note2: "A"
          Assets:B  5 USD
            note2: "B"
          Expenses:FIXME -10 USD
        """,
        matches="""
        2016-01-01 * "Narration"
          Income:A  -1 USD
            note1: "A"
          Income:B  -9 USD
            note1: "B"
          Assets:A  5 USD
            note2: "A"
          Assets:B  5 USD
            note2: "B"
        """)


def test_partial_match():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Income:RSU  -9 USD
            note1: "A"
          Assets:Cash  7 USD
            note1: "B"
          Expenses:Taxes:A  1 USD
            note1: "C"
          Expenses:Taxes:B  1 USD
            note1: "D"
        """,
        pending="""
        2016-01-01 * "Narration"
          Income:RSU  -9 USD
            cleared: TRUE
            note2: "A"
          Assets:Cash  7 USD
            cleared: TRUE
            note2: "B"
          Expenses:FIXME 2 USD
          Assets:Cash  -4 USD
            cleared: TRUE
            note2: "D"
          Assets:Stock  2 STOCK {2 USD}
            cleared: TRUE
            note2: "E"
        """,
        matches="""
        2016-01-01 * "Narration"
          Income:RSU  -9 USD
            cleared: TRUE
            note1: "A"
            note2: "A"
          Assets:Cash  7 USD
            cleared: TRUE
            note1: "B"
            note2: "B"
          Expenses:Taxes:A  1 USD
            note1: "C"
          Expenses:Taxes:B  1 USD
            note1: "D"
          Assets:Cash  -4 USD
            cleared: TRUE
            note2: "D"
          Assets:Stock  2 STOCK {2 USD}
            cleared: TRUE
            note2: "E"
        """)


def test_partial_match_two_removals_same_sign():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Income:RSU  -9 USD
            note1: "A"
          Expenses:FIXME  7 USD
          Expenses:Taxes:A  1 USD
            note1: "C"
          Expenses:Taxes:B  1 USD
            note1: "D"
        """,
        pending="""
        2016-01-01 * "Narration"
          Income:RSU  -9 USD
            cleared: TRUE
            note2: "A"
          Expenses:FIXME 2 USD
          Assets:Stock  3 STOCK {2 USD}
            cleared: TRUE
            note2: "C"
          Expenses:Fees 1 USD
            note2: "D"
        """,
        matches="""
        2016-01-01 * "Narration"
          Income:RSU  -9 USD
            cleared: TRUE
            note1: "A"
            note2: "A"
          Expenses:Taxes:A  1 USD
            note1: "C"
          Expenses:Taxes:B  1 USD
            note1: "D"
          Assets:Stock  3 STOCK {2 USD}
            cleared: TRUE
            note2: "C"
          Expenses:Fees 1 USD
            note2: "D"
        """)


def test_match_buy():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:Checking  -400 USD
            note1: "A"
            cleared: TRUE
          Expenses:FIXME   400 USD
            note1: "B"
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:Stock  4 STOCK {100 USD}
            note2: "A"
            cleared: TRUE
          Expenses:FIXME  -400 USD
            note2: "B"
        """,
        matches="""
        2016-01-01 * "Narration"
          Assets:Checking  -400 USD
            note1: "A"
            note2: "B"
            cleared: TRUE
          Assets:Stock  4 STOCK {100 USD}
            note2: "A"
            note1: "B"
            cleared: TRUE
        """)


def test_match_buy_residual():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:Checking  -1200.21 USD
            note1: "A"
            cleared: TRUE
          Expenses:FIXME   1200.21 USD
            note1: "B"
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:Stock  5.838 STOCK {205.59 USD}
            note2: "A"
            cleared: TRUE
          Expenses:FIXME  -1200.21 USD
            note2: "B"
        """,
        matches="""
        2016-01-01 * "Narration"
          Assets:Checking  -1200.21 USD
            note1: "A"
            note2: "B"
            cleared: TRUE
          Assets:Stock  5.838 STOCK {205.59 USD}
            note2: "A"
            note1: "B"
            cleared: TRUE
        """)


def test_match_split_unknown():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:A  -10 USD
            note1: "A"
          Expenses:FIXME:A  8 USD
            note1: "B"
          Expenses:FIXME:A  2 USD
            note1: "C"
        """,
        journal="""
        2016-01-01 * "Narration"
          Assets:A  -10 USD
            note2: "A"
          Expenses:A  10 USD
            note2: "B"
        """,
        matches="""
        2016-01-01 * "Narration"
          Assets:A  -10 USD
            note1: "A"
            note2: "A"
          Expenses:A  8 USD
            note1: "B"
            note2: "B"
          Expenses:A  2 USD
            note1: "C"
            note2: "B"
        """)


def test_match_split_unknown_2():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:A  -23.80 USD
            note1: "A"
          Expenses:FIXME:A  16.84 USD
            note1: "B"
          Expenses:FIXME:A  4.99 USD
            note1: "C"
          Expenses:FIXME:A  1.97 USD
            note1: "D"
        """,
        journal="""
        2016-01-01 * "Narration"
          Assets:A  -23.80 USD
            note2: "A"
          Expenses:A  23.80 USD
            note2: "B"
        """,
        matches="""
        2016-01-01 * "Narration"
          Assets:A  -23.80 USD
            note1: "A"
            note2: "A"
          Expenses:A  16.84 USD
            note1: "B"
            note2: "B"
          Expenses:A  4.99 USD
            note1: "C"
            note2: "B"
          Expenses:A  1.97 USD
            note1: "D"
            note2: "B"
        """)


def test_self_match():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:A -10 USD
          Expenses:FIXME 10 USD
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:A 10 USD
          Expenses:FIXME -10 USD
        """,
        matches="""
        """)


def test_match_merged():
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Expenses:A 10 USD
          Expenses:A 2 USD
          Expenses:B 13 USD
          Expenses:B 3 USD
          Liabilities:A -12 USD
          Liabilities:A -16 USD
        """,
        pending="""
        2016-01-01 * "Narration"
          Liabilities:A -28 USD
            note: "Hello"
            cleared: TRUE
          Expenses:FIXME 28 USD
        """,
        matches="""
        2016-01-01 * "Narration"
          Expenses:A 10 USD
          Expenses:A 2 USD
          Expenses:B 13 USD
          Expenses:B 3 USD
          Liabilities:A -28 USD
            note: "Hello"
            cleared: TRUE
        """)


def test_match_merged2():
    assert_match(
        pending_candidate="""
        2017-03-27 * "Credit card txn"
          Liabilities:A         -431.45 USD
            date: 2017-03-27
            cleared: TRUE
          Expenses:FIXME   431.45 USD
        """,
        journal="""
        2017-03-27 * "Amazon.com" "Order"
          Expenses:X             79.23 USD
          Expenses:X              7.33 USD
          Expenses:X            184.95 USD
          Expenses:X             39.95 USD
          Expenses:X            135.00 USD
          Assets:A           -15.01 USD
          Liabilities:A   -94.31 USD
            transaction_date: 2017-03-29
          Liabilities:A  -161.55 USD
            transaction_date: 2017-03-29
          Liabilities:A   -43.52 USD
            transaction_date: 2017-03-29
          Liabilities:A  -132.07 USD
            transaction_date: 2017-03-29
        """,
        matches="""
        2017-03-27 * "Amazon.com" "Order"
          Expenses:X             79.23 USD
          Expenses:X              7.33 USD
          Expenses:X            184.95 USD
          Expenses:X             39.95 USD
          Expenses:X            135.00 USD
          Assets:A           -15.01 USD
          Liabilities:A   -431.45 USD
            transaction_date: 2017-03-29
            date: 2017-03-27
            cleared: TRUE
        """)


def test_match_merged_fixme():
    assert_match(
        pending_candidate="""
        2017-03-27 * "Credit card txn"
          Liabilities:A         -12.00 USD
            date: 2017-03-27
            cleared: TRUE
          Expenses:FIXME   12.00 USD
        """,
        pending="""
        2017-03-27 * "Amazon.com" "Order"
          Expenses:FIXME:A             5.00 USD
          Expenses:FIXME:A             1.00 USD
          Expenses:FIXME:A             6.00 USD
          Liabilities:A  -12.00 USD
        """,
        matches="""
        2017-03-27 * "Credit card txn"
          Liabilities:A         -12.00 USD
            date: 2017-03-27
            cleared: TRUE
          Expenses:FIXME:A             5.00 USD
          Expenses:FIXME:A             1.00 USD
          Expenses:FIXME:A             6.00 USD
        """)


def test_match_no_delete():
    assert_match(
        pending_candidate="""
        2017-03-27 * "Credit card txn"
          Liabilities:A         -11.99 USD
            date: 2017-03-27
            cleared: TRUE
          Expenses:FIXME   11.99 USD
        """,
        pending="""
        2017-03-27 * "Amazon.com" "Order"
          Expenses:FIXME:A             11.99 USD
            amazon_item_description: "Item"
          Liabilities:A  -11.99 USD
        """,
        matches="""
        2017-03-27 * "Credit card txn"
          Liabilities:A         -11.99 USD
            date: 2017-03-27
            cleared: TRUE
          Expenses:FIXME:A             11.99 USD
            amazon_item_description: "Item"
        """)


def test_match_many_merged():
    # This is an actual Amazon.com transaction consisting of many items
    # purchased on a single order invoice, and 4 different actual credit
    # card transactions corresponding to 6 listed credit card transactions
    # in the order invoice.
    assert_file_match('match_many_merged')


def test_posting_metadata_incompatibility():
    # The incompatible values of `note` prevent a match.
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
            cleared: TRUE
          Assets:B 1 USD
            note: "A"
        """,
        pending="""
        2016-01-01 * "Narration"
          Assets:A  -1 USD
          Assets:B 1 USD
            cleared: TRUE
            note: "B"
        """)


def test_transaction_metadata_incompatibility():
    # The incompatible values of `note` prevent a match.
    assert_match(
        pending_candidate="""
        2016-01-01 * "Narration"
          note: "A"
          Assets:A  -1 USD
            cleared: TRUE
          Assets:B 1 USD
        """,
        pending="""
        2016-01-01 * "Narration"
          note: "B"
          Assets:A  -1 USD
          Assets:B 1 USD
            cleared: TRUE
        """)
