import os

from .source_test import check_source, import_result
from ..training import PredictionInput
import datetime
from beancount.core.amount import Amount
from beancount.core.number import D

testdata_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'mint'))

mint_filename = os.path.join(testdata_dir, 'mint.csv')

def test_basic(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.mint',
            'filename': mint_filename,
        },
        journal_contents=r"""
        1900-01-01 open Liabilities:Credit-Card  USD
          mint_id: "My Credit Card"

        1900-01-01 open Assets:Checking  USD
          mint_id: "My Checking"
        """,
        accounts=frozenset([
            'Assets:Checking',
            'Liabilities:Credit-Card',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': mint_filename,
                    'line': 2
                },
                entries=r"""
                2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
                  Liabilities:Credit-Card   66.88 USD
                    date: 2013-11-27
                    source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
                  Expenses:FIXME           -66.88 USD
                
                """,
                unknown_account_prediction_inputs=[
                    PredictionInput(
                        source_account='Liabilities:Credit-Card',
                        amount=Amount(D('66.88'), 'USD'),
                        date=datetime.date(2013, 11, 27),
                        key_value_pairs={
                            'desc': 'CR CARD PAYMENT ALEXANDRIA VA'
                        })
                ],
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': mint_filename,
                    'line': 3
                },
                entries=r"""
                2013-12-02 * "NATIONAL FEDERAL DES:TRNSFR"
                  Assets:Checking  -66.88 USD
                    date: 2013-12-02
                    source_desc: "NATIONAL FEDERAL DES:TRNSFR"
                  Expenses:FIXME    66.88 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': mint_filename,
                    'line': 1
                },
                entries=r"""
                2016-08-10 * "STARBUCKS STORE 12345"
                  Liabilities:Credit-Card  -2.45 USD
                    date: 2016-08-10
                    source_desc: "STARBUCKS STORE 12345"
                  Expenses:FIXME            2.45 USD
                
                """,
            ),
        ],
    )

def test_training_examples(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.mint',
            'filename': mint_filename,
        },
        journal_contents=r"""
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Liabilities:Credit-Card  USD
          mint_id: "My Credit Card"

        1900-01-01 open Assets:Checking  USD
          mint_id: "My Checking"

        2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
          Liabilities:Credit-Card   66.88 USD
            date: 2013-11-27
            source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
            cleared: TRUE
          Assets:Checking  -66.88 USD
            date: 2013-12-02
            source_desc: "NATIONAL FEDERAL DES:TRNSFR"
            cleared: TRUE

        2016-08-10 * "STARBUCKS STORE 12345"
          Liabilities:Credit-Card  -2.45 USD
            date: 2016-08-10
            source_desc: "STARBUCKS STORE 12345"
            cleared: TRUE
          Expenses:Coffee            2.45 USD
        """,
        accounts=frozenset([
            'Assets:Checking',
            'Liabilities:Credit-Card',
        ]),
        pending=[],
        training_examples=[
            (PredictionInput(
                source_account='Liabilities:Credit-Card',
                amount=Amount(D('66.88'), 'USD'),
                date=datetime.date(2013, 11, 27),
                key_value_pairs={'desc': 'CR CARD PAYMENT ALEXANDRIA VA'}),
             'Assets:Checking'),
            (PredictionInput(
                source_account='Assets:Checking',
                amount=Amount(D('-66.88'), 'USD'),
                date=datetime.date(2013, 12, 2),
                key_value_pairs={'desc': 'NATIONAL FEDERAL DES:TRNSFR'}),
             'Liabilities:Credit-Card'),
            (PredictionInput(
                source_account='Liabilities:Credit-Card',
                amount=Amount(D('-2.45'), 'USD'),
                date=datetime.date(2016, 8, 10),
                key_value_pairs={'desc': 'STARBUCKS STORE 12345'}),
             'Expenses:Coffee'),
        ],
    )

def test_invalid(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.mint',
            'filename': mint_filename,
        },
        journal_contents=r"""
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Liabilities:Credit-Card  USD
          mint_id: "My Credit Card"

        1900-01-01 open Assets:Checking  USD
          mint_id: "My Checking"

        2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
          invalid_id: "a"
          Liabilities:Credit-Card   66.88 USD
            invalid_id: "p"
            date: 2013-11-27
            source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
            cleared: TRUE
          Assets:Checking  -66.88 USD
            date: 2013-12-02
            source_desc: "NATIONAL FEDERAL DES:TRNSFR"
            cleared: TRUE

        2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
          invalid_id: "b"
          Liabilities:Credit-Card   66.88 USD
            invalid_id: "p"
            date: 2013-11-27
            source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
            cleared: TRUE
          Expenses:FIXME  -66.88 USD

        2016-08-10 * "STARBUCKS STORE 12345"
          invalid_id: "c"
          Liabilities:Credit-Card  -2.45 USD
            invalid_id: "p"
            date: 2016-08-10
            source_desc: "STARBUCKS STORE 12345"
            cleared: TRUE
          Expenses:Coffee            2.45 USD

        2016-08-10 * "STARBUCKS STORE 12345"
          invalid_id: "d"
          Liabilities:Credit-Card  -2.45 USD
            invalid_id: "p"
            date: 2016-08-10
            source_desc: "STARBUCKS STORE 12345"
            cleared: TRUE
          Expenses:Coffee            2.45 USD

        2016-08-10 * "STARBUCKS STORE 12345"
          invalid_id: "e"
          Liabilities:Credit-Card  -2.45 USD
            invalid_id: "p"
            date: 2016-08-10
            source_desc: "STARBUCKS STORE 12345"
            cleared: TRUE
          Expenses:Coffee            2.45 USD
        """,
        accounts=frozenset([
            'Assets:Checking',
            'Liabilities:Credit-Card',
        ]),
        pending=[],
        invalid_references=[
            (1, [('a', 'p'), ('b', 'p')]),
            (2, [('c', 'p'), ('d', 'p'), ('e', 'p')]),
        ],
    )
