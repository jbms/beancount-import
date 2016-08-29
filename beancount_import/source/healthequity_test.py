import os
import datetime

import pytest
from beancount.core.amount import Amount
from beancount.core.number import D

from .source_test import check_source, import_result
from ..training import PredictionInput

testdata_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source',
        'healthequity'))


def test_basic(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.healthequity',
            'directory': testdata_dir,
        },
        journal_contents=r'''
        2016-01-01 open Assets:HSA:HealthEquity
          healthequity_account_id: "1234567"
          dividend_account: "Income:HealthEquity:Dividends"
          capital_gains_account: "Income:HealthEquity:Capital-Gains"
        ''',
        accounts=frozenset([
            'Assets:HSA:HealthEquity:VIIIX',
            'Assets:HSA:HealthEquity:Cash',
            'Assets:HSA:HealthEquity',
        ]),
        pending=[
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-contribution.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-15 * "Employer Contribution (Tax year: 2016)"
                  Assets:HSA:HealthEquity:Cash   800.00 USD
                    date: 2016-01-15
                    healthequity_transaction_type: "Contribution"
                    source_desc: "Employer Contribution (Tax year: 2016)"
                  Expenses:FIXME                           -800.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-16 * "Investment: VIIIX"
                  Assets:HSA:HealthEquity:Cash  -300.00 USD
                    date: 2016-01-16
                    healthequity_transaction_type: "Other"
                    source_desc: "Investment: VIIIX"
                  Expenses:FIXME                            300.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-contribution.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-16 balance Assets:HSA:HealthEquity:Cash         800.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-17 balance Assets:HSA:HealthEquity:Cash         500.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-19 * "Buy"
                  Assets:HSA:HealthEquity:VIIIX    1.745 VIIIX {171.92 USD}
                    date: 2016-01-19
                    source_desc: "Buy"
                  Assets:HSA:HealthEquity:Cash   -300.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-20 balance Assets:HSA:HealthEquity:VIIIX        1.745 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-01-31 * "Interest for Jan-16"
                  Assets:HSA:HealthEquity:Cash   0.01 USD
                    date: 2016-01-31
                    healthequity_transaction_type: "Other"
                    source_desc: "Interest for Jan-16"
                  Expenses:FIXME                           -0.01 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-02-01 balance Assets:HSA:HealthEquity:Cash         500.01 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-02-29 * "Interest for Feb-16"
                  Assets:HSA:HealthEquity:Cash   0.02 USD
                    date: 2016-02-29
                    healthequity_transaction_type: "Other"
                    source_desc: "Interest for Feb-16"
                  Expenses:FIXME                           -0.02 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-03-01 balance Assets:HSA:HealthEquity:Cash         500.03 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-contribution.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-03-11 * "Employer Contribution (Tax year: 2015)"
                  Assets:HSA:HealthEquity:Cash   1600.00 USD
                    date: 2016-03-11
                    healthequity_transaction_type: "Contribution"
                    source_desc: "Employer Contribution (Tax year: 2015)"
                  Expenses:FIXME                           -1600.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-03-11 * "Investment: VIIIX"
                  Assets:HSA:HealthEquity:Cash  -1600.03 USD
                    date: 2016-03-11
                    healthequity_transaction_type: "Other"
                    source_desc: "Investment: VIIIX"
                  Expenses:FIXME                            1600.03 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-03-11 * "Buy"
                  Assets:HSA:HealthEquity:VIIIX     8.622 VIIIX {185.58 USD}
                    date: 2016-03-11
                    source_desc: "Buy"
                  Assets:HSA:HealthEquity:Cash   -1600.03 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-03-12 balance Assets:HSA:HealthEquity:Cash         500.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-03-12 balance Assets:HSA:HealthEquity:VIIIX        10.367 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-03-16 * "Dividend"
                  Assets:HSA:HealthEquity:VIIIX   0.056 VIIIX {185.71 USD}
                    date: 2016-03-16
                    source_desc: "Dividend"
                  Income:HealthEquity:Dividends:VIIIX       -10.40 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-03-17 balance Assets:HSA:HealthEquity:VIIIX        10.423 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-06-16 * "Dividend"
                  Assets:HSA:HealthEquity:VIIIX  0.051 VIIIX {191.57 USD}
                    date: 2016-06-16
                    source_desc: "Dividend"
                  Income:HealthEquity:Dividends:VIIIX       -9.77 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-06-17 balance Assets:HSA:HealthEquity:VIIIX        10.474 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/2016-08-26T152440-0700.balances.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-08-26 price VIIIX                              199.17 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/2016-08-27T200554-0700.balances.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-08-27 price VIIIX                              198.86 USD
                """,
            )
        ],
    )


def test_matching(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.healthequity',
            'directory': testdata_dir,
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"
        option "infer_tolerance_from_cost" "true"

        2016-01-01 open Assets:HSA:HealthEquity
          healthequity_account_id: "1234567"
          dividend_account: "Income:HealthEquity:Dividends"
          capital_gains_account: "Income:HealthEquity:Capital-Gains"

        2016-01-01 * "Employer Contribution (Tax year: 2016)"
          Assets:HSA:HealthEquity:Cash   800.00 USD
            date: 2016-01-15
            healthequity_transaction_type: "Contribution"
            source_desc: "Employer Contribution (Tax year: 2016)"
            cleared: TRUE
          Expenses:FIXME                           -800.00 USD

        2016-01-16 * "Investment: VIIIX"
          Assets:HSA:HealthEquity:Cash  -300.00 USD
            date: 2016-01-16
            healthequity_transaction_type: "Other"
            source_desc: "Investment: VIIIX"
            cleared: TRUE
          Assets:HSA:HealthEquity:VIIIX    1.745 VIIIX {171.92 USD}
            date: 2016-01-19
            source_desc: "Buy"
            cleared: TRUE

        2016-01-31 * "Interest for Jan-16"
          Assets:HSA:HealthEquity:Cash   0.01 USD
            date: 2016-01-31
            healthequity_transaction_type: "Other"
            source_desc: "Interest for Jan-16"
            cleared: TRUE
          Expenses:FIXME                           -0.01 USD

        2016-02-29 * "Interest for Feb-16"
          Assets:HSA:HealthEquity:Cash   0.02 USD
            date: 2016-02-29
            healthequity_transaction_type: "Other"
            source_desc: "Interest for Feb-16"
            cleared: TRUE
          Expenses:FIXME                           -0.02 USD

        2016-03-11 * "Employer Contribution (Tax year: 2015)"
          Assets:HSA:HealthEquity:Cash   1600.00 USD
            date: 2016-03-11
            healthequity_transaction_type: "Contribution"
            source_desc: "Employer Contribution (Tax year: 2015)"
            cleared: TRUE
          Expenses:FIXME                           -1600.00 USD

        2016-03-11 * "Investment: VIIIX"
          Assets:HSA:HealthEquity:Cash  -1600.03 USD
            date: 2016-03-11
            healthequity_transaction_type: "Other"
            source_desc: "Investment: VIIIX"
            cleared: TRUE
          Assets:HSA:HealthEquity:VIIIX     8.622 VIIIX {185.58 USD}
            date: 2016-03-11
            source_desc: "Buy"
            cleared: TRUE

        2016-03-16 * "Dividend"
          Assets:HSA:HealthEquity:VIIIX   0.056 VIIIX {185.71 USD}
            date: 2016-03-16
            source_desc: "Dividend"
            cleared: TRUE
          Income:HealthEquity:Dividends:VIIIX       -10.40 USD

        2016-06-16 * "Dividend"
          Assets:HSA:HealthEquity:VIIIX  0.051 VIIIX {191.57 USD}
            date: 2016-06-16
            source_desc: "Dividend"
            cleared: TRUE
          Income:HealthEquity:Dividends:VIIIX       -9.77 USD
        ''',
        accounts=frozenset([
            'Assets:HSA:HealthEquity:VIIIX',
            'Assets:HSA:HealthEquity:Cash',
            'Assets:HSA:HealthEquity',
        ]),
        pending=[
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-contribution.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-16 balance Assets:HSA:HealthEquity:Cash         800.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-17 balance Assets:HSA:HealthEquity:Cash         500.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-20 balance Assets:HSA:HealthEquity:VIIIX        1.745 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-02-01 balance Assets:HSA:HealthEquity:Cash         500.01 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-03-01 balance Assets:HSA:HealthEquity:Cash         500.03 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-03-12 balance Assets:HSA:HealthEquity:Cash         500.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-03-12 balance Assets:HSA:HealthEquity:VIIIX        10.367 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-03-17 balance Assets:HSA:HealthEquity:VIIIX        10.423 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-06-17 balance Assets:HSA:HealthEquity:VIIIX        10.474 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/2016-08-26T152440-0700.balances.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-08-26 price VIIIX                              199.17 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/2016-08-27T200554-0700.balances.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-08-27 price VIIIX                              198.86 USD
                """,
            )
        ],
        training_examples=[
            (PredictionInput(
                source_account='Assets:HSA:HealthEquity:Cash',
                amount=Amount(D('-300.00'), 'USD'),
                date=datetime.date(2016, 1, 16),
                key_value_pairs={
                    'desc': 'Investment: VIIIX',
                    'healthequity_transaction_type': 'Other'
                }), 'Assets:HSA:HealthEquity:VIIIX'),
            (PredictionInput(
                source_account='Assets:HSA:HealthEquity:VIIIX',
                amount=Amount(D('1.745'), 'VIIIX'),
                date=datetime.date(2016, 1, 19),
                key_value_pairs={'desc': 'Buy'}),
             'Assets:HSA:HealthEquity:Cash'),
            (PredictionInput(
                source_account='Assets:HSA:HealthEquity:Cash',
                amount=Amount(D('-1600.03'), 'USD'),
                date=datetime.date(2016, 3, 11),
                key_value_pairs={
                    'desc': 'Investment: VIIIX',
                    'healthequity_transaction_type': 'Other'
                }), 'Assets:HSA:HealthEquity:VIIIX'),
            (PredictionInput(
                source_account='Assets:HSA:HealthEquity:VIIIX',
                amount=Amount(D('8.622'), 'VIIIX'),
                date=datetime.date(2016, 3, 11),
                key_value_pairs={'desc': 'Buy'}),
             'Assets:HSA:HealthEquity:Cash'),
            (PredictionInput(
                source_account='Assets:HSA:HealthEquity:VIIIX',
                amount=Amount(D('0.056'), 'VIIIX'),
                date=datetime.date(2016, 3, 16),
                key_value_pairs={'desc': 'Dividend'}),
             'Income:HealthEquity:Dividends:VIIIX'),
            (PredictionInput(
                source_account='Assets:HSA:HealthEquity:VIIIX',
                amount=Amount(D('0.051'), 'VIIIX'),
                date=datetime.date(2016, 6, 16),
                key_value_pairs={'desc': 'Dividend'}),
             'Income:HealthEquity:Dividends:VIIIX'),
        ],
    )


def test_invalid(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.healthequity',
            'directory': testdata_dir,
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"
        option "infer_tolerance_from_cost" "true"

        2016-01-01 open Assets:HSA:HealthEquity
          healthequity_account_id: "1234567"
          dividend_account: "Income:HealthEquity:Dividends"
          capital_gains_account: "Income:HealthEquity:Capital-Gains"

        2016-01-15 * "Employer Contribution (Tax year: 2016)"
          Assets:HSA:HealthEquity:Cash   800.00 USD
            date: 2016-01-15
            healthequity_transaction_type: "Contribution"
            source_desc: "Employer Contribution (Tax year: 2016)"
            cleared: TRUE
          Expenses:FIXME                           -800.00 USD

        2016-01-16 * "Investment: VIIIX"
          Assets:HSA:HealthEquity:Cash  -300.00 USD
            date: 2016-01-16
            healthequity_transaction_type: "Other"
            source_desc: "Investment: VIIIX"
            cleared: TRUE
          Assets:HSA:HealthEquity:VIIIX    1.745 VIIIX {171.92 USD}
            date: 2016-01-19
            source_desc: "Buy"
            cleared: TRUE

        2016-01-31 * "Interest for Jan-16"
          Assets:HSA:HealthEquity:Cash   0.01 USD
            date: 2016-01-31
            healthequity_transaction_type: "Other"
            source_desc: "Interest for Jan-16"
            cleared: TRUE
          Expenses:FIXME                           -0.01 USD

        2016-02-29 * "Interest for Feb-16"
          Assets:HSA:HealthEquity:Cash   0.02 USD
            date: 2016-02-29
            healthequity_transaction_type: "Other"
            source_desc: "Interest for Feb-16"
            cleared: TRUE
          Expenses:FIXME                           -0.02 USD

        2016-03-11 * "Employer Contribution (Tax year: 2015)"
          Assets:HSA:HealthEquity:Cash   1600.00 USD
            date: 2016-03-11
            healthequity_transaction_type: "Contribution"
            source_desc: "Employer Contribution (Tax year: 2015)"
            cleared: TRUE
          Expenses:FIXME                           -1600.00 USD

        2016-03-11 * "Investment: VIIIX"
          Assets:HSA:HealthEquity:Cash  -1600.03 USD
            date: 2016-03-11
            healthequity_transaction_type: "Other"
            source_desc: "Investment: VIIIX"
            cleared: TRUE
          Assets:HSA:HealthEquity:VIIIX     8.622 VIIIX {185.58 USD}
            date: 2016-03-11
            source_desc: "Buy"
            cleared: TRUE

        2016-03-16 * "Dividend"
          Assets:HSA:HealthEquity:VIIIX   0.056 VIIIX {185.71 USD}
            date: 2016-03-16
            source_desc: "Dividend"
            cleared: TRUE
          Income:HealthEquity:Dividends:VIIIX       -10.40 USD

        2016-06-16 * "Dividend"
          invalid_id: "a"
          Assets:HSA:HealthEquity:VIIIX  0.051 VIIIX {191.57 USD}
            invalid_id: "p"
            date: 2016-06-16
            source_desc: "Dividend"
            cleared: TRUE
          Income:HealthEquity:Dividends:VIIIX       -9.77 USD

        2016-06-16 * "Dividend"
          invalid_id: "b"
          Assets:HSA:HealthEquity:VIIIX  0.051 VIIIX {191.57 USD}
            invalid_id: "p"
            date: 2016-06-16
            source_desc: "Dividend"
            cleared: TRUE
          Income:HealthEquity:Dividends:VIIIX       -9.77 USD
        ''',
        accounts=frozenset([
            'Assets:HSA:HealthEquity:VIIIX',
            'Assets:HSA:HealthEquity:Cash',
            'Assets:HSA:HealthEquity',
        ]),
        pending=[
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-contribution.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-16 balance Assets:HSA:HealthEquity:Cash         800.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-17 balance Assets:HSA:HealthEquity:Cash         500.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-01-20 balance Assets:HSA:HealthEquity:VIIIX        1.745 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-02-01 balance Assets:HSA:HealthEquity:Cash         500.01 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-03-01 balance Assets:HSA:HealthEquity:Cash         500.03 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/cash-transactions-other.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-03-12 balance Assets:HSA:HealthEquity:Cash         500.00 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    2
                },
                entries=r"""
                2016-03-12 balance Assets:HSA:HealthEquity:VIIIX        10.367 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    3
                },
                entries=r"""
                2016-03-17 balance Assets:HSA:HealthEquity:VIIIX        10.423 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/investment-transactions.csv'),
                    'line':
                    4
                },
                entries=r"""
                2016-06-17 balance Assets:HSA:HealthEquity:VIIIX        10.474 VIIIX
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/2016-08-26T152440-0700.balances.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-08-26 price VIIIX                              199.17 USD
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/csv',
                    'filename':
                    os.path.join(testdata_dir,
                                 '1234567/2016-08-27T200554-0700.balances.csv'),
                    'line':
                    1
                },
                entries=r"""
                2016-08-27 price VIIIX                              198.86 USD
                """,
            )
        ],
        invalid_references=[
            (1, [('a', 'p'), ('b', 'p')]),
        ],
    )
