import os
import datetime

import pytest
from beancount.core.amount import Amount
from beancount.core.number import D

from .source_test import check_source, import_result
from ..training import PredictionInput

testdata_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'ofx'))

vanguard_path = os.path.join(testdata_dir, 'vanguard.ofx')


def test_vanguard_basic(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [vanguard_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Investment:Vanguard
          ofx_org: "The Vanguard Group"
          ofx_broker_id: "vanguard.com"
          ofx_account_type: "securities_only"
          account_id: "01234567890"
          capital_gains_account: "Income:Vanguard:Capital-Gains"
        ''',
        accounts=frozenset([
            'Assets:Investment:Vanguard',
            'Assets:Investment:Vanguard:VFINX',
            'Assets:Investment:Vanguard:VFIAX',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-15 * "SELLMF - THIS IS A MEMO"
                  Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
                    date: 2011-07-15
                    ofx_fitid: "01234567890.0123.07152011.0"
                    ofx_memo: "THIS IS A MEMO"
                    ofx_type: "SELLMF"
                  Income:Vanguard:Capital-Gains:VFINX                            
                  Expenses:FIXME      4212.30 USD               
                """,
                unknown_account_prediction_inputs=[
                    PredictionInput(
                        source_account='Assets:Investment:Vanguard:VFINX',
                        amount=Amount(D('-42.123'), 'VFINX'),
                        date=datetime.date(2011, 7, 15),
                        key_value_pairs={
                            'ofx_type': 'SELLMF',
                            'ofx_memo': 'THIS IS A MEMO',
                            'desc': 'THIS IS A MEMO'
                        })
                ],
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFINX                102.0 VFINX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFINX                              100.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFIAX                142.2 VFIAX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFIAX                              100.42 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_vanguard_matching(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [vanguard_path],
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Assets:Investment:Vanguard
          ofx_org: "The Vanguard Group"
          ofx_broker_id: "vanguard.com"
          ofx_account_type: "securities_only"
          account_id: "01234567890"
          capital_gains_account: "Income:Vanguard:Capital-Gains"

        2011-07-01 * "Manually added BUY transaction"
          Assets:Investment:Vanguard:VFINX     100 VFINX {90.00 USD}
          Assets:Investment:Vanguard:Cash

        2011-07-15 * "SELLMF - THIS IS A MEMO"
          Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
            date: 2011-07-15
            ofx_fitid: "01234567890.0123.07152011.0"
            ofx_memo: "THIS IS A MEMO"
            ofx_type: "SELLMF"
            cleared: TRUE
          Income:Vanguard:Capital-Gains:VFINX                            
          Assets:Checking      4212.30 USD               
        ''',
        accounts=frozenset([
            'Assets:Investment:Vanguard',
            'Assets:Investment:Vanguard:VFINX',
            'Assets:Investment:Vanguard:VFIAX',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFINX                102.0 VFINX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFINX                              100.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFIAX                142.2 VFIAX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFIAX                              100.42 USD
                """,
            )
        ],
        training_examples=[
            (PredictionInput(
                source_account='Assets:Investment:Vanguard:VFINX',
                amount=Amount(D('-42.123'), 'VFINX'),
                date=datetime.date(2011, 7, 15),
                key_value_pairs={
                    'ofx_type': 'SELLMF',
                    'ofx_memo': 'THIS IS A MEMO',
                    'desc': 'THIS IS A MEMO'
                }), 'Assets:Checking'),
        ],
    )


def test_vanguard_invalid(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [vanguard_path],
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Assets:Investment:Vanguard
          ofx_org: "The Vanguard Group"
          ofx_broker_id: "vanguard.com"
          ofx_account_type: "securities_only"
          account_id: "01234567890"
          capital_gains_account: "Income:Vanguard:Capital-Gains"

        2011-07-01 * "Manually added BUY transaction"
          Assets:Investment:Vanguard:VFINX     100 VFINX {90.00 USD}
          Assets:Investment:Vanguard:Cash

        2011-07-15 * "SELLMF - THIS IS A MEMO"
          invalid_id: "a"
          Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
            invalid_id: "p"
            date: 2011-07-15
            ofx_fitid: "01234567890.0123.07152011.0"
            ofx_memo: "THIS IS A MEMO"
            ofx_type: "SELLMF"
            cleared: TRUE
          Income:Vanguard:Capital-Gains:VFINX                            
          Assets:Checking      4212.30 USD               

        2011-07-15 * "SELLMF - THIS IS A MEMO"
          invalid_id: "b"
          Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
            invalid_id: "p"
            date: 2011-07-15
            ofx_fitid: "01234567890.0123.07152011.0"
            ofx_memo: "THIS IS A MEMO"
            ofx_type: "SELLMF"
            cleared: TRUE
          Income:Vanguard:Capital-Gains:VFINX                            
          Assets:Checking      4212.30 USD               
        ''',
        accounts=frozenset([
            'Assets:Investment:Vanguard',
            'Assets:Investment:Vanguard:VFINX',
            'Assets:Investment:Vanguard:VFIAX',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFINX                102.0 VFINX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFINX                              100.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFIAX                142.2 VFIAX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFIAX                              100.42 USD
                """,
            )
        ],
        invalid_references=[
            (1, [('a', 'p'), ('b', 'p')]),
        ],
    )


def test_vanguard_with_cash_account(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [vanguard_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Investment:Vanguard
          ofx_org: "The Vanguard Group"
          ofx_broker_id: "vanguard.com"
          ofx_account_type: "securities_and_pending_cash"
          account_id: "01234567890"
          capital_gains_account: "Income:Vanguard:Capital-Gains"
        ''',
        accounts=frozenset([
            'Assets:Investment:Vanguard',
            'Assets:Investment:Vanguard:Cash',
            'Assets:Investment:Vanguard:VFINX',
            'Assets:Investment:Vanguard:VFIAX',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-15 * "SELLMF - THIS IS A MEMO"
                  Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
                    date: 2011-07-15
                    ofx_fitid: "01234567890.0123.07152011.0"
                    ofx_memo: "THIS IS A MEMO"
                    ofx_type: "SELLMF"
                  Income:Vanguard:Capital-Gains:VFINX                            
                  Assets:Investment:Vanguard:Cash      4212.30 USD               
                    ofx_fitid: "01234567890.0123.07152011.0"
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-15 * "Transfer due to: SELLMF - THIS IS A MEMO"
                  Assets:Investment:Vanguard:Cash  -4212.30 USD
                    ofx_fitid_transfer: "01234567890.0123.07152011.0"
                    date: 2011-07-15
                    ofx_memo: "THIS IS A MEMO"
                    ofx_type_transfer: "SELLMF"
                  Expenses:FIXME                    4212.30 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFINX                102.0 VFINX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFINX                              100.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFIAX                142.2 VFIAX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFIAX                              100.42 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_vanguard_with_cash_account_matching_missing_transfer(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [vanguard_path],
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"
        1900-01-01 open Assets:Investment:Vanguard
          ofx_org: "The Vanguard Group"
          ofx_broker_id: "vanguard.com"
          ofx_account_type: "securities_and_pending_cash"
          account_id: "01234567890"
          capital_gains_account: "Income:Vanguard:Capital-Gains"

        1900-01-01 * "Fake buy"
          Assets:Investment:Vanguard:VFINX     42.123 VFINX {80.00 USD}
          Expenses:FIXME

        2011-07-15 * "SELLMF - THIS IS A MEMO"
          Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
            date: 2011-07-15
            ofx_fitid: "01234567890.0123.07152011.0"
            ofx_memo: "THIS IS A MEMO"
            ofx_type: "SELLMF"
            cleared: TRUE
          Income:Vanguard:Capital-Gains:VFINX                            
          Assets:Investment:Vanguard:Cash      4212.30 USD               
            ofx_fitid: "01234567890.0123.07152011.0"
            cleared: TRUE
        ''',
        accounts=frozenset([
            'Assets:Investment:Vanguard',
            'Assets:Investment:Vanguard:Cash',
            'Assets:Investment:Vanguard:VFINX',
            'Assets:Investment:Vanguard:VFIAX',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-15 * "Transfer due to: SELLMF - THIS IS A MEMO"
                  Assets:Investment:Vanguard:Cash  -4212.30 USD
                    ofx_fitid_transfer: "01234567890.0123.07152011.0"
                    date: 2011-07-15
                    ofx_memo: "THIS IS A MEMO"
                    ofx_type_transfer: "SELLMF"
                  Expenses:FIXME                    4212.30 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFINX                102.0 VFINX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFINX                              100.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFIAX                142.2 VFIAX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFIAX                              100.42 USD
                """,
            )
        ],
    )


def test_vanguard_with_cash_account_matching_missing_primary(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [vanguard_path],
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"
        1900-01-01 open Assets:Investment:Vanguard
          ofx_org: "The Vanguard Group"
          ofx_broker_id: "vanguard.com"
          ofx_account_type: "securities_and_pending_cash"
          account_id: "01234567890"
          capital_gains_account: "Income:Vanguard:Capital-Gains"

        2011-07-15 * "Transfer due to: SELLMF - THIS IS A MEMO"
          Assets:Investment:Vanguard:Cash  -4212.30 USD
            ofx_fitid_transfer: "01234567890.0123.07152011.0"
          Expenses:FIXME                    4212.30 USD
        ''',
        accounts=frozenset([
            'Assets:Investment:Vanguard',
            'Assets:Investment:Vanguard:Cash',
            'Assets:Investment:Vanguard:VFINX',
            'Assets:Investment:Vanguard:VFIAX',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-15 * "SELLMF - THIS IS A MEMO"
                  Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
                    date: 2011-07-15
                    ofx_fitid: "01234567890.0123.07152011.0"
                    ofx_memo: "THIS IS A MEMO"
                    ofx_type: "SELLMF"
                  Income:Vanguard:Capital-Gains:VFINX                            
                  Assets:Investment:Vanguard:Cash      4212.30 USD               
                    ofx_fitid: "01234567890.0123.07152011.0"
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFINX                102.0 VFINX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFINX                              100.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 balance Assets:Investment:Vanguard:VFIAX                142.2 VFIAX
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard_path
                },
                entries=r"""
                2011-07-26 price VFIAX                              100.42 USD
                """,
            )
        ],
    )


def test_vanguard401k(tmpdir):
    vanguard401k_path = os.path.join(testdata_dir, 'vanguard401k.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [vanguard401k_path],
        },
        journal_contents=r'''
        1900-01-01 commodity VANGUARD-92202V351
          cusip: "92202V351"

        1900-01-01 open Assets:Retirement:Vanguard:Company401k
          ofx_org: "Vanguard"
          ofx_broker_id: "vanguard.com"
          ofx_account_type: "securities_only"
          account_id: "0123456"
          capital_gains_account: "Income:Vanguard:Capital-Gains"
          match_contribution_account: "Income:Company:Match"
        ''',
        accounts=frozenset([
            'Assets:Retirement:Vanguard:Company401k:PreTax:VANGUARD-92202V351',
            'Assets:Retirement:Vanguard:Company401k:Match:VANGUARD-92202V351',
            'Assets:Retirement:Vanguard:Company401k',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard401k_path
                },
                entries=r"""
                2013-09-05 * "TRANSFER - MATCH - Investment Expense"
                  Assets:Retirement:Vanguard:Company401k:Match:VANGUARD-92202V351  -0.04241 VANGUARD-92202V351 {} @ 39.37 USD
                    date: 2013-09-05
                    ofx_fitid: "1234567890123456795AAA"
                    ofx_memo: "Investment Expense"
                    ofx_type: "TRANSFER"
                  Income:Vanguard:Capital-Gains:VANGUARD-92202V351                                                           
                  Expenses:FIXME                                                       1.67 USD                              
                """,
                unknown_account_prediction_inputs=[
                    PredictionInput(
                        source_account=
                        'Assets:Retirement:Vanguard:Company401k:Match:VANGUARD-92202V351',
                        amount=Amount(D('-0.04241'), 'VANGUARD-92202V351'),
                        date=datetime.date(2013, 9, 5),
                        key_value_pairs={
                            'ofx_type': 'TRANSFER',
                            'ofx_memo': 'Investment Expense',
                            'desc': 'Investment Expense'
                        }),
                ],
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard401k_path
                },
                entries=r"""
                2014-09-26 * "BUYMF - PRETAX"
                  Assets:Retirement:Vanguard:Company401k:PreTax:VANGUARD-92202V351  14.61137 VANGUARD-92202V351 {46.06 USD}
                    date: 2014-09-26
                    ofx_fitid: "1234567890123456790AAA"
                    ofx_type: "BUYMF"
                  Expenses:FIXME                 -673.00 USD                           
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard401k_path
                },
                entries=r"""
                2014-09-26 * "BUYMF - MATCH"
                  Assets:Retirement:Vanguard:Company401k:Match:VANGUARD-92202V351  7.30568 VANGUARD-92202V351 {46.06 USD}
                    date: 2014-09-26
                    ofx_fitid: "1234567890123456791AAA"
                    ofx_type: "BUYMF"
                  Income:Company:Match                                             -336.50 USD                           
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard401k_path
                },
                entries=r"""
                2014-10-10 * "BUYMF - PRETAX"
                  Assets:Retirement:Vanguard:Company401k:PreTax:VANGUARD-92202V351  15.25039 VANGUARD-92202V351 {44.13 USD}
                    date: 2014-10-10
                    ofx_fitid: "1234567890123456793AAA"
                    ofx_type: "BUYMF"
                  Expenses:FIXME                 -673.00 USD                           
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard401k_path
                },
                entries=r"""
                2014-10-10 * "BUYMF - MATCH"
                  Assets:Retirement:Vanguard:Company401k:Match:VANGUARD-92202V351  7.62519 VANGUARD-92202V351 {44.13 USD}
                    date: 2014-10-10
                    ofx_fitid: "1234567890123456794AAA"
                    ofx_type: "BUYMF"
                  Income:Company:Match                                             -336.50 USD                           
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard401k_path
                },
                entries=r"""
                2014-10-17 balance Assets:Retirement:Vanguard:Company401k          117.506 VANGUARD-92202V351
                
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': vanguard401k_path
                },
                entries=r"""
                2014-10-17 price VANGUARD-92202V351                  44.01 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_fidelity_savings(tmpdir):
    fidelity_savings_path = os.path.join(testdata_dir, 'fidelity-savings.ofx')
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [fidelity_savings_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Savings:Fidelity
          ofx_org: "fidelity.com"
          ofx_broker_id: "fidelity.com"
          account_id: "X0000001"
          ofx_account_type: "cash_only"
        ''',
        accounts=frozenset(['Assets:Savings:Fidelity']),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': fidelity_savings_path
                },
                entries=r"""
                2012-07-20 * "INVBANKTRAN - Check Paid #0000001001"
                  Assets:Savings:Fidelity  -1500.00 USD
                    check: 1001
                    date: 2012-07-20
                    ofx_fitid: "X0000000000000000000001"
                    ofx_memo: "Check Paid #0000001001"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME                 1500.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': fidelity_savings_path
                },
                entries=r"""
                2012-07-27 * "INVBANKTRAN - TRANSFERRED FROM VS X10-08144-1"
                  Assets:Savings:Fidelity   115.83 USD
                    date: 2012-07-27
                    ofx_fitid: "X0000000000000000000002"
                    ofx_memo: "TRANSFERRED FROM VS X10-08144-1"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME                -115.83 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': fidelity_savings_path
                },
                entries=r"""
                2012-07-27 * "INVBANKTRAN - BILL PAYMENT CITICORP CHOICE /0001/N********"
                  Assets:Savings:Fidelity  -197.11 USD
                    date: 2012-07-27
                    ofx_fitid: "X0000000000000000000003"
                    ofx_memo: "BILL PAYMENT CITICORP CHOICE /0001/N********"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME                 197.11 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': fidelity_savings_path
                },
                entries=r"""
                2012-07-27 * "INVBANKTRAN - DIRECT DEBIT HOMESTREET LS LOAN PMT"
                  Assets:Savings:Fidelity  -197.12 USD
                    date: 2012-07-27
                    ofx_fitid: "X0000000000000000000004"
                    ofx_memo: "DIRECT DEBIT HOMESTREET LS LOAN PMT"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME                 197.12 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_suncorp(tmpdir):
    source_path = os.path.join(testdata_dir, 'suncorp.ofx')
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Checking:Suncorp
          ofx_org: "SUNCORP"
          ofx_broker_id: ""
          account_id: "123456789"
          ofx_account_type: "cash_only"
        ''',
        accounts=frozenset(['Assets:Checking:Suncorp']),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2013-12-15 * "STMTTRN - EFTPOS WDL HANDYWAY ALDI STORE GEELONG WEST VICAU"
                  Assets:Checking:Suncorp  -16.85 AUD
                    date: 2013-12-15
                    ofx_fitid: "1"
                    ofx_memo: "EFTPOS WDL HANDYWAY ALDI STORE GEELONG WEST VICAU"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME   16.85 AUD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2013-12-15 balance Assets:Checking:Suncorp                         1234.12 AUD
                """,
            )
        ],
        training_examples=[],
    )


def test_checking(tmpdir):
    source_path = os.path.join(testdata_dir, 'checking.ofx')
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Checking
          ofx_org: "FAKE"
          ofx_broker_id: ""
          account_id: "1452687~7"
          ofx_account_type: "cash_only"
        ''',
        accounts=frozenset(['Assets:Checking']),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2011-03-31 * "STMTTRN - DIVIDEND EARNED FOR PERIOD OF 03/01/2011 THROUGH 03/31/2011 ANNUAL PERCENTAGE YIELD EARNED IS 0.05%"
                  Assets:Checking   0.01 USD
                    date: 2011-03-31
                    ofx_fitid: "0000486"
                    ofx_memo: "DIVIDEND EARNED FOR PERIOD OF 03/01/2011 THROUGH 03/31/2011 ANNUAL PERCENTAGE YIELD EARNED IS 0.05%"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME  -0.01 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2011-04-05 * "STMTTRN - AUTOMATIC WITHDRAWAL, ELECTRIC BILL WEB(S )"
                  Assets:Checking  -34.51 USD
                    date: 2011-04-05
                    ofx_fitid: "0000487"
                    ofx_memo: "AUTOMATIC WITHDRAWAL, ELECTRIC BILL WEB(S )"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME   34.51 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2011-04-07 * "STMTTRN - RETURNED CHECK FEE, CHECK # 319 FOR $45.33 ON 04/07/11"
                  Assets:Checking  -25.00 USD
                    check: 319
                    date: 2011-04-07
                    ofx_fitid: "0000488"
                    ofx_memo: "RETURNED CHECK FEE, CHECK # 319 FOR $45.33 ON 04/07/11"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME   25.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2013-05-25 balance Assets:Checking                                 100.99 USD
                
                """,
            )
        ],
        training_examples=[],
    )


def test_td_ameritrade(tmpdir):
    source_path = os.path.join(testdata_dir, 'td_ameritrade.ofx')
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Investment:TD-Ameritrade
          ofx_org: "ameritrade.com"
          ofx_broker_id: "ameritrade.com"
          account_id: "121212121"
          ofx_account_type: "securities_and_cash"

        1900-01-01 commodity AMERITRADE-912810RW0
          cusip: "912810RW0"
        ''',
        accounts=frozenset([
            'Assets:Investment:TD-Ameritrade',
            'Assets:Investment:TD-Ameritrade:AMZN',
            'Assets:Investment:TD-Ameritrade:AMERITRADE-912810RW0',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2017-12-03 balance Assets:Investment:TD-Ameritrade:AMZN            1 AMZN
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2017-12-03 price AMZN                                 1000 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2017-12-03 balance Assets:Investment:TD-Ameritrade:AMERITRADE-912810RW0       1000 AMERITRADE-912810RW0
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2017-12-03 price AMERITRADE-912810RW0                             100 USD
                """,
            )
        ],
    )


def test_anzcc(tmpdir):
    source_path = os.path.join(testdata_dir, 'anzcc.ofx')
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Liabilities:Credit-Card
          ofx_org: ""
          ofx_broker_id: ""
          account_id: "1234123412341234"
          ofx_account_type: "cash_only"
        ''',
        accounts=frozenset(['Liabilities:Credit-Card']),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2017-05-08 * "STMTTRN - SOME MEMO"
                  Liabilities:Credit-Card  -5.50 AUD
                    date: 2017-05-08
                    ofx_fitid: "201705080001"
                    ofx_memo: "SOME MEMO"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME                 5.50 AUD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2017-05-10 balance Liabilities:Credit-Card                    -123.45 AUD
                """,
            )
        ],
    )


def test_multiple_accounts(tmpdir):
    source_path = os.path.join(testdata_dir, 'multiple_accounts.ofx')
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Checking
          ofx_org: "blah"
          ofx_broker_id: ""
          account_id: "9100"
          ofx_account_type: "cash_only"

        1900-01-01 open Assets:Savings
          ofx_org: "blah"
          ofx_broker_id: ""
          account_id: "9200"
          ofx_account_type: "cash_only"
        ''',
        accounts=frozenset([
            'Assets:Checking',
            'Assets:Savings',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2012-06-03 balance Assets:Checking                                 111 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2012-06-03 balance Assets:Savings                                  222 USD
                """,
            )
        ],
    )


def test_bank_medium(tmpdir):
    source_path = os.path.join(testdata_dir, 'bank_medium.ofx')
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Checking
          ofx_org: ""
          ofx_broker_id: ""
          account_id: "12300 000012345678"
          ofx_account_type: "cash_only"
        ''',
        accounts=frozenset([
            'Assets:Checking',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2009-04-01 * "STMTTRN - POS MERCHANDISE;MCDONALD'S #112"
                  Assets:Checking  -6.60 CAD
                    date: 2009-04-01
                    ofx_fitid: "0000123456782009040100001"
                    ofx_memo: "POS MERCHANDISE;MCDONALD'S #112"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME    6.60 CAD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2009-04-02 * "STMTTRN - MISCELLANEOUS PAYMENTS;Joe's Bald Hairstyles"
                  Assets:Checking  -316.67 CAD
                    date: 2009-04-02
                    ofx_fitid: "0000123456782009040200004"
                    ofx_memo: "MISCELLANEOUS PAYMENTS;Joe's Bald Hairstyles"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME    316.67 CAD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2009-04-03 * "STMTTRN - POS MERCHANDISE;CONNIE'S HAIR D"
                  Assets:Checking  -22.00 CAD
                    date: 2009-04-03
                    ofx_fitid: "0000123456782009040300005"
                    ofx_memo: "POS MERCHANDISE;CONNIE'S HAIR D"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME    22.00 CAD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2009-05-23 balance Assets:Checking                                 382.34 CAD
                """,
            )
        ],
    )


def test_investment_401k(tmpdir):
    source_path = os.path.join(testdata_dir, 'investment_401k.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Retirement:ExampleOrg:Company401k
          ofx_org: "EXAMPLE"
          ofx_broker_id: "example.org"
          ofx_account_type: "securities_only"
          account_id: "12345678.123456-01"
          capital_gains_account: "Income:ExampleOrg:Capital-Gains"
        ''',
        accounts=frozenset([
            'Assets:Retirement:ExampleOrg:Company401k',
            'Assets:Retirement:ExampleOrg:Company401k:BAR',
            'Assets:Retirement:ExampleOrg:Company401k:BAZ',
            'Assets:Retirement:ExampleOrg:Company401k:FOO',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-06-17 * "BUYMF"
                  Assets:Retirement:ExampleOrg:Company401k:FOO   8.846699 FOO {22.2908 USD}
                    date: 2014-06-17
                    ofx_fitid: "1"
                    ofx_type: "BUYMF"
                  Expenses:FIXME   -197.20 USD              
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-06-30 * "TRANSFER"
                  Assets:Retirement:ExampleOrg:Company401k:BAR  6.800992 BAR {1 USD, "FIXME"}
                    date: 2014-06-30
                    ofx_fitid: "2"
                    ofx_type: "TRANSFER"
                  Expenses:FIXME                                 -198.69 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-06-30 * "TRANSFER"
                  Assets:Retirement:ExampleOrg:Company401k:BAZ  -9.060702 BAZ {} @ 21.928764 USD
                    date: 2014-06-30
                    ofx_fitid: "3"
                    ofx_type: "TRANSFER"
                  Income:ExampleOrg:Capital-Gains:BAZ                                           
                  Expenses:FIXME                                   198.69 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-06-30 balance Assets:Retirement:ExampleOrg:Company401k:FOO    17.604312 FOO
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-06-30 price FOO                             22.517211 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-06-30 price BAR                             29.214855 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-06-30 price BAZ                                   0.0 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_investment_buy_sell_income(tmpdir):
    source_path = os.path.join(testdata_dir, 'investment_buy_sell_income.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Investment:MyBank
          ofx_org: "MyBank"
          ofx_broker_id: "MyBank"
          ofx_account_type: "securities_and_cash"
          account_id: "123456789"
          div_income_account: "Income:MyBank:Dividends"
          interest_income_account: "Income:MyBank:Interest"
          capital_gains_account: "Income:MyBank:Capital-Gains"
          fees_account: "Expenses:Investment:MyBank:Fees"
          commission_account: "Expenses:Investment:MyBank:Commission"

        1900-01-01 commodity QTSAQ
          equivalent_currency: "USD"
        ''',
        accounts=frozenset([
            'Assets:Investment:MyBank',
            'Assets:Investment:MyBank:Cash',
            'Assets:Investment:MyBank:DGTGU',
            'Assets:Investment:MyBank:DSNTH',
            'Assets:Investment:MyBank:EEBHF',
            'Assets:Investment:MyBank:FCSNO',
            'Assets:Investment:MyBank:IQVJK',
            'Assets:Investment:MyBank:MCYFM',
            'Assets:Investment:MyBank:MXMUK',
            'Assets:Investment:MyBank:OMCNS',
            'Assets:Investment:MyBank:RDTAF',
            'Assets:Investment:MyBank:SDVMV',
            'Assets:Investment:MyBank:SVDIE',
            'Assets:Investment:MyBank:URMFO',
            'Assets:Investment:MyBank:WDUZQ',
            'Assets:Investment:MyBank:WKPSD',
            'Assets:Investment:MyBank:XBFMS',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-02 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   62.65 USD
                    date: 2018-07-02
                    ofx_fitid: "fd2561ce31fca077e.87f.0"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:URMFO  -62.65 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-09 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   79.36 USD
                    date: 2018-07-09
                    ofx_fitid: "b7b7c2d1ed2107a05.91c.e"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:WDUZQ  -79.36 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-09 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   84.97 USD
                    date: 2018-07-09
                    ofx_fitid: "86dc8b0770f1583be.6b2.8"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:WKPSD  -84.97 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-10 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   98.24 USD
                    date: 2018-07-10
                    ofx_fitid: "b9ffa5afd2b02b418b.4ed.6"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:DGTGU  -98.24 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-10 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   90.20 USD
                    date: 2018-07-10
                    ofx_fitid: "0dc909094220704bf.c5.6"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:MXMUK  -90.20 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-10 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   86.76 USD
                    date: 2018-07-10
                    ofx_fitid: "ccdb6a24f107053d47.ab3.8"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:XBFMS  -86.76 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-10 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   73.47 USD
                    date: 2018-07-10
                    ofx_fitid: "7c6779dc3ceb37328d.5e0.e"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:OMCNS  -73.47 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-10 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   75.57 USD
                    date: 2018-07-10
                    ofx_fitid: "5edc79aaa46e67da46.cff.7"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:RDTAF  -75.57 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-24 * "INVBANKTRAN - PORTFOLIO SRVCS. FEE PORTFOLIO S"
                  Assets:Investment:MyBank:Cash  -8.48 USD
                    date: 2018-07-24
                    ofx_fitid: "4b50d5-548.13"
                    ofx_name: "PORTFOLIO SRVCS. FEE PORTFOLIO S"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME                  8.48 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-31 * "INCOME - INTEREST"
                  Assets:Investment:MyBank:Cash   57.91 USD
                    date: 2018-07-31
                    ofx_fitid: "ea13308d69059413.f73.9"
                    ofx_type: "INCOME"
                  Income:MyBank:Interest:QTSAQ   -57.91 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-01 * "BUYSTOCK"
                  Assets:Investment:MyBank:SDVMV         60.01318 SDVMV {67.141053527 USD}
                    date: 2018-08-01
                    ofx_fitid: "aedf1852aa39a54-623ee.4d104.5"
                    ofx_type: "BUYSTOCK"
                  Assets:Investment:MyBank:Cash          -4115.86 USD                     
                    ofx_fitid: "aedf1852aa39a54-623ee.4d104.5"
                  Expenses:Investment:MyBank:Fees         63.4869 USD                     
                  Expenses:Investment:MyBank:Commission   23.0233 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-01 * "INCOME - DIV"
                  Assets:Investment:MyBank:Cash   42.30 USD
                    date: 2018-08-01
                    ofx_fitid: "ba3db75b5ed7f44e2.f10.0"
                    ofx_type: "INCOME"
                  Income:MyBank:Dividends:URMFO  -42.30 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-01 * "SELLSTOCK"
                  Assets:Investment:MyBank:EEBHF         -41.50416 EEBHF {} @ 83.661853593 USD
                    date: 2018-08-01
                    ofx_fitid: "4a5141ead2c672e8a559.65-80e.b"
                    ofx_type: "SELLSTOCK"
                  Income:MyBank:Capital-Gains:EEBHF                                           
                  Assets:Investment:MyBank:Cash            3382.60 USD                        
                    ofx_fitid: "4a5141ead2c672e8a559.65-80e.b"
                  Expenses:Investment:MyBank:Fees          31.9944 USD                        
                  Expenses:Investment:MyBank:Commission    57.7239 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:Cash                   7.36 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:FCSNO                  99.80986 FCSNO
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price FCSNO                          47.5020085 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:DSNTH                  97.46709 DSNTH
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price DSNTH                          45.6437641 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:MCYFM                  18.61553 MCYFM
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price MCYFM                          43.6188596 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:URMFO                  44.71348 URMFO
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price URMFO                          19.9728700 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:DGTGU                  99.97586 DGTGU
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price DGTGU                          25.3577699 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:MXMUK                  82.54558 MXMUK
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price MXMUK                          81.7052756 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:XBFMS                  72.07965 XBFMS
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price XBFMS                          97.2183155 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:OMCNS                  85.74475 OMCNS
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price OMCNS                          93.7687778 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:RDTAF                  15.30638 RDTAF
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price RDTAF                          80.9348507 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:IQVJK                  51.68027 IQVJK
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price IQVJK                          27.7635148 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:SDVMV                  13.02776 SDVMV
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price SDVMV                          62.3900924 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:SVDIE                  96.94912 SVDIE
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price SVDIE                          85.7179472 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:WDUZQ                  44.21114 WDUZQ
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price WDUZQ                          52.1515218 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 balance Assets:Investment:MyBank:WKPSD                  48.99957 WKPSD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-08-04 price WKPSD                          24.7722967 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_vanguard_roth_ira(tmpdir):
    source_path = os.path.join(testdata_dir, 'vanguard_roth_ira.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Retirement:Vanguard:Roth-IRA
          ofx_org: "MyBank"
          ofx_broker_id: "MyBank"
          ofx_account_type: "securities_only"
          account_id: "123456789"
          div_income_account: "Income:Vanguard:Dividends"
        ''',
        accounts=frozenset([
            'Assets:Retirement:Vanguard:Roth-IRA',
            'Assets:Retirement:Vanguard:Roth-IRA:TYCDT',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-21 * "REINVEST - DIV"
                  Assets:Retirement:Vanguard:Roth-IRA:TYCDT  31.704 TYCDT {2.94 USD}
                    date: 2018-06-21
                    ofx_fitid: "7c9254b784a.a9bd.edcfa27b.b"
                    ofx_type: "REINVEST"
                  Income:Vanguard:Dividends:TYCDT            -93.21 USD             
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-03 balance Assets:Retirement:Vanguard:Roth-IRA:TYCDT       46.872 TYCDT
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-03 price TYCDT                               84.20 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_vanguard_roth_ira_matching(tmpdir):
    source_path = os.path.join(testdata_dir, 'vanguard_roth_ira.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Assets:Retirement:Vanguard:Roth-IRA
          ofx_org: "MyBank"
          ofx_broker_id: "MyBank"
          ofx_account_type: "securities_only"
          account_id: "123456789"
          div_income_account: "Income:Vanguard:Dividends"

        2018-06-21 * "REINVEST - DIV"
          Assets:Retirement:Vanguard:Roth-IRA:TYCDT  31.704 TYCDT {2.94 USD}
            date: 2018-06-21
            ofx_fitid: "7c9254b784a.a9bd.edcfa27b.b"
            ofx_type: "REINVEST"
            cleared: TRUE
          Income:Vanguard:Dividends:TYCDT            -93.21 USD             
        ''',
        accounts=frozenset([
            'Assets:Retirement:Vanguard:Roth-IRA',
            'Assets:Retirement:Vanguard:Roth-IRA:TYCDT',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-03 balance Assets:Retirement:Vanguard:Roth-IRA:TYCDT       46.872 TYCDT
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-07-03 price TYCDT                               84.20 USD
                """,
            )
        ],
    )


def test_checking2(tmpdir):
    source_path = os.path.join(testdata_dir, 'checking2.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Assets:Checking:MyBank
          ofx_org: "MyBank"
          ofx_broker_id: "MyBank"
          ofx_account_type: "cash_only"
          account_id: "123456789"
        ''',
        accounts=frozenset([
            'Assets:Checking:MyBank',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-08 * "INVBANKTRAN - DIRECT DEBIT WELLSFARGO CRCARDPMT"
                  Assets:Checking:MyBank  -33.72 USD
                    date: 2018-06-08
                    ofx_fitid: "46f2144d0ce515a855234ec"
                    ofx_memo: "DIRECT DEBIT WELLSFARGO CRCARDPMT"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           33.72 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS AMZN.COM/BILL WA"
                  Assets:Checking:MyBank  -27.68 USD
                    date: 2018-06-11
                    ofx_fitid: "c91d80d9358f433cd9ab34f"
                    ofx_memo: "DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS AMZN.COM/BILL WA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           27.68 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - CHECK RECEIVED"
                  Assets:Checking:MyBank   75.60 USD
                    date: 2018-06-11
                    ofx_fitid: "9a69c0efa290c0aa6cd2e96"
                    ofx_memo: "CHECK RECEIVED"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME          -75.60 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DIRECT DEBIT CHASE TRNSFR DR"
                  Assets:Checking:MyBank  -20.28 USD
                    date: 2018-06-11
                    ofx_fitid: "7565f8c69ff4148710c25d7"
                    ofx_memo: "DIRECT DEBIT CHASE TRNSFR DR"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           20.28 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 7-ELEVEN SACRAMENTO CA"
                  Assets:Checking:MyBank  -36.07 USD
                    date: 2018-06-11
                    ofx_fitid: "bfea1b3fc6b9cd383d62383"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 7-ELEVEN SACRAMENTO CA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           36.07 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DEBIT CARD PURCHASE STARBUCKS STORE 12345"
                  Assets:Checking:MyBank  -79.70 USD
                    date: 2018-06-11
                    ofx_fitid: "13517076f2512f0830a4380"
                    ofx_memo: "DEBIT CARD PURCHASE STARBUCKS STORE 12345"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           79.70 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-12 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 WAL-MART #1234 SACRAMENTO CA"
                  Assets:Checking:MyBank  -94.49 USD
                    date: 2018-06-12
                    ofx_fitid: "d1f6c4345793074d46e3502"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 WAL-MART #1234 SACRAMENTO CA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           94.49 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-12 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 TARGET T-1234"
                  Assets:Checking:MyBank  -46.80 USD
                    date: 2018-06-12
                    ofx_fitid: "c994842e8c0170e8262860d"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 TARGET T-1234"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           46.80 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-12 * "INVBANKTRAN - DEBIT CARD PURCHASE POS9999 TARGET T-1234"
                  Assets:Checking:MyBank  -46.00 USD
                    date: 2018-06-12
                    ofx_fitid: "3bebaf840f4451980de527a"
                    ofx_memo: "DEBIT CARD PURCHASE POS9999 TARGET T-1234"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           46.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-13 * "INVBANKTRAN - DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS W WWW.AMAZON.CO WA"
                  Assets:Checking:MyBank  -9.90 USD
                    date: 2018-06-13
                    ofx_fitid: "87ddbe56b49fdfad11c9a5b"
                    ofx_memo: "DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS W WWW.AMAZON.CO WA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           9.90 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-14 * "INVBANKTRAN - DIRECT DEPOSIT MYCOMPANY PAYROLL"
                  Assets:Checking:MyBank   98.16 USD
                    date: 2018-06-14
                    ofx_fitid: "e92c27aa0ce372e7842bb27"
                    ofx_memo: "DIRECT DEPOSIT MYCOMPANY PAYROLL"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME          -98.16 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-14 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 SHELL Service"
                  Assets:Checking:MyBank  -97.05 USD
                    date: 2018-06-14
                    ofx_fitid: "d3c478700a7412ddea3bc74"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 SHELL Service"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           97.05 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-15 * "INVBANKTRAN - DEBIT CARD PURCHASE COFFEE HOUSE NEW YORK NY"
                  Assets:Checking:MyBank  -70.71 USD
                    date: 2018-06-15
                    ofx_fitid: "41fc88f8bb518e9b5c000c7"
                    ofx_memo: "DEBIT CARD PURCHASE COFFEE HOUSE NEW YORK NY"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           70.71 USD
                """,
            )
        ],
        training_examples=[],
    )


def test_checking2_matching(tmpdir):
    source_path = os.path.join(testdata_dir, 'checking2.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Assets:Checking:MyBank
          ofx_org: "MyBank"
          ofx_broker_id: "MyBank"
          ofx_account_type: "cash_only"
          account_id: "123456789"

        2018-06-08 * "INVBANKTRAN - DIRECT DEBIT WELLSFARGO CRCARDPMT"
          Assets:Checking:MyBank  -33.72 USD
            date: 2018-06-08
            ofx_fitid: "46f2144d0ce515a855234ec"
            ofx_memo: "DIRECT DEBIT WELLSFARGO CRCARDPMT"
            ofx_type: "INVBANKTRAN"
            cleared: TRUE
          Liabilities:Credit-Card           33.72 USD
        ''',
        accounts=frozenset([
            'Assets:Checking:MyBank',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS AMZN.COM/BILL WA"
                  Assets:Checking:MyBank  -27.68 USD
                    date: 2018-06-11
                    ofx_fitid: "c91d80d9358f433cd9ab34f"
                    ofx_memo: "DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS AMZN.COM/BILL WA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           27.68 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - CHECK RECEIVED"
                  Assets:Checking:MyBank   75.60 USD
                    date: 2018-06-11
                    ofx_fitid: "9a69c0efa290c0aa6cd2e96"
                    ofx_memo: "CHECK RECEIVED"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME          -75.60 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DIRECT DEBIT CHASE TRNSFR DR"
                  Assets:Checking:MyBank  -20.28 USD
                    date: 2018-06-11
                    ofx_fitid: "7565f8c69ff4148710c25d7"
                    ofx_memo: "DIRECT DEBIT CHASE TRNSFR DR"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           20.28 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 7-ELEVEN SACRAMENTO CA"
                  Assets:Checking:MyBank  -36.07 USD
                    date: 2018-06-11
                    ofx_fitid: "bfea1b3fc6b9cd383d62383"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 7-ELEVEN SACRAMENTO CA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           36.07 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-11 * "INVBANKTRAN - DEBIT CARD PURCHASE STARBUCKS STORE 12345"
                  Assets:Checking:MyBank  -79.70 USD
                    date: 2018-06-11
                    ofx_fitid: "13517076f2512f0830a4380"
                    ofx_memo: "DEBIT CARD PURCHASE STARBUCKS STORE 12345"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           79.70 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-12 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 WAL-MART #1234 SACRAMENTO CA"
                  Assets:Checking:MyBank  -94.49 USD
                    date: 2018-06-12
                    ofx_fitid: "d1f6c4345793074d46e3502"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 WAL-MART #1234 SACRAMENTO CA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           94.49 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-12 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 TARGET T-1234"
                  Assets:Checking:MyBank  -46.80 USD
                    date: 2018-06-12
                    ofx_fitid: "c994842e8c0170e8262860d"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 TARGET T-1234"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           46.80 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-12 * "INVBANKTRAN - DEBIT CARD PURCHASE POS9999 TARGET T-1234"
                  Assets:Checking:MyBank  -46.00 USD
                    date: 2018-06-12
                    ofx_fitid: "3bebaf840f4451980de527a"
                    ofx_memo: "DEBIT CARD PURCHASE POS9999 TARGET T-1234"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           46.00 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-13 * "INVBANKTRAN - DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS W WWW.AMAZON.CO WA"
                  Assets:Checking:MyBank  -9.90 USD
                    date: 2018-06-13
                    ofx_fitid: "87ddbe56b49fdfad11c9a5b"
                    ofx_memo: "DEBIT CARD PURCHASE AMAZON MKTPLACE PMTS W WWW.AMAZON.CO WA"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           9.90 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-14 * "INVBANKTRAN - DIRECT DEPOSIT MYCOMPANY PAYROLL"
                  Assets:Checking:MyBank   98.16 USD
                    date: 2018-06-14
                    ofx_fitid: "e92c27aa0ce372e7842bb27"
                    ofx_memo: "DIRECT DEPOSIT MYCOMPANY PAYROLL"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME          -98.16 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-14 * "INVBANKTRAN - DEBIT CARD PURCHASE POS1234 SHELL Service"
                  Assets:Checking:MyBank  -97.05 USD
                    date: 2018-06-14
                    ofx_fitid: "d3c478700a7412ddea3bc74"
                    ofx_memo: "DEBIT CARD PURCHASE POS1234 SHELL Service"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           97.05 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2018-06-15 * "INVBANKTRAN - DEBIT CARD PURCHASE COFFEE HOUSE NEW YORK NY"
                  Assets:Checking:MyBank  -70.71 USD
                    date: 2018-06-15
                    ofx_fitid: "41fc88f8bb518e9b5c000c7"
                    ofx_memo: "DEBIT CARD PURCHASE COFFEE HOUSE NEW YORK NY"
                    ofx_type: "INVBANKTRAN"
                  Expenses:FIXME           70.71 USD
                """,
            )
        ],
        training_examples=[
            (PredictionInput(
                source_account='Assets:Checking:MyBank',
                amount=Amount(D('-33.72'), 'USD'),
                date=datetime.date(2018, 6, 8),
                key_value_pairs={
                    'ofx_type': 'INVBANKTRAN',
                    'ofx_memo': 'DIRECT DEBIT WELLSFARGO CRCARDPMT',
                    'desc': 'DIRECT DEBIT WELLSFARGO CRCARDPMT'
                }), 'Liabilities:Credit-Card'),
        ],
    )



def test_amex(tmpdir):
    source_path = os.path.join(testdata_dir, 'amex.ofx')

    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [source_path],
        },
        journal_contents=r'''
        1900-01-01 open Liabilities:Credit-Card:Amex
          ofx_org: "AMEX"
          ofx_broker_id: ""
          ofx_account_type: "cash_only"
          account_id: "379700001111222"
        ''',
        accounts=frozenset([
            'Liabilities:Credit-Card:Amex',
        ]),
        pending=[
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2013-11-24 * "STMTTRN - PRUNE NEW YORK - 7101466 RESTAURANT"
                  Liabilities:Credit-Card:Amex  -143.94 USD
                    date: 2013-11-24
                    ofx_fitid: "320133280255184014"
                    ofx_memo: "7101466 RESTAURANT"
                    ofx_name: "PRUNE NEW YORK"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME                 143.94 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2013-11-25 * "STMTTRN - TAKAHACHI RESTAURANTNEW YORK - 000451990 RESTAURANT"
                  Liabilities:Credit-Card:Amex  -28.05 USD
                    date: 2013-11-25
                    ofx_fitid: "320133290268683266"
                    ofx_memo: "000451990 RESTAURANT"
                    ofx_name: "TAKAHACHI RESTAURANTNEW YORK"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME                 28.05 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2013-11-26 * "STMTTRN - UNION MARKET - HOUSNEW YORK - 47155 GROCERY STORE"
                  Liabilities:Credit-Card:Amex  -18.76 USD
                    date: 2013-11-26
                    ofx_fitid: "320133300285014247"
                    ofx_memo: "47155 GROCERY STORE"
                    ofx_name: "UNION MARKET - HOUSNEW YORK"
                    ofx_type: "STMTTRN"
                  Expenses:FIXME                 18.76 USD
                """,
            ),
            import_result(
                info={
                    'type': 'application/x-ofx',
                    'filename': source_path
                },
                entries=r"""
                2014-01-12 balance Liabilities:Credit-Card:Amex                    -2356.38 USD
                """,
            )
        ],
        training_examples=[],
    )
