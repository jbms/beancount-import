import os
import datetime

from beancount.core.amount import Amount
from beancount.core.number import D

from .source_test import check_source, import_result
from ..training import PredictionInput

testdata_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'venmo'))

transactions_filename = os.path.join(testdata_dir, 'transactions.csv')
balances_filename = os.path.join(testdata_dir, 'balances.csv')


def test_basic(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.venmo',
            'directory': testdata_dir,
            'assets_account': 'Assets:Venmo',
        },
        accounts=frozenset(['Assets:Venmo']),
        pending=[
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 2
                },
                entries=r"""
                2017-04-05 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 1
                },
                entries=r"""
                2017-04-25 * "Tom Johnson" "Transfer" ^venmo.2394198259925614643
                  Assets:Venmo     220.00 USD
                    date: 2017-04-25
                    venmo_account_description: "Visa Debit *1559"
                    venmo_description: "Tutoring"
                    venmo_payee: "Tom Johnson"
                    venmo_transfer_id: "2394198259925614643"
                    venmo_type: "Payment"
                  Expenses:FIXME  -220.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 1
                },
                entries=r"""
                2017-04-25 * "Tom Johnson" "Tutoring" ^venmo.2394198259925614643
                  Assets:Venmo    -220.00 USD
                    date: 2017-04-25
                    venmo_description: "Tutoring"
                    venmo_payee: "Tom Johnson"
                    venmo_payment_id: "2394198259925614643"
                    venmo_type: "Payment"
                  Expenses:FIXME   220.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 2
                },
                entries=r"""
                2017-07-04 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 3
                },
                entries=r"""
                2017-07-04 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 2
                },
                entries=r"""
                2017-09-02 * "Maria Anderson" "Transfer" ^venmo.3483695350643256240
                  Assets:Venmo     559.00 USD
                    date: 2017-09-02
                    venmo_account_description: "Visa Debit *1559"
                    venmo_description: "Security deposit"
                    venmo_payee: "Maria Anderson"
                    venmo_transfer_id: "3483695350643256240"
                    venmo_type: "Payment"
                  Expenses:FIXME  -559.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 2
                },
                entries=r"""
                2017-09-02 * "Maria Anderson" "Security deposit" ^venmo.3483695350643256240
                  Assets:Venmo    -559.00 USD
                    date: 2017-09-02
                    venmo_description: "Security deposit"
                    venmo_payee: "Maria Anderson"
                    venmo_payment_id: "3483695350643256240"
                    venmo_type: "Payment"
                  Expenses:FIXME   559.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 3
                },
                entries=r"""
                2017-09-06 * "Sally Smith" "Rent"
                  Assets:Venmo     1150.00 USD
                    date: 2017-09-06
                    venmo_description: "Rent"
                    venmo_payer: "Sally Smith"
                    venmo_payment_id: "0454063333607815882"
                    venmo_type: "Payment"
                  Expenses:FIXME  -1150.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 4
                },
                entries=r"""
                2017-09-06 * "Venmo" "Transfer"
                  Assets:Venmo    -1150.00 USD
                    date: 2017-09-06
                    venmo_account_description: "Visa Debit *8967"
                    venmo_transfer_id: "355418184"
                    venmo_type: "Standard Transfer"
                  Expenses:FIXME   1150.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 3
                },
                entries=r"""
                2017-10-02 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 4
                },
                entries=r"""
                2017-10-02 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 4
                },
                entries=r"""
                2017-12-31 balance Assets:Venmo                                    1528.25 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 5
                },
                entries=r"""
                2017-12-31 balance Assets:Venmo                                    1528.25 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 5
                },
                entries=r"""
                2018-01-21 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 5
                },
                entries=r"""
                2018-04-05 * "Maria Anderson" "Transfer" ^venmo.3641977597632667950
                  Assets:Venmo     120.00 USD
                    date: 2018-04-05
                    venmo_account_description: "Visa Debit *1559"
                    venmo_description: "Utilities"
                    venmo_payee: "Maria Anderson"
                    venmo_transfer_id: "3641977597632667950"
                    venmo_type: "Charge"
                  Expenses:FIXME  -120.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 5
                },
                entries=r"""
                2018-04-05 * "Maria Anderson" "Utilities" ^venmo.3641977597632667950
                  Assets:Venmo    -120.00 USD
                    date: 2018-04-05
                    venmo_description: "Utilities"
                    venmo_payee: "Maria Anderson"
                    venmo_payment_id: "3641977597632667950"
                    venmo_type: "Charge"
                  Expenses:FIXME   120.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 6
                },
                entries=r"""
                2018-04-17 * "Maria Anderson" "Transfer" ^venmo.4354205292885612406
                  Assets:Venmo     3.00 USD
                    date: 2018-04-17
                    venmo_account_description: "Visa Debit *1559"
                    venmo_description: "Bagels"
                    venmo_payee: "Maria Anderson"
                    venmo_transfer_id: "4354205292885612406"
                    venmo_type: "Payment"
                  Expenses:FIXME  -3.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 6
                },
                entries=r"""
                2018-04-17 * "Maria Anderson" "Bagels" ^venmo.4354205292885612406
                  Assets:Venmo    -3.00 USD
                    date: 2018-04-17
                    venmo_description: "Bagels"
                    venmo_payee: "Maria Anderson"
                    venmo_payment_id: "4354205292885612406"
                    venmo_type: "Payment"
                  Expenses:FIXME   3.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 7
                },
                entries=r"""
                2018-05-01 * "Sally Smith" "Rent"
                  Assets:Venmo     1423.73 USD
                    date: 2018-05-01
                    venmo_description: "Rent"
                    venmo_payer: "Sally Smith"
                    venmo_payment_id: "3083645406127028805"
                    venmo_type: "Payment"
                  Expenses:FIXME  -1423.73 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 8
                },
                entries=r"""
                2018-05-19 * "Maria Anderson" "Utilities"
                  Assets:Venmo    -82.50 USD
                    date: 2018-05-19
                    venmo_description: "Utilities"
                    venmo_payee: "Maria Anderson"
                    venmo_payment_id: "1326234690911981511"
                    venmo_type: "Charge"
                  Expenses:FIXME   82.50 USD
                
                """,
                unknown_account_prediction_inputs=[
                    PredictionInput(
                        source_account='Assets:Venmo',
                        amount=Amount(D('-82.50'), 'USD'),
                        date=datetime.date(2018, 5, 19),
                        key_value_pairs={
                            'venmo_type': 'Charge',
                            'venmo_description': 'Utilities',
                            'venmo_payee': 'Maria Anderson',
                        })
                ],
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': transactions_filename,
                    'line': 9
                },
                entries=r"""
                2018-05-19 * "Venmo" "Transfer"
                  Assets:Venmo    -1341.23 USD
                    date: 2018-05-19
                    venmo_account_description: "Visa Debit *8967"
                    venmo_transfer_id: "7315187729"
                    venmo_type: "Standard Transfer"
                  Expenses:FIXME   1341.23 USD
                
                """,
                unknown_account_prediction_inputs=[
                    PredictionInput(
                        source_account='Assets:Venmo',
                        amount=Amount(D('-1341.23'), 'USD'),
                        date=datetime.date(2018, 5, 19),
                        key_value_pairs={
                            'venmo_type': 'Standard Transfer',
                            'venmo_account_description': 'Visa Debit *8967',
                        })
                ],
            )
        ],
    )


def test_matching(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.venmo',
            'directory': testdata_dir,
            'assets_account': 'Assets:Venmo',
        },
        accounts=frozenset(['Assets:Venmo']),
        journal_contents='''
        plugin "beancount.plugins.auto_accounts"

        2017-04-25 * "Tom Johnson" "Transfer" ^venmo.2394198259925614643
          Assets:Venmo     220.00 USD
            date: 2017-04-25
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Tutoring"
            venmo_payee: "Tom Johnson"
            venmo_transfer_id: "2394198259925614643"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Checking  -220.00 USD

        2017-04-25 * "Tom Johnson" "Tutoring" ^venmo.2394198259925614643
          Assets:Venmo    -220.00 USD
            date: 2017-04-25
            venmo_description: "Tutoring"
            venmo_payee: "Tom Johnson"
            venmo_payment_id: "2394198259925614643"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:Tutoring   220.00 USD

        2017-09-02 * "Maria Anderson" "Transfer" ^venmo.3483695350643256240
          Assets:Venmo     559.00 USD
            date: 2017-09-02
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Security deposit"
            venmo_payee: "Maria Anderson"
            venmo_transfer_id: "3483695350643256240"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Checking  -559.00 USD

        2017-09-02 * "Maria Anderson" "Security deposit" ^venmo.3483695350643256240
          Assets:Venmo    -559.00 USD
            date: 2017-09-02
            venmo_description: "Security deposit"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "3483695350643256240"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Security-Deposit   559.00 USD

        2017-09-06 * "Sally Smith" "Rent"
          Assets:Venmo     1150.00 USD
            date: 2017-09-06
            venmo_description: "Rent"
            venmo_payer: "Sally Smith"
            venmo_payment_id: "0454063333607815882"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:Rent  -1150.00 USD

        2017-09-06 * "Venmo" "Transfer"
          Assets:Venmo    -1150.00 USD
            date: 2017-09-06
            venmo_account_description: "Visa Debit *8967"
            venmo_transfer_id: "355418184"
            venmo_type: "Standard Transfer"
            cleared: TRUE
          Assets:Checking   1150.00 USD

        2018-04-05 * "Maria Anderson" "Transfer" ^venmo.3641977597632667950
          Assets:Venmo     120.00 USD
            date: 2018-04-05
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Utilities"
            venmo_payee: "Maria Anderson"
            venmo_transfer_id: "3641977597632667950"
            venmo_type: "Charge"
            cleared: TRUE
          Assets:Checking  -120.00 USD

        2018-04-05 * "Maria Anderson" "Utilities" ^venmo.3641977597632667950
          Assets:Venmo    -120.00 USD
            date: 2018-04-05
            venmo_description: "Utilities"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "3641977597632667950"
            venmo_type: "Charge"
            cleared: TRUE
          Expenses:Utilities   120.00 USD

        2018-04-17 * "Maria Anderson" "Transfer" ^venmo.4354205292885612406
          Assets:Venmo     3.00 USD
            date: 2018-04-17
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Bagels"
            venmo_payee: "Maria Anderson"
            venmo_transfer_id: "4354205292885612406"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Checking  -3.00 USD

        2018-04-17 * "Maria Anderson" "Bagels" ^venmo.4354205292885612406
          Assets:Venmo    -3.00 USD
            date: 2018-04-17
            venmo_description: "Bagels"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "4354205292885612406"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:Groceries   3.00 USD

        2018-05-01 * "Sally Smith" "Rent"
          Assets:Venmo     1423.73 USD
            date: 2018-05-01
            venmo_description: "Rent"
            venmo_payer: "Sally Smith"
            venmo_payment_id: "3083645406127028805"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:Rent  -1423.73 USD

        2018-05-19 * "Maria Anderson" "Utilities"
          Assets:Venmo    -82.50 USD
            date: 2018-05-19
            venmo_description: "Utilities"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "1326234690911981511"
            venmo_type: "Charge"
            cleared: TRUE
          Expenses:Utilities   82.50 USD

        2018-05-19 * "Venmo" "Transfer"
          Assets:Venmo    -1341.23 USD
            date: 2018-05-19
            venmo_account_description: "Visa Debit *8967"
            venmo_transfer_id: "7315187729"
            venmo_type: "Standard Transfer"
            cleared: TRUE
          Assets:Checking   1341.23 USD

        ''',
        pending=[
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 2
                },
                entries=r"""
                2017-04-05 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 2
                },
                entries=r"""
                2017-07-04 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 3
                },
                entries=r"""
                2017-07-04 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 3
                },
                entries=r"""
                2017-10-02 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 4
                },
                entries=r"""
                2017-10-02 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 4
                },
                entries=r"""
                2017-12-31 balance Assets:Venmo                                    1528.25 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 5
                },
                entries=r"""
                2017-12-31 balance Assets:Venmo                                    1528.25 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 5
                },
                entries=r"""
                2018-01-21 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
        ],
        training_examples=[
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('220.00'), 'USD'),
                date=datetime.date(2017, 4, 25),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_account_description': 'Visa Debit *1559'
                }), 'Assets:Checking'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('-220.00'), 'USD'),
                date=datetime.date(2017, 4, 25),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_description': 'Tutoring',
                    'venmo_payee': 'Tom Johnson'
                }), 'Expenses:Tutoring'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('559.00'), 'USD'),
                date=datetime.date(2017, 9, 2),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_account_description': 'Visa Debit *1559'
                }), 'Assets:Checking'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('-559.00'), 'USD'),
                date=datetime.date(2017, 9, 2),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_description': 'Security deposit',
                    'venmo_payee': 'Maria Anderson'
                }), 'Assets:Security-Deposit'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('1150.00'), 'USD'),
                date=datetime.date(2017, 9, 6),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_description': 'Rent',
                    'venmo_payer': 'Sally Smith'
                }), 'Expenses:Rent'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('-1150.00'), 'USD'),
                date=datetime.date(2017, 9, 6),
                key_value_pairs={
                    'venmo_type': 'Standard Transfer',
                    'venmo_account_description': 'Visa Debit *8967'
                }), 'Assets:Checking'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('120.00'), 'USD'),
                date=datetime.date(2018, 4, 5),
                key_value_pairs={
                    'venmo_type': 'Charge',
                    'venmo_account_description': 'Visa Debit *1559'
                }), 'Assets:Checking'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('-120.00'), 'USD'),
                date=datetime.date(2018, 4, 5),
                key_value_pairs={
                    'venmo_type': 'Charge',
                    'venmo_description': 'Utilities',
                    'venmo_payee': 'Maria Anderson'
                }), 'Expenses:Utilities'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('3.00'), 'USD'),
                date=datetime.date(2018, 4, 17),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_account_description': 'Visa Debit *1559'
                }), 'Assets:Checking'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('-3.00'), 'USD'),
                date=datetime.date(2018, 4, 17),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_description': 'Bagels',
                    'venmo_payee': 'Maria Anderson'
                }), 'Expenses:Groceries'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('1423.73'), 'USD'),
                date=datetime.date(2018, 5, 1),
                key_value_pairs={
                    'venmo_type': 'Payment',
                    'venmo_description': 'Rent',
                    'venmo_payer': 'Sally Smith'
                }), 'Expenses:Rent'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('-82.50'), 'USD'),
                date=datetime.date(2018, 5, 19),
                key_value_pairs={
                    'venmo_type': 'Charge',
                    'venmo_description': 'Utilities',
                    'venmo_payee': 'Maria Anderson'
                }), 'Expenses:Utilities'),
            (PredictionInput(
                source_account='Assets:Venmo',
                amount=Amount(D('-1341.23'), 'USD'),
                date=datetime.date(2018, 5, 19),
                key_value_pairs={
                    'venmo_type': 'Standard Transfer',
                    'venmo_account_description': 'Visa Debit *8967'
                }), 'Assets:Checking'),
        ],
    )


def test_invalid_references(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.venmo',
            'directory': testdata_dir,
            'assets_account': 'Assets:Venmo',
        },
        accounts=frozenset(['Assets:Venmo']),
        journal_contents='''
        plugin "beancount.plugins.auto_accounts"

        2017-04-25 * "Tom Johnson" "Transfer" ^venmo.2394198259925614643
          Assets:Venmo     220.00 USD
            date: 2017-04-25
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Tutoring"
            venmo_payee: "Tom Johnson"
            venmo_transfer_id: "2394198259925614643"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Checking  -220.00 USD

        2017-04-25 * "Tom Johnson" "Tutoring" ^venmo.2394198259925614643
          Assets:Venmo    -220.00 USD
            date: 2017-04-25
            venmo_description: "Tutoring"
            venmo_payee: "Tom Johnson"
            venmo_payment_id: "2394198259925614643"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:Tutoring   220.00 USD

        2017-09-02 * "Maria Anderson" "Transfer" ^venmo.3483695350643256240
          Assets:Venmo     559.00 USD
            date: 2017-09-02
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Security deposit"
            venmo_payee: "Maria Anderson"
            venmo_transfer_id: "3483695350643256240"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Checking  -559.00 USD

        2017-09-02 * "Maria Anderson" "Security deposit" ^venmo.3483695350643256240
          Assets:Venmo    -559.00 USD
            date: 2017-09-02
            venmo_description: "Security deposit"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "3483695350643256240"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Security-Deposit   559.00 USD

        2017-09-06 * "Sally Smith" "Rent"
          Assets:Venmo     1150.00 USD
            date: 2017-09-06
            venmo_description: "Rent"
            venmo_payer: "Sally Smith"
            venmo_payment_id: "0454063333607815882"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:Rent  -1150.00 USD

        2017-09-06 * "Venmo" "Transfer"
          Assets:Venmo    -1150.00 USD
            date: 2017-09-06
            venmo_account_description: "Visa Debit *8967"
            venmo_transfer_id: "355418184"
            venmo_type: "Standard Transfer"
            cleared: TRUE
          Assets:Checking   1150.00 USD

        2018-04-05 * "Maria Anderson" "Transfer" ^venmo.3641977597632667950
          Assets:Venmo     120.00 USD
            date: 2018-04-05
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Utilities"
            venmo_payee: "Maria Anderson"
            venmo_transfer_id: "3641977597632667950"
            venmo_type: "Charge"
            cleared: TRUE
          Assets:Checking  -120.00 USD

        2018-04-05 * "Maria Anderson" "Utilities" ^venmo.3641977597632667950
          Assets:Venmo    -120.00 USD
            date: 2018-04-05
            venmo_description: "Utilities"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "3641977597632667950"
            venmo_type: "Charge"
            cleared: TRUE
          Expenses:Utilities   120.00 USD

        2018-04-17 * "Maria Anderson" "Transfer" ^venmo.4354205292885612406
          Assets:Venmo     3.00 USD
            date: 2018-04-17
            venmo_account_description: "Visa Debit *1559"
            venmo_description: "Bagels"
            venmo_payee: "Maria Anderson"
            venmo_transfer_id: "4354205292885612406"
            venmo_type: "Payment"
            cleared: TRUE
          Assets:Checking  -3.00 USD

        2018-04-17 * "Maria Anderson" "Bagels" ^venmo.4354205292885612406
          Assets:Venmo    -3.00 USD
            date: 2018-04-17
            venmo_description: "Bagels"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "4354205292885612406"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:Groceries   3.00 USD

        2018-05-01 * "Sally Smith" "Rent"
          Assets:Venmo     1423.73 USD
            date: 2018-05-01
            venmo_description: "Rent"
            venmo_payer: "Sally Smith"
            venmo_payment_id: "3083645406127028805"
            venmo_type: "Payment"
            cleared: TRUE
          Expenses:FIXME  -1423.73 USD

        2018-05-19 * "Maria Anderson" "Utilities"
          Assets:Venmo    -82.50 USD
            date: 2018-05-19
            venmo_description: "Utilities"
            venmo_payee: "Maria Anderson"
            venmo_payment_id: "1326234690911981511"
            venmo_type: "Charge"
            cleared: TRUE
          Expenses:FIXME   82.50 USD

        2018-05-19 * "Venmo" "Transfer"
          invalid_id: "a"
          Assets:Venmo    -1341.23 USD
            invalid_id: "p"
            date: 2018-05-19
            venmo_account_description: "Visa Debit *8967"
            venmo_transfer_id: "7315187729"
            venmo_type: "Standard Transfer"
            cleared: TRUE
          Expenses:FIXME   1341.23 USD

        2018-05-19 * "Venmo" "Transfer"
          invalid_id: "b"
          Assets:Venmo    -1341.23 USD
            invalid_id: "p"
            date: 2018-05-19
            venmo_account_description: "Visa Debit *8967"
            venmo_transfer_id: "7315187729"
            venmo_type: "Standard Transfer"
            cleared: TRUE
          Expenses:FIXME   1341.23 USD
        ''',
        pending=[
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 2
                },
                entries=r"""
                2017-04-05 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 2
                },
                entries=r"""
                2017-07-04 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 3
                },
                entries=r"""
                2017-07-04 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 3
                },
                entries=r"""
                2017-10-02 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 4
                },
                entries=r"""
                2017-10-02 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 4
                },
                entries=r"""
                2017-12-31 balance Assets:Venmo                                    1528.25 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 5
                },
                entries=r"""
                2017-12-31 balance Assets:Venmo                                    1528.25 USD
                
                """,
            ),
            import_result(
                info={
                    'type': 'text/csv',
                    'filename': balances_filename,
                    'line': 5
                },
                entries=r"""
                2018-01-21 balance Assets:Venmo                                    0.00 USD
                
                """,
            ),
        ],
        invalid_references=[
            (1, [('a', 'p'), ('b', 'p')]),
        ],
    )
