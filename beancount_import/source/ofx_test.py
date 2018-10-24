import os

import pytest

from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'ofx'))

examples = [
    ('test_vanguard_basic', 'vanguard.ofx'),
    ('test_vanguard_matching', 'vanguard.ofx'),
    ('test_vanguard_invalid', 'vanguard.ofx'),
    ('test_vanguard_with_cash_account', 'vanguard.ofx'),
    ('test_vanguard_with_cash_account_matching_missing_transfer',
     'vanguard.ofx'),
    ('test_vanguard_with_cash_account_matching_missing_primary',
     'vanguard.ofx'),
    ('test_vanguard401k', 'vanguard401k.ofx'),
    ('test_fidelity_savings', 'fidelity-savings.ofx'),
    ('test_suncorp', 'suncorp.ofx'),
    ('test_checking', 'checking.ofx'),
    ('test_td_ameritrade', 'td_ameritrade.ofx'),
    ('test_anzcc', 'anzcc.ofx'),
    ('test_multiple_accounts', 'multiple_accounts.ofx'),
    ('test_bank_medium', 'bank_medium.ofx'),
    ('test_investment_401k', 'investment_401k.ofx'),
    ('test_investment_buy_sell_income', 'investment_buy_sell_income.ofx'),
    ('test_vanguard_roth_ira', 'vanguard_roth_ira.ofx'),
    ('test_vanguard_roth_ira_matching', 'vanguard_roth_ira.ofx'),
    ('test_checking2', 'checking2.ofx'),
    ('test_checking2_matching', 'checking2.ofx'),
    ('test_amex', 'amex.ofx'),
]


@pytest.mark.parametrize('name,ofx_filename', examples)
def test_source(name: str, ofx_filename: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [os.path.join(testdata_dir, ofx_filename)],
        },
        replacements=[(testdata_dir, '<testdata>')])
