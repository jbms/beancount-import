import os

import pytest

from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source',
        'paypal'))

examples = [
    'test_basic',
    'test_matching',
]


@pytest.mark.parametrize('name', examples)
def test_source_en_US(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.paypal',
            'directory': testdata_dir,
            'assets_account': 'Assets:Paypal',
            'fee_account': 'Expenses:Financial:Paypal:Fees',
            'prefix': 'paypal',
        },
        replacements=[(testdata_dir, '<testdata>')])


# locale DE
examples = [
    'test_basic',
    # 'test_matching',
]


@pytest.mark.parametrize('name', examples)
def test_source_de_DE(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, 'de_DE', name),
        source_spec={
            'module': 'beancount_import.source.paypal',
            'directory': os.path.join(testdata_dir, 'de_DE'),
            'assets_account': 'Assets:Paypal',
            'fee_account': 'Expenses:Financial:Paypal:Fees',
            'prefix': 'paypal',
            'locale': 'de_DE'
        },
        replacements=[(testdata_dir, '<testdata>')])