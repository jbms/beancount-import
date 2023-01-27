import os
import shutil
import sys

import pytest

from . import ultipro_google
from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'ultipro_google'))

@pytest.mark.skipif(
        shutil.which('pdftotext') is None,
        reason='the pdftotext program must be available')
@pytest.mark.skipif(
        sys.platform.startswith('win'),
        reason='parsing does not work with Windows newlines')
@pytest.mark.parametrize('name', ['test_basic', 'test_20211223', 'test_20220107'])
def test_source(name: str):
    example_dir = os.path.join(testdata_dir, name)
    check_source_example(
        example_dir=example_dir,
        # source_spec is the example in ultipro_google.py.
        source_spec=dict(
            module='beancount_import.source.ultipro_google',
            company_name='Google',
            key_prefix='google_payroll',
            currency='USD',
            directory=example_dir,
            rules={
                'Earnings': [
                    ('Regular Pay', 'Income:Google:Salary'),
                    ('Annual Bonus', 'Income:Google:Annual-Bonus'),
                    ('HSA ER Seed', 'Income:Google:HSA'),
                ],
                'Deductions': [
                    ('Dental', 'Expenses:Health:Dental:Insurance'),
                    ('Medical', 'Expenses:Health:Medical:Insurance'),
                ],
                'Taxes': [
                    ('Federal Income Tax',
                     'Income:Expenses:Taxes:TY{year:04d}:Federal:Income'),
                    ('Employee Medicare',
                     'Income:Expenses:Taxes:TY{year:04d}:Federal:Medicare'),
                    ('Social Security Employee Tax',
                     'Income:Expenses:Taxes:TY{year:04d}:Federal:Social-Security'),
                    ('CA State Income Tax',
                     'Income:Expenses:Taxes:TY{year:04d}:California:Income'),
                    ('CA Private Disability Employee',
                     'Income:Expenses:Taxes:TY{year:04d}:California:Disability'),
                ],
                'Net Pay Distribution': [
                    ('x+1234', 'Assets:Checking:My-Bank'),
                ],
            }),
        replacements=[(testdata_dir, '<testdata>')])
