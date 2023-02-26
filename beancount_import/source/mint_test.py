import os

import pytest

from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'mint'))

examples = [
    'test_basic',
    'test_training_examples',
    'test_invalid',
]


@pytest.mark.parametrize('name', examples)
def test_source(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.mint',
            'filename': os.path.join(testdata_dir, 'mint.csv'),
        },
        replacements=[(testdata_dir, '<testdata>')])

def test_currency():
    check_source_example(
        example_dir=os.path.join(testdata_dir, 'test_currency'),
        source_spec={
            'module': 'beancount_import.source.mint',
            'filename': os.path.join(testdata_dir, 'mint.csv'),
            'currency': 'CAD',
        },
        replacements=[(testdata_dir, '<testdata>')])
