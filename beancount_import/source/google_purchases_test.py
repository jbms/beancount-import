import os

import pytest

from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source',
        'google_purchases'))

examples = [
    'test_basic',
    'test_matching',
    'test_invalid',
]


@pytest.mark.parametrize('name', examples)
def test_source(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.google_purchases',
            'directory': testdata_dir,
            'link_prefix': 'google_purchase.',
            'time_zone': 'US/Pacific',
        },
        replacements=[(testdata_dir, '<testdata>')])
