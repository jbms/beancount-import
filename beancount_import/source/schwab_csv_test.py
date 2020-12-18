import glob
import os

import pytest

from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'schwab_csv'))

examples = [
    'test_basic',
]


@pytest.mark.parametrize('name', examples)
def test_source(name: str):
    example_dir = os.path.join(testdata_dir, name)
    check_source_example(
        example_dir=example_dir,
        source_spec={
            'module': 'beancount_import.source.schwab_csv',
            "transaction_csv_filenames": sorted(glob.glob(f"{example_dir}/transactions/*.CSV")),
            "position_csv_filenames": sorted(glob.glob(f"{example_dir}/positions/*.CSV")),
        },
        replacements=[(testdata_dir, '<testdata>')],
    )

