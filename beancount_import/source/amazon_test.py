import os
from typing import Dict, Any

import pytest

from .amazon_invoice_test import testdata_dir
from .source_test import check_source_example
from ..source import SourceSpec

source_spec_without_posttax_accounts = {
    'module': 'beancount_import.source.amazon',
    'directory': testdata_dir,
    'amazon_account': 'name@domain.com',
}

source_spec_with_posttax_accounts = {
    **source_spec_without_posttax_accounts,
    'posttax_adjustment_accounts': {
        'Gift Card Amount': 'Assets:Gift-Cards:Amazon',
        'Rewards Points': 'Income:Amazon:Cashback',
    },
}  # type: Dict[str, Any]

examples = [
    ('test_basic', source_spec_with_posttax_accounts),
    ('test_credit_card_transactions', source_spec_with_posttax_accounts),
    ('test_cleared_and_invalid', source_spec_with_posttax_accounts),
    ('test_prediction', source_spec_without_posttax_accounts),
]


@pytest.mark.parametrize('name,source_spec', examples)
def test_source(name: str, source_spec: SourceSpec):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec=source_spec,
        replacements=[(testdata_dir, '<testdata>')])
