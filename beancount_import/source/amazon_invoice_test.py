import collections
import json
import os

import pytest

from . import amazon_invoice

testdata_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'amazon'))


@pytest.mark.parametrize('name', [
    '277-5312419-9119541',
    '781-8429198-6057878',
    '166-7926740-5141621',
    'D56-5204779-4181560',
])
def test_parsing(name: str):
    source_path = os.path.join(testdata_dir, name + '.html')
    invoice = amazon_invoice.parse_invoice(source_path)
    json_path = os.path.join(testdata_dir, name + '.json')
    expected = json.load(
        open(json_path, 'r'), object_pairs_hook=collections.OrderedDict)
    expected_str = json.dumps(expected, indent=4)
    actual = amazon_invoice.to_json(invoice)
    actual_str = json.dumps(actual, indent=4)
    if expected_str != actual_str:
        print(actual_str)
    assert expected_str == actual_str
