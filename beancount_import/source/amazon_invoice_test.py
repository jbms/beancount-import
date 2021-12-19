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
def test_parsing_en_EN(name: str):
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


@pytest.mark.parametrize('name', [
    '256-0244967-2403944', # regular order
    '393-2608279-9292916', # Spar-Abo, payed with gift card 
    '898-5185906-0096901', # Spar-Abo
    '974-6135682-9358749', # several credit card transactions
    'D22-9220967-2566135', # digital order, audible subscription
    'D60-9825125-4795642', # digital order
    '399-5779972-5007935', # Direct Debit (Bankeinzug)
    '071-4816388-0694813', # gift card amazon
    '075-2225405-7594823', # gift card spotify
    '447-6209054-6766419', # charge up Amazon account
])
def test_parsing_de_DE(name: str):
    testdata_dir_locale = os.path.join(testdata_dir, 'de_DE')
    source_path = os.path.join(testdata_dir_locale, name + '.html')
    invoice = amazon_invoice.parse_invoice(source_path, locale=amazon_invoice.LOCALES['de_DE']())
    json_path = os.path.join(testdata_dir_locale, name + '.json')
    expected = json.load(
        open(json_path, 'r'), object_pairs_hook=collections.OrderedDict)
    expected_str = json.dumps(expected, indent=4)
    actual = amazon_invoice.to_json(invoice)
    actual_str = json.dumps(actual, indent=4)
    if expected_str != actual_str:
        print(actual_str)
    assert expected_str == actual_str
