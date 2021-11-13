import os

import pytest

from .source_test import check_source_example
from beancount.ingest.importers.csv import Importer as CSVImporter, Col
from beancount.ingest.importer import ImporterProtocol
from beancount.parser.parser import parse_file

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'generic_importer'))

testdata_csv = os.path.join(testdata_dir, "csv")

examples = [
    'test_basic',
    'test_invalid',
    'test_training_examples'
]

importer = CSVImporter({Col.DATE: 'Date',
                        Col.NARRATION1: 'Description',
                        Col.AMOUNT: 'Amount',
                        },
                       'Assets:Bank',
                       'USD',
                       '"Date","Description","Amount"',
                       )


@pytest.mark.parametrize('name', examples)
def test_source(name: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.generic_importer_source',
            'directory': testdata_csv,
            'account': 'Assets:Bank',
            'importer': importer,
        },
        replacements=[(testdata_dir, '<testdata>')])


# The existing testcases use CSVImporter, which cannot handle costs.
# This importer instead loads the transaction from a beancount file.
class BeancountLedgerImporter(ImporterProtocol):
    def extract(self, file, existing_entries):
        entries, errors, options_map = parse_file(file.name)
        return entries
    def identify(self, file):
        return True


def test_cost():
    example_dir = os.path.join(testdata_dir, "test_cost")
    check_source_example(
        example_dir=example_dir,
        source_spec={
            'module': 'beancount_import.source.generic_importer_source',
            'directory': os.path.join(example_dir, "input"),
            'account': 'Assets:Bank',
            'importer': BeancountLedgerImporter(),
        },
        replacements=[(testdata_dir, '<testdata>')])
    
