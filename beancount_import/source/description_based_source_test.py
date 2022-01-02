import unittest

from beancount.core.amount import Amount
from beancount.core.data import Posting
from beancount.core.number import D

from .description_based_source import DescriptionBasedSource


class DescriptionBasedSourceTest(unittest.TestCase):
    def test_merged_posting_is_cleared(self):
        postings = [
            Posting(account="a", units=Amount(D(10), "USD"), cost=None,
                    price=None, flag=None, meta=None),
            Posting(account="a", units=Amount(D(10), "USD"), cost=None,
                    price=None, flag=None, meta={"merge": True}),
        ]
        source = DescriptionBasedSource(lambda s: None)
        self.assertEqual(source.is_posting_cleared(postings[0]), False)
        self.assertEqual(source.is_posting_cleared(postings[1]), True)
