import unittest

from beancount.core.amount import Amount
from beancount.core.data import Posting
from beancount.core.number import D

from .description_based_source import (DescriptionBasedSource,
                                       get_posting_source_descs)


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

    def test_missing_posting_date_error(self):
        posting = Posting(account="a", units=Amount(D(10), "USD"), cost=None,
                          price=None, flag=None,
                          meta={"source_desc": "Coffee"})
        with self.assertRaisesRegex(RuntimeError,
                                    "Posting date is missing on entry"):
            list(get_posting_source_descs(posting))
