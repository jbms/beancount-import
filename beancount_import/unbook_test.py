import unittest

from beancount.core.amount import Amount
from beancount.core.data import Posting
from beancount.core.number import D

from . import unbook


class UnbookTest(unittest.TestCase):
    def test_posting_no_merge(self):
        postings = [
            Posting(account="a", units=Amount(D(10), "USD"), cost=None,
                    price=None, flag=None, meta=None),
            Posting(account="a", units=Amount(D(20), "USD"), cost=None,
                    price=None, flag=None, meta=None),
        ]
        result = list(unbook.group_postings_by_meta(postings))
        assert len(result) == 2

    def test_posting_merge_first_posting_does_nothing(self):
        postings = [
            Posting(account="a", units=Amount(D(10), "USD"), cost=None,
                    price=None, flag=None, meta={"merge": True}),
            Posting(account="a", units=Amount(D(20), "USD"), cost=None,
                    price=None, flag=None, meta=None),
        ]
        result = list(unbook.group_postings_by_meta(postings))
        assert len(result) == 2

    def test_merge_posting_meta(self):
        postings = [
            Posting(account="a", units=Amount(D(10), "USD"), cost=None,
                    price=None, flag=None, meta=None),
            Posting(account="a", units=Amount(D(20), "USD"), cost=None,
                    price=None, flag=None, meta={"merge": True}),
        ]
        result = list(unbook.group_postings_by_meta(postings))
        assert len(result) == 1
        assert len(result[0]) == 2


if __name__ == '__main__':
    unittest.main()
