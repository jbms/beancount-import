import datetime

from beancount.core.amount import Amount
from beancount.core.number import D

from . import training
from .classifier import NearestNeighborClassifier

ACCOUNT = 'Liabilities:Card'


def _ofx(name: str,
         date: datetime.date = datetime.date(2020, 1, 1),
         amount: str = '-10 USD',
         source_account: str = ACCOUNT) -> training.PredictionInput:
    return training.PredictionInput(
        source_account=source_account,
        amount=Amount.from_string(amount),
        date=date,
        key_value_pairs={'ofx_name': name})


def _item(description: str,
          date: datetime.date = datetime.date(2020, 1, 1),
          amount: str = '10 USD') -> training.PredictionInput:
    return training.PredictionInput(
        source_account='',
        amount=Amount.from_string(amount),
        date=date,
        key_value_pairs={
            'amazon_account': ['a@example.com'],
            'amazon_item_description': [description],
        })


def test_predicts_most_similar_merchant():
    c = NearestNeighborClassifier()
    c.add(_ofx('SAFEWAY #1234'), 'Expenses:Groceries')
    c.add(_ofx('SHELL OIL 5555'), 'Expenses:Auto:Gas')
    c.add(_ofx('TST* PIZZA PLACE'), 'Expenses:Dining')
    assert c.predict(_ofx('SAFEWAY #9876')) == 'Expenses:Groceries'
    assert type(c.predict(_ofx('SAFEWAY #9876'))) is str
    assert c.predict(_ofx('SHELL OIL 1111')) == 'Expenses:Auto:Gas'


def test_learns_incrementally():
    c = NearestNeighborClassifier()
    c.add(_ofx('SAFEWAY'), 'Expenses:Groceries')
    assert c.predict(_ofx('ACME HARDWARE')) == 'Expenses:Groceries'
    c.add(_ofx('ACME HARDWARE'), 'Expenses:Home')
    assert c.predict(_ofx('ACME HARDWARE')) == 'Expenses:Home'


def test_recent_choice_wins():
    c = NearestNeighborClassifier()
    for year in (2015, 2016, 2017):
        c.add(_ofx('NETFLIX', datetime.date(year, 1, 1)), 'Expenses:Entertainment')
    for day in (1, 2):
        c.add(_ofx('NETFLIX', datetime.date(2020, 1, day)), 'Expenses:Subscriptions')
    assert c.predict(_ofx('NETFLIX', datetime.date(2020, 2, 1))) == 'Expenses:Subscriptions'


def test_keeps_card_digits():
    c = NearestNeighborClassifier()
    c.add(_ofx('PAYMENT TO CARD ENDING IN 1111', source_account='Assets:Checking'), 'Liabilities:A')
    c.add(_ofx('PAYMENT TO CARD ENDING IN 2222', source_account='Assets:Checking'), 'Liabilities:B')
    assert c.predict(_ofx('PAYMENT TO CARD ENDING IN 2222', source_account='Assets:Checking')) == 'Liabilities:B'


def test_only_compares_same_kind():
    card = training.PredictionInput(
        source_account='',
        amount=Amount.from_string('-10 USD'),
        date=datetime.date(2020, 1, 1),
        key_value_pairs={
            'amazon_account': ['a@example.com'],
            'amazon_credit_card_description': ['Visa ending in 1234'],
        })
    item = ('amazon_item_description', )
    c = NearestNeighborClassifier()
    for _ in range(20):
        c.add(card, 'Liabilities:Visa', kind=('amazon_credit_card_description', ))
    c.add(_item('Organic Bananas', amount='500 USD'), 'Expenses:Groceries', kind=item)
    # The new item shares no words with past items; its closest matches are
    # the card payments (same account, same amount), which must not count.
    query = _item('Garden Hose 50ft', amount='-10 USD')
    assert c.predict(query, kind=item) == 'Expenses:Groceries'
    assert c.predict(query) == 'Liabilities:Visa'  # without kinds


def test_never_predicts_source_account():
    c = NearestNeighborClassifier()
    c.add(_ofx('PAYMENT THANK YOU', source_account='Assets:Checking', amount='-50 USD'), ACCOUNT)
    assert c.predict(_ofx('STARBUCKS', source_account=ACCOUNT)) is None


def test_encoder_blend_smoke():
    c = NearestNeighborClassifier()
    for i in range(5):
        c.add(_ofx('SAFEWAY STORE %d' % i), 'Expenses:Groceries')
        c.add(_ofx('SHELL OIL %d' % i), 'Expenses:Auto:Gas')
    c.fit_encoder(steps=50)
    embeddings = c.encoder.transform([_ofx('SAFEWAY STORE 7')])
    assert abs(float((embeddings**2).sum()) - 1) < 1e-4
    assert c.predict(_ofx('SAFEWAY STORE 9')) == 'Expenses:Groceries'
    c.add(_ofx('CORNER GROCER'), 'Expenses:Groceries')
    assert c.predict(_ofx('CORNER GROCER')) == 'Expenses:Groceries'


def test_cardholder_prefix_is_not_a_merchant():
    def amex(memo):
        return training.PredictionInput(
            source_account='Liabilities:AMEX', amount=Amount.from_string('-10 USD'),
            date=datetime.date(2020, 1, 1), key_value_pairs={'ofx_memo': memo})
    c = NearestNeighborClassifier()
    for ref in ('aaaa', 'bbbb', 'cccc', 'dddd', 'ffff'):
        c.add(amex('JANE DOE-12345-%s squareup.com/receipts AplPay CITY TAXI Anytown' % ref),
              'Expenses:Public-Transportation')
    c.add(amex('CORNER MARKET'), 'Expenses:Groceries')
    # Left in, the shared cardholder prefix makes every such memo look like
    # the taxi rides.
    query = amex('JANE DOE-12345-eeee squareup.com/receipts GglPay CORNER MARKET')
    assert c.predict(query) == 'Expenses:Groceries'


def test_reused_vectors_match_recomputed():
    """A classifier rebuilt after a journal reload takes vectors of unchanged
    examples from the previous one; the index must be identical to one
    computed from scratch, whatever was added, removed or reordered."""
    import numpy as np

    old = [(_ofx('SAFEWAY STORE %d' % i), 'Expenses:Groceries') for i in range(6)]
    old += [(_ofx('SHELL OIL %d' % i), 'Expenses:Auto:Gas') for i in range(6)]
    old += [(_item('Paper towels'), 'Expenses:Household')]
    previous = NearestNeighborClassifier()
    for example, target in old:
        previous.add(example, target)
    previous.fit_encoder(steps=50)
    previous.prepare()
    # Added after the last index build: has no vectors yet.
    previous.add(_ofx('UNINDEXED CAFE'), 'Expenses:Dining')

    # The reloaded journal dropped one example, changed one, reordered the
    # rest and added new ones (one a duplicate of an old example).
    new = old[1:5] + [(_ofx('SAFEWAY STORE 5', amount='-11 USD'),
                       'Expenses:Groceries')] + old[6:][::-1]
    new += [(_ofx('CORNER GROCER'), 'Expenses:Groceries'),
            (_ofx('UNINDEXED CAFE'), 'Expenses:Dining'), old[2]]

    computed = []
    encoder = previous.encoder
    transform = encoder.transform
    def counting_transform(examples):
        computed.append(('embed', len(examples)))
        return transform(examples)
    encoder.transform = counting_transform

    def build(reuse):
        c = NearestNeighborClassifier()
        if reuse:
            c.reuse_vectors_of(previous)
        hash_rows = c._hasher.transform
        def counting_hash(rows):
            rows = list(rows)
            computed.append(('hash', len(rows)))
            return hash_rows(rows)
        c._hasher.transform = counting_hash
        for example, target in new:
            c.add(example, target)
        c.set_encoder(encoder)
        c.prepare()
        return c

    reused = build(True)
    # Only the changed and the never-indexed examples are computed.
    assert computed == [('hash', 3), ('embed', 3)]
    fresh = build(False)
    assert (reused._matrix != fresh._matrix).nnz == 0
    np.testing.assert_array_equal(reused._embeddings, fresh._embeddings)
    # Later additions are computed, not looked up.
    for c in (reused, fresh):
        c.add(old[0][0], 'Expenses:Other')
        c.prepare()
    assert (reused._matrix != fresh._matrix).nnz == 0
    np.testing.assert_array_equal(reused._embeddings, fresh._embeddings)
    query = _ofx('SAFEWAY STORE 9')
    assert reused.predict_scores(query) == fresh.predict_scores(query)


def test_reused_vectors_ignore_other_encoder():
    import numpy as np

    previous = NearestNeighborClassifier()
    for i in range(4):
        previous.add(_ofx('SAFEWAY STORE %d' % i), 'Expenses:Groceries')
        previous.add(_ofx('SHELL OIL %d' % i), 'Expenses:Auto:Gas')
    previous.fit_encoder(steps=20)
    previous.prepare()

    c = NearestNeighborClassifier()
    c.reuse_vectors_of(previous)
    for example in previous._examples:
        c.add(example, 'Expenses:Groceries')
    c.fit_encoder(steps=20, seed=1)
    c.prepare()
    np.testing.assert_allclose(
        c._embeddings, c.encoder.transform(previous._examples), atol=1e-6)
