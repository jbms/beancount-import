from . import test_util
from . import training


def test_get_unknown_account_group_numbers():
    entry, = test_util.parse("""
        1900-01-01 * "Narration"
          Expenses:A   -40 USD
          Expenses:FIXME  3 USD
          Expenses:B   5 USD
          Expenses:FIXME:A  5 USD
          Expenses:FIXME:A  6 USD
          Expenses:FIXME:B  8 USD
          Expenses:FIXME:B  10 USD
          Expenses:FIXME  5 USD
        """)
    assert training.get_unknown_account_group_numbers(entry) == [
        0, 1, 1, 2, 2, 3
    ]
    assert training.get_unknown_account_names(entry) == [
        'Expenses:FIXME',
        'Expenses:FIXME:A',
        'Expenses:FIXME:A',
        'Expenses:FIXME:B',
        'Expenses:FIXME:B',
        'Expenses:FIXME',
    ]
