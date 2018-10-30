"""Transaction matching support.

This module implements a mechanism that, given a single candidate transaction,
and a `PostingDatabase` of other transactions, computes a set of merged
transactions that merge the candidate transaction with one or more transactions
in the database.

When importing transactions from external sources, the user is presented with
the possibly-empty set of merged transaction candidates, and has the option of
accepting one of them, or choosing the original unmerged transaction.

Simple example
==============

For example, the `beancount_import.source.mint` module might generate two
pending transactions from a Mint CSV file:

    2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
      Liabilities:Credit-Card             66.88 USD
        date: 2013-11-27
        source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
      Expenses:FIXME                     -66.88 USD

    2013-12-02 * "NATIONAL FEDERAL DES:TRNSFR"
      Assets:Checking                    -66.88 USD
        date: 2013-12-02
        source_desc: "NATIONAL FEDERAL DES:TRNSFR"
      Expenses:FIXME                      66.88 USD

The `Expenses:FIXME` account is a special account name that indicates the
account is unknown.  These two generated transactions in fact correspond to a
single real transaction.  In addition to the transactions themselves, the
matching mechanism relies on an externally-supplied `is_cleared` function
associated with the database of transactions that determines whether a given
posting is considered to be "cleared".  In this case, the first posting of each
generated transaction would be considered to be "cleared" (due to the presence
of the `date` and `source_desc` metadata fields).

An attempt to find matches for the first transaction, using a `PostingDatabase`
containing the second transaction, would proceed as follows:

1. The posting database would be queried with each of the postings in the first
   transaction.

   a. Querying with the "Liabilities:Credit-Card 66.88 USD" posting finds all
      postings with an amount of 66.88 USD within `fuzzy_match_days` (defaulting
      to 5 days) of 2013-11-27 with either an unknown account or an account of
      `Liabilities:Credit-Card`.  This would find the "Expenses:FIXME 66.88 USD"
      posting of the second transaction.

   b. Querying with the "Expenses:FIXME -66.88 USD" posting finds all postings
       with an amount of -66.88 USD within `fuzzy_match_days` of 2013-11-27
       (with no constraint on the account).  This would find the
       "Assets:Checking -66.88 USD" posting of the second transaction.

   The matches from all of these queries are merged to obtain a single set of
   matching transactions that may be merged with the initial transaction.

2. For each matching transaction, an attempt is made to merge it with the
   initial transaction.  This merging is accomplished by finding candidate
   correspondences between the postings of the initial transaction and the
   postings of the matching transaction (subject to various constraints), and
   and then finding combinations of these correspondences that are mutually
   compatible and which lead to a balanced transaction.  One such constraint is
   that a cleared posting cannot be merged with another cleared posting.  The
   result of this step is a set of zero or more successfully merged transactions
   obtained by merging the initial transaction with one of the matching
   transactions.  Note that in rare cases, there may be more than one valid way
   to merge the initial transaction with a given matching transaction; in that
   case all such valid merged transactions will be included in the result set.

   In the case of this example, we result in a single merged transaction (the
   correct result):

       2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
         Liabilities:Credit-Card             66.88 USD
           date: 2013-11-27
           source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
         Assets:Checking                    -66.88 USD
           date: 2013-12-02
           source_desc: "NATIONAL FEDERAL DES:TRNSFR"

3. For each merged transaction obtained from the previous step, we return to
   step 1 and attempt to merge it with additional transactions in the database.
   This is done in such a way as to avoid redundant search paths.

   In this case of this example, this would lead to no further matches.

Full transaction merging algorithm
==================================

The `get_extended_transactions` function implements the computation of merged
transactions as a depth-first search over the space of merged transactions.
Given a merged transaction, the child states, corresponding to merging in one
additional transaction, are computed by `get_single_step_extended_transactions`.

Two transactions `a` and `b` are merged by matching distinct and disjoint
subsets of postings in `a` with distinct and disjoint subsets of postings in
`b`.  Each matched pair of a subset `pa` of postings from `a` and a subset `pb`
of postings from `b` must consist of at least one subset of size 1.  The
postings that may be contained within a single subset are subject to the
constraints specified by the `get_aggregate_posting_candidates` function.

A singleton posting set from one transaction may be matched to an empty set of
postings from the other transaction; this corresponds to removing the posting.
However, as implied by the constraint that all subsets of each transaction must
be distinct, at most one posting from each transaction may be removed.

Except in the case of a match to an empty subset of postings, the aggregate
weights of the two matched subsets must be equal (within a small tolerance), and
must satisfy `are_postings_mergeable`.

Merging is only attempted with transactions/postings within `fuzzy_match_days`
(defaulting to 5) days of the existing transaction/posting.

The final key constraint is that the combined set of matches must balance.  This
is determined by adding the weight of each match, where the weight of a match
involving an empty set is equal to the weight of the other, singleton posting
set.  The resulting sum must equal 0 within a small tolerance.

"""

import datetime
import collections
import itertools
from typing import Sequence, Tuple, List, NamedTuple, Dict, Callable, Optional, Iterable, Set, cast, FrozenSet, Union, Any

from beancount.core.number import MISSING, ZERO, Decimal
from beancount.core.data import Transaction, Posting, Meta, Cost, CostSpec
from beancount.core.amount import mul as amount_mul
from beancount.core.amount import Amount
from beancount.parser import booking_full
import beancount.parser.printer

from .journal_editor import META_IGNORE
from .sorted_list import SortedList
from .posting_date import get_posting_date, POSTING_DATE_KEY, POSTING_TRANSACTION_DATE_KEY

FIXME_ACCOUNT = 'Expenses:FIXME'

CHECK_KEY = 'check'

CLEARED_KEY = 'cleared'

MERGEABLE_FIXME_ACCOUNT_PREFIX = FIXME_ACCOUNT + ':'

DEBUG = False

IsClearedFunction = Callable[[Posting], bool]
MatchGroupKey = NamedTuple('MatchGroupKey', [('currency', str),
                                             ('is_positive', bool)])
WeightedPosting = NamedTuple('WeightedPosting', [('posting', Posting),
                                                 ('weight', Optional[Amount])])
MatchablePosting = NamedTuple('MatchablePosting',
                              [('posting', Posting),
                               ('weight', Amount),
                               ('source_postings', Sequence[Posting])])
MatchablePostings = Sequence[MatchablePosting]
PostingMatch = Tuple[MatchablePosting, MatchablePosting]
PostingMatches = List[PostingMatch]

SingleSignMatchablePostings = Tuple[MatchablePostings, MatchablePostings]
BothSignMatchablePostings = Tuple[SingleSignMatchablePostings,
                                  SingleSignMatchablePostings]

PostingMatchSet = NamedTuple('PostingMatchSet',
                             [('matches', PostingMatches),
                              ('removals', MatchablePostings)])

SingleSignMatchGroup = List[Tuple[Decimal, PostingMatchSet]]
BothSignMatchGroup = Tuple[SingleSignMatchGroup, SingleSignMatchGroup]

SingleSignMatchGroups = NamedTuple(
    'SingleSignMatchGroups',
    [('no_removals', SingleSignMatchGroup),
     ('single_removals', Tuple[SingleSignMatchGroup, SingleSignMatchGroup]),
     ('double_removals', SingleSignMatchGroup)])
BothSignMatchGroups = Tuple[SingleSignMatchGroups, SingleSignMatchGroups]


def is_unknown_account(account: str):
    return account == FIXME_ACCOUNT or account.startswith(
        MERGEABLE_FIXME_ACCOUNT_PREFIX)


def get_posting_weight(posting: Posting) -> Optional[Amount]:
    if posting.units is MISSING or posting.units is None:
        return None

    if posting.cost is not None and posting.cost.currency is not None:
        posting = booking_full.convert_costspec_to_cost(posting)
        if posting.cost.number is not None and posting.cost.number is not MISSING:
            return Amount(posting.cost.number * posting.units.number,
                          posting.cost.currency)
        return None
    elif posting.price is not None:
        return amount_mul(posting.price, posting.units.number)
    return posting.units


def _date_amount_key(entry: Transaction, mp: MatchablePosting):
    return (get_posting_date(entry, mp.posting), mp.weight)


def _entry_and_posting_ids_key(entry: Transaction, mp: MatchablePosting):
    return (id(entry), ) + tuple(id(p) for p in mp.source_postings)


# Key used for querying postings by date and amount.
DatabaseDateAmountKey = Tuple[datetime.date, Amount]

# Key used for querying postings by special metadata field, such as `check`.
# account, key, value, weight
DatabaseMetadataKey = Tuple[str, str, Any, Amount]

SourcePostingIds = Tuple[int, ...]

# Keyed by the sequence of source_posting ids.
DatabaseValues = Dict[SourcePostingIds, Tuple[Transaction, MatchablePosting]]

CHECK_KEY = 'check'

class PostingDatabase(object):
    def __init__(self, fuzzy_match_days: int,
                 is_cleared: IsClearedFunction,
                 metadata_keys=frozenset()) -> None:
        self.fuzzy_match_days = fuzzy_match_days
        self.is_cleared = is_cleared
        self._postings = {}  # type: Dict[DatabaseDateAmountKey, DatabaseValues]
        self._keyed_postings = {
        }  # type: Dict[DatabaseMetadataKey, DatabaseValues]
        self.metadata_keys = metadata_keys

    def get_fuzzy_date_range(self, orig_date: datetime.date):
        for day_offset in range(-self.fuzzy_match_days,
                                self.fuzzy_match_days + 1):
            yield orig_date + datetime.timedelta(days=day_offset)

    def add_posting(self, entry: Transaction, mp: MatchablePosting):
        source_posting_ids = _entry_and_posting_ids_key(entry, mp)

        meta = mp.posting.meta
        account = mp.posting.account
        if not is_unknown_account(account) and meta:
            for key in self.metadata_keys:
                value = meta.get(key)
                if value is None: continue
                group = self._keyed_postings.setdefault((account, key, value, mp.weight), {})
                group[source_posting_ids] = (entry, mp)

        group = self._postings.setdefault(_date_amount_key(entry, mp), {})
        group[source_posting_ids] = (entry, mp)

    def add_transaction(self, transaction: Transaction):
        for mp in get_matchable_postings_from_transaction(
                transaction, self.is_cleared):
            self.add_posting(transaction, mp)

    def remove_posting(self, entry: Transaction, mp: MatchablePosting):
        source_posting_ids = _entry_and_posting_ids_key(entry, mp)

        meta = mp.posting.meta
        account = mp.posting.account
        if not is_unknown_account(account) and meta:
            for key in self.metadata_keys:
                value = meta.get(key)
                if value is None: continue
                group = self._keyed_postings.get((account, key, value, mp.weight))
                if group is not None:
                    group.pop(source_posting_ids, None)

        group = self._postings.get(_date_amount_key(entry, mp))
        if group is not None:
            group.pop(source_posting_ids, None)

    def remove_transaction(self, transaction: Transaction):
        for mp in get_matchable_postings_from_transaction(
                transaction, self.is_cleared):
            self.remove_posting(transaction, mp)

    def get_posting_matches(self,
                            entry: Transaction,
                            posting: Posting,
                            negate=False) -> List[Tuple[Transaction, MatchablePosting]]:
        posting_date = posting.meta and posting.meta.get(POSTING_DATE_KEY)
        is_date_exact = posting_date is not None
        date = (posting_date or
                (posting.meta and
                 posting.meta.get(POSTING_TRANSACTION_DATE_KEY)) or entry.date)
        weight = get_posting_weight(posting)
        if weight is None:
            return []
        if negate:
            weight = -weight
        matches_dict = self._get_matches(
            account=posting.account,
            date=date,
            amount=weight,
            is_date_exact=is_date_exact)
        if not negate and not is_unknown_account(posting.account):
            meta = posting.meta
            if meta:
                # Look for metadata matches
                for key in self.metadata_keys:
                    value = meta.get(key)
                    if value is None: continue
                    cur_matches = self._keyed_postings.get((posting.account, key, value, weight))
                    if cur_matches is not None:
                        matches_dict.update(cur_matches)
        return sorted(matches_dict.values(), key=lambda x: get_posting_date(x[0], x[1].posting))

    def _get_matches(
            self, account: str, date: datetime.date, amount: Amount,
            is_date_exact: bool) -> DatabaseValues:
        matches = dict() # type: DatabaseValues
        for adjusted_date in self.get_fuzzy_date_range(date):
            cur_matches = self._postings.get((adjusted_date, amount))
            if cur_matches is not None:
                for key, (entry, mp) in cur_matches.items():
                    posting = mp.posting
                    # Verify that the account is compatible.
                    if not are_accounts_mergeable(account, posting.account):
                        continue

                    # Verify that the date is compatible.
                    if is_date_exact:
                        posting_date = posting.meta and posting.meta.get(
                            POSTING_DATE_KEY)
                        if posting_date and posting_date != date:
                            continue

                    matches[key] = (entry, mp)
        return matches


def is_entry_from_journal(entry: Transaction):
    return entry.meta and 'filename' in entry.meta


def combine_optional_frozensets(a: Optional[frozenset], b: Optional[frozenset]):
    if a is None:
        return b
    if b is None:
        return a
    return frozenset(a) | frozenset(b)


NUM_CLEARED_POSTING_MATCHES_KEY = '__num_cleared_posting_matches'
NUM_UNCLEARED_POSTING_MATCHES_KEY = '__num_uncleared_posting_matches'
NUM_UNKNOWN_POSTINGS_REMOVED_KEY = '__num_unknown_postings_removed'

TRANSACTION_POSTING_COUNT_KEYS = [
    NUM_CLEARED_POSTING_MATCHES_KEY,
    NUM_UNCLEARED_POSTING_MATCHES_KEY,
    NUM_UNKNOWN_POSTINGS_REMOVED_KEY,
]
IGNORED_META_KEYS_FOR_MATCHING = frozenset(['lineno', 'filename'])

TRANSACTION_TEMP_METADATA_KEYS = TRANSACTION_POSTING_COUNT_KEYS


def get_count(transaction: Transaction, key: str):
    if transaction.meta is None:
        return 0
    return transaction.meta.get(key, 0)


def merged_transaction_sort_key(transaction: Transaction) -> Tuple[int, int, int]:
    """Returns a sort key for a merged transaction.

    This is used by `get_extended_transactions` to order the merge results.

    :param transaction: A transaction augmented with metadata keys from
        `TRANSACTION_POSTING_COUNT_KEYS`, as returned by
        `combine_transactions_using_match_set`.

    :returns: A sort key such that transactions are sorted in:
        1. descending order of number of cleared postings matched, then
        2. descending order of number of uncleared postings matched, then
        3. ascending order of number of unknown postings removed.
    """
    return (-get_count(transaction, NUM_CLEARED_POSTING_MATCHES_KEY),
            -get_count(transaction, NUM_UNCLEARED_POSTING_MATCHES_KEY),
            get_count(transaction, NUM_UNKNOWN_POSTINGS_REMOVED_KEY))


def get_combined_posting(posting: Posting, matched_posting: Posting,
                         new_transaction_meta: Meta,
                         is_cleared: IsClearedFunction):
    """Merges a posting with a matching posting.

    :param posting: The primary posting.

    :param matching_posting: The secondary posting being merged in.

    :param new_transaction_meta: Metadata dictionary for the new merged
        transaction being created.  This function updates the
        `NUM_CLEARED_POSTING_MATCHES_KEY` and
        `NUM_UNCLEARED_POSTING_MATCHES_KEY` fields.

    :param is_cleared: Function that determines whether a posting is cleared.

    :returns: The merged posting.
    """
    account = posting.account
    if is_cleared(matched_posting):
        new_transaction_meta[NUM_CLEARED_POSTING_MATCHES_KEY] += 1
    else:
        new_transaction_meta[NUM_UNCLEARED_POSTING_MATCHES_KEY] += 1

    if (is_unknown_account(account) and
        (not is_unknown_account(matched_posting.account) or
         (not account.startswith(MERGEABLE_FIXME_ACCOUNT_PREFIX) and
          matched_posting.account.startswith(MERGEABLE_FIXME_ACCOUNT_PREFIX)))):
        account = matched_posting.account

    if posting.price or posting.cost:
        price = posting.price
        cost = posting.cost
        units = posting.units
    elif matched_posting.price or matched_posting.cost:
        price = matched_posting.price
        cost = matched_posting.cost
        units = matched_posting.units
    else:
        price = None
        cost = None
        units = posting.units
    cost = posting.cost or matched_posting.cost
    flag = posting.flag or matched_posting.flag
    new_posting_meta = collections.OrderedDict()  # type: Meta
    new_posting_meta.update(posting.meta or {})
    new_posting_meta.update(matched_posting.meta or {})
    return Posting(
        account=account,
        units=units,
        cost=cost,
        price=price,
        flag=flag,
        meta=new_posting_meta)


def _make_match_dict_from_matches(
        matches: Sequence[PostingMatch]) -> Dict[int, PostingMatch]:
    match_ids = {}
    for m in matches:
        for mp, other_mp in ((m[0], m[1]), (m[1], m[0])):
            for p in mp.source_postings:
                match_ids[id(p)] = (mp, other_mp)
    return match_ids


def combine_transactions_using_match_set(
        txns: Tuple[Transaction, Transaction], is_cleared: IsClearedFunction,
        match_set: PostingMatchSet) -> Transaction:
    """Combines two transactions.
    
    Metadata is merged (it is assumed that the metadata keys, other than
    ignorable ones, are disjoint).
    
    If a cleared transaction is matched with multiple transactions (guaranteed
    to be uncleared), the result is a single merged transaction.

    If an uncleared transaction is matched with multiple transactions
    (guaranteed to be uncleared), the result is multiple transactions.

    Matched postings are merged using `get_combined_postings`.

    :returns: The merged transaction.
    """
    if is_entry_from_journal(txns[1]) and not is_entry_from_journal(txns[0]):
        txns = txns[1], txns[0]

    new_txn_meta = collections.OrderedDict()  # type: Meta
    new_txn_meta.update(txns[0].meta or {})
    new_txn_meta.update(txns[1].meta or {})

    for key in TRANSACTION_POSTING_COUNT_KEYS:
        new_txn_meta[key] = get_count(txns[0], key) + get_count(txns[1], key)

    seen_matched_postings = set()  # type: Set[int]

    removal_ids = set(id(p.posting) for p in match_set.removals)
    match_ids = _make_match_dict_from_matches(match_set.matches)

    new_postings = []
    for posting in txns[0].postings:
        if id(posting) in removal_ids:
            new_txn_meta[NUM_UNKNOWN_POSTINGS_REMOVED_KEY] += 1
            continue
        m = match_ids.get(id(posting))  # type: Optional[PostingMatch]
        if m is None:
            new_postings.append(posting)
            continue
        mp, other_mp = m
        if len(other_mp.source_postings) == 1:
            matched_posting = other_mp.posting
            if len(mp.source_postings) != 1:
                # `posting` is one of multiple postings matched to `other_mp.posting`.
                if id(matched_posting) in seen_matched_postings:
                    # We've already added other_mp.posting.  Don't add it a second time.
                    continue

                if is_cleared(matched_posting):
                    # Create a single combined posting from matched_posting and
                    # all postings in mp.source_postings.
                    seen_matched_postings.add(id(matched_posting))
                    result = matched_posting
                    for p in mp.source_postings:
                        result = get_combined_posting(
                            posting=p._replace(
                                cost=result.cost,
                                price=result.price,
                                units=result.units),
                            matched_posting=result,
                            new_transaction_meta=new_txn_meta,
                            is_cleared=is_cleared)
                    new_postings.append(result)
                    continue

                #
                # Ignore the cost, price, and units on other_mp.posting.
                matched_posting = matched_posting._replace(
                    cost=None, price=None, units=None)

            new_posting = get_combined_posting(
                posting=posting,
                matched_posting=matched_posting,
                new_transaction_meta=new_txn_meta,
                is_cleared=is_cleared)
            new_postings.append(new_posting)
            continue

        # `posting` is matched to multiple other postings
        if is_cleared(posting):
            # Create a single combined posting from posting and all postings in
            # other_mp.source_postings.
            result = posting
            for matched_posting in other_mp.source_postings:
                result = get_combined_posting(
                    posting=posting,
                    matched_posting=matched_posting._replace(
                        cost=None, price=None, units=None),
                    new_transaction_meta=new_txn_meta,
                    is_cleared=is_cleared)
            new_postings.append(result)
            continue

        # `posting` is matched to multiple `matched_posting` Posting instances.
        # Always use the units, price, and cost information of the single postings.
        for matched_posting in other_mp.source_postings:
            new_posting = get_combined_posting(
                posting=posting._replace(
                    units=matched_posting.units,
                    cost=matched_posting.cost,
                    price=matched_posting.price),
                matched_posting=matched_posting,
                new_transaction_meta=new_txn_meta,
                is_cleared=is_cleared)
            new_postings.append(new_posting)

    for posting in txns[1].postings:
        if id(posting) in removal_ids:
            new_txn_meta[NUM_UNKNOWN_POSTINGS_REMOVED_KEY] += 1
            continue
        if id(posting) in match_ids:
            continue
        new_postings.append(posting)

    return txns[0]._replace(
        meta=new_txn_meta,
        tags=combine_optional_frozensets(txns[0].tags, txns[1].tags),
        links=combine_optional_frozensets(txns[0].links, txns[1].links),
        postings=new_postings)


def remove_metadata_keys(entry: Union[Transaction, Posting], keys: Sequence[str]):
    if not entry.meta:
        return entry
    if any(key in entry.meta for key in keys):
        meta = entry.meta.copy()
        for key in keys:
            meta.pop(key, None)
        return entry._replace(meta=meta)
    return entry


def normalize_transaction(transaction: Transaction) -> Transaction:
    """Removes the TRANSACTION_TEMP_METADATA_KEYS metadata keys.

    These metadata keys are used only for ordering merged transactions.

    :returns: The new transaction with the clean metadata.
    """
    return remove_metadata_keys(transaction, TRANSACTION_TEMP_METADATA_KEYS)


class SimpleInventory(dict):
    def __iadd__(self, amount: Amount):
        """Adds an amount to the inventory."""
        if amount is None or amount.number == ZERO:
            return self

        currency = amount.currency
        value = self[currency]
        value = value + amount.number
        if value == ZERO:
            del self[currency]
        else:
            self[currency] = value
        return self

    def __isub__(self, amount: Amount):
        """Subtracts an amount from the inventory."""
        if amount is None or amount.number == ZERO:
            return self

        currency = amount.currency
        value = self[currency]
        value = value - amount.number
        if value == ZERO:
            del self[currency]
        else:
            self[currency] = value
        return self

    def copy(self):
        return SimpleInventory(super().copy())

    def __missing__(self, _):
        return ZERO


def get_max_residuals_from_weights(a_weights, b_weights):
    combined_inventory = SimpleInventory()

    # If any posting is unweighted, we require all residuals to be 0.
    if None in a_weights or None in b_weights:
        return combined_inventory

    for weight in a_weights:
        combined_inventory += weight

    max_residuals = combined_inventory.copy()

    for weight in b_weights:
        combined_inventory += weight

    # Set max_tolerances to be the maximum absolute value of the residual in
    # `a`, `b`, or `a + b`.
    #
    # This loop will exclude currencies for which the `a + b` value is ZERO, but
    # we deal with that in the next loop.
    for currency, combined_value in combined_inventory.items():
        a_value = max_residuals[currency]
        b_value = combined_value - a_value
        max_residuals[currency] = max(
            abs(a_value), abs(b_value), abs(combined_value))

    # Convert all residual values to their absolute values.  This handles
    # currencies for which the `a + b` value is ZERO, and were skipped by the
    # prior loop.
    for currency, value in max_residuals.items():
        max_residuals[currency] = abs(value)

    return max_residuals


def is_mergeable_unknown_account(account: str):
    return account is not None and account.startswith(
        MERGEABLE_FIXME_ACCOUNT_PREFIX)


def get_aggregate_posting_candidates(
        postings: Iterable[Posting], is_cleared: IsClearedFunction
) -> List[Tuple[Posting, Tuple[Posting, ...]]]:
    """Computes valid subsets of `postings` that may be used for matching.

    1. Only subsets with at least two elements are returned.

    2. All postings in a returned subset must have identical accounts (as
       determined by string equality).  An unknown account is not considered
       identical to a known account.  Additionally, `Expenses:FIXME` is not
       considered identical to `Expenses:FIXME:A`.

    3. Subsets must not contain cleared postings, or postings with a `cost` or
       `price` specification, or with `MISSING` units.

    4. All postings in a subset must have the same `units.currency`, and the
       same sign of `units.number` (i.e. positive or negative).

    6. To limit the computational cost, subsets are limited to at most 4
       elements, except that all maximal subsets are also returned.

    The returned subsets are not, in general, disjoint.

    :returns: The list of pairs of `(effective_posting, source_postings)`, where
        `source_postings` is the list of postings in the subset and
        `effective_posting` is a synthesized posting with the common account and
        the sum of the `units` of each posting in the subset.
    """
    possible_sets = collections.OrderedDict(
    )  # type: Dict[Tuple[str, str, bool], List[Posting]]
    for posting in postings:
        if (posting.price is not None or posting.cost is not None or
                posting.units is None or posting.units is MISSING):
            continue
        if is_cleared(posting):
            continue
        possible_sets.setdefault((posting.account, posting.units.currency,
                                  posting.units.number > ZERO),
                                 []).append(posting)
    results = []
    max_subset_size = 4

    def add_subset(account, currency, subset):
        total = sum(x.units.number for x in subset)
        aggregate_posting = Posting(
            account=account,
            units=Amount(currency=currency, number=total),
            cost=None,
            price=None,
            flag=None,
            meta=None)
        results.append((aggregate_posting, tuple(subset)))

    for (account, currency, _), posting_list in possible_sets.items():
        if len(posting_list) == 1:
            continue
        if len(posting_list) > max_subset_size:
            add_subset(account, currency, posting_list)
        for subset_size in range(
                2, min(len(posting_list) + 1, max_subset_size + 1)):
            for subset in itertools.combinations(posting_list, subset_size):
                add_subset(account, currency, subset)
    return results


def get_match_group_key(weight: Amount) -> MatchGroupKey:
    return MatchGroupKey(weight.currency, weight.number > ZERO)


def get_weighted_postings(postings: Sequence[Posting]) -> List[WeightedPosting]:
    return [WeightedPosting(p, get_posting_weight(p)) for p in postings]


def get_matchable_postings(
        weighted_postings: Sequence[WeightedPosting],
        is_cleared: IsClearedFunction) -> Iterable[MatchablePosting]:
    """Returns the list of all valid MatchablePosting objects.

    A MatchablePosting corresponds to a subset of one or more underlying Posting
    objects from a transaction, with a known combined weight.  Subsets of more
    than one Posting are subject to the constraints specified by the
    `get_aggregate_posting_candidates` function.

    This returns both MatchablePosting objects corresponding to a singleton
    posting set as well as MatchablePosting objects corresponding to multiple
    underlying postings.
    """
    for p, weight in weighted_postings:
        if weight is None:
            continue
        yield MatchablePosting(p, weight, (p, ))
    for p, all_ps in get_aggregate_posting_candidates(
        (p for p, _ in weighted_postings), is_cleared):
        yield MatchablePosting(p, p.units, all_ps)


def get_matchable_posting_groups(
        weighted_postings: Sequence[WeightedPosting],
        is_cleared: IsClearedFunction
) -> Dict[MatchGroupKey, List[MatchablePosting]]:
    results = collections.OrderedDict(
    )  # type: Dict[MatchGroupKey, List[MatchablePosting]]
    for mp in get_matchable_postings(weighted_postings, is_cleared):
        key = get_match_group_key(mp.weight)
        results.setdefault(key, []).append(mp)
    return results


def get_matchable_postings_from_transaction(
        transaction: Transaction,
        is_cleared: IsClearedFunction) -> Iterable[MatchablePosting]:
    return get_matchable_postings(
        get_weighted_postings(transaction.postings), is_cleared)


PostingSpecs = Set[Tuple[str, Amount, Optional[Union[Cost, CostSpec]], Optional[
    Amount]]]


def get_transaction_posting_specs(transaction: Transaction) -> PostingSpecs:
    """Returns the set of `(account, units, cost, price)` tuples.

    This is used by IsTransactionMergeablePredicate.
    """
    return set((posting.account, posting.units, posting.cost, posting.price)
               for posting in transaction.postings
               if not is_unknown_account(posting.account))


def transaction_has_opposite_posting(transaction: Transaction,
                                     posting_specs: PostingSpecs) -> bool:
    """Returns True if `transaction` has a posting opposite to one in
    `posting_specs`.

    This is used by IsTransactionMergeablePredicate.
    """
    for posting in transaction.postings:
        if posting.units is not MISSING:
            opposite_key = (posting.account, -posting.units, posting.cost,
                            posting.price)
            if opposite_key in posting_specs:
                return True
    return False


def is_metadata_mergeable(*metas: Optional[Meta]) -> bool:
    """Returns `True` if a sequence of metadata dictionaries can all be merged
    without conflicts.
    """
    combined = {}  # type: Meta
    for meta in metas:
        if not meta:
            continue
        for k, v in meta.items():
            if k in IGNORED_META_KEYS_FOR_MATCHING:
                continue
            if combined.setdefault(k, v) != v:
                return False
    return True


def are_accounts_mergeable(account_a: str, account_b: str) -> bool:
    """Returns `True` if the two accounts may be equivalent."""
    return account_a == account_b or is_unknown_account(
        account_a) or is_unknown_account(account_b)


def are_postings_mergeable(a: MatchablePosting, b: MatchablePosting,
                           is_cleared: IsClearedFunction) -> bool:
    """Check if two MatchablePosting objects can be merged together.

    This does not check the posting weight, because this function is only
    called for postings with equal weights.
    """
    if len(a.source_postings) > 1 and len(b.source_postings) > 1:
        return False

    if not are_accounts_mergeable(a.posting.account, b.posting.account):
        return False

    if len(a.source_postings) > 1:
        a, b = b, a

    # Ensures that len(a.source_postings) == 1

    a_cleared = is_cleared(a.posting)
    b_cleared = is_cleared(b.posting)
    if a_cleared and b_cleared:
        return False

    if a_cleared:
        # The combined transaction will merge all of the source_postings of `b`
        # into the single source_posting `a`.  Therefore, the metadata must all
        # be compatible.
        if not is_metadata_mergeable(a.posting.meta,
                                     *(p.meta for p in b.source_postings)):
            return False
    else:
        # The combined transaction will merge the single source_posting of `a`
        # with each of the `source_postings` of `b`.  Therefore, the metadata of
        # `a` must be individually compatible with the metadata of each
        # source_posting of `b`.
        if any(not is_metadata_mergeable(a.posting.meta, p.meta)
               for p in b.source_postings):
            return False
    return True


def get_posting_ids_in_match(m: PostingMatch) -> List[int]:
    return [id(p) for mp in m for p in mp.source_postings]


def is_removal_candidate(mp: MatchablePosting) -> bool:
    """Determines whether a MatchablePosting may be removed.

    A matchable posting must satisfy the following constraints to be considered
    for removal:

    1. The account must be unknown.

    2. It must correspond to a single underlying Posting.

    3. It must not have a `price` or `cost` specification.

    4. It must not have any metadata fields (except the automatically generated
       `filename` and `lineno`).

    """
    if not is_unknown_account(mp.posting.account):
        return False
    if mp.posting.price or mp.posting.cost:
        return False
    if len(mp.source_postings) != 1:
        return False
    if mp.posting.meta:
        for key in mp.posting.meta:
            if key not in IGNORED_META_KEYS_FOR_MATCHING:
                return False
    return True


def compute_single_sign_match_groups(
        matchable_postings: SingleSignMatchablePostings,
        is_cleared: IsClearedFunction,
        max_residual: Decimal) -> SingleSignMatchGroups:
    """Given a list of single-sign matchable postings for each of two transactions,
    computes the list of valid PostingMatchSet objects.  A PostingMatchSet is a
    set of non-conflicting valid matches between matchable postings in the first
    transaction and matchable postings in the second transaction as well as a
    list of postings to remove.  The computed PostingMatchSet objects are
    organized into SingleSignMatchGroup objects according to whether 0 or 1
    postings from each transaction are removed (more than 1 removal from each of
    the two transactions is not permitted).

    A match between two MatchablePosting objects is valid if, and only if, their
    weights differ by at most `max_residual` and they satisfy the
    `are_postings_mergeable` predicate.

    Each MatchablePosting may correspond either to a single underlying Posting
    or a subset of underlying Posting objects of one of the two transactions.  A
    set of matches is non-conflicting if no two matches contained in the set use
    the same underlying posting of either of the two transactions.

    A PostingMatchSet is permitted to contain a removal only under certain
    constraints: the MatchablePosting must satisfy `is_removal_candidate`, and
    it must not be possible to replace the removal with a regualr match that
    does not conflict with any existing matches in the PostingMatchSet.
    """

    b_lookup_table = SortedList(
        (x.weight.number, x) for x in matchable_postings[1])

    def get_possible_matches_for_posting_a(a: MatchablePosting):
        weight = a[1]
        matching_postings = b_lookup_table.find(weight.number - max_residual,
                                                weight.number + max_residual)
        for b in matching_postings:
            if not are_postings_mergeable(a, b, is_cleared):
                continue
            yield b

    possible_matches = [(a, b) for a in matchable_postings[0]
                        for b in get_possible_matches_for_posting_a(a)]

    possible_matches_for = {}  # type: Dict[int, List[MatchablePosting]]
    for a, b in possible_matches:
        possible_matches_for.setdefault(id(a), []).append(b)
        possible_matches_for.setdefault(id(b), []).append(a)

    removal_candidates = [[
        x for x in txn_matchable_postings if is_removal_candidate(x)
    ] for txn_matchable_postings in matchable_postings]

    used_postings = set()  # type: Set[int]

    result = SingleSignMatchGroups([], ([], []), [])

    def consider_removal_extensions(current_sum, matches):
        for txn_removal_candidates, txn_removal_results in zip(
                removal_candidates, result.single_removals):
            for x in txn_removal_candidates:
                # Exclude this removal candidate if it is part of the match set.
                if id(x.posting) in used_postings:
                    continue

                # Exclude this removal candidate if it can be matched.  We have
                # already verified that it is not part of the match set, so we
                # only need to find a single partner that is also disjoint from
                # the match set.
                if any(
                        used_postings.isdisjoint(
                            id(p) for p in m.source_postings)
                        for m in possible_matches_for.get(id(x), ())):
                    continue
                txn_removal_results.append((current_sum + x.weight.number,
                                            PostingMatchSet(matches, (x, ))))

        for a in removal_candidates[0]:
            if id(a.posting) in used_postings:
                continue
            used_postings.add(id(a.posting))
            for b in removal_candidates[1]:
                if id(b.posting) in used_postings:
                    continue
                used_postings.add(id(b.posting))
                if (not any(
                        used_postings.isdisjoint(
                            id(p) for p in m.source_postings)
                        for m in possible_matches_for.get(id(a), ())) and  #
                        not any(
                            used_postings.isdisjoint(
                                id(p) for p in m.source_postings)
                            for m in possible_matches_for.get(id(b), ()))):
                    result.double_removals.append(
                        (current_sum + a.weight.number + b.weight.number,
                         PostingMatchSet(matches,
                                         (a, b))))
                used_postings.remove(id(b.posting))
            used_postings.remove(id(a.posting))

    def consider_match_extensions(current_sum: Decimal,
                                  matches: List[PostingMatch],
                                  next_match_i: int):
        # Search for the possible match, starting at next_match_i, that does not
        # conflict with `used_postings`.
        for match_i in range(next_match_i, len(possible_matches)):
            m = possible_matches[match_i]
            posting_ids_in_match = get_posting_ids_in_match(m)
            if not used_postings.isdisjoint(posting_ids_in_match):
                continue
            # Consider match extensions that do not include `m`.
            consider_match_extensions(current_sum, matches, match_i + 1)
            used_postings.update(posting_ids_in_match)
            new_sum = current_sum + m[0].weight.number
            new_matches = list(matches)
            new_matches.append(m)
            result.no_removals.append((new_sum, PostingMatchSet(
                new_matches, ())))
            # Consider match extensions that do include `m`.
            consider_match_extensions(new_sum, new_matches, match_i + 1)
            used_postings.difference_update(posting_ids_in_match)
            return
        if next_match_i != 0:
            consider_removal_extensions(current_sum, matches)

    # Start from the empty match.
    consider_match_extensions(ZERO, [], 0)
    consider_removal_extensions(ZERO, [])
    return result


def get_valid_single_sign_group_combinations(
        match_groups: BothSignMatchGroups
) -> Sequence[Tuple[SingleSignMatchGroup, SortedList[Decimal, PostingMatchSet]]]:

    """Given the negative and positive weight match groups for a single currency,
    returns the list of valid pairings of one negative-weight match group with
    one positive-weight match group.

    Valid pairings are those that involve at most one removal from each of the
    two transactions.

    The positive-weight match groups are converted to a SortedList
    representation.

    This is used by `compute_balanced_match_groups`.
    """
    pos_no_removals = SortedList(match_groups[1].no_removals)
    pos_single_removals = [
        SortedList(x) for x in match_groups[1].single_removals
    ]
    pos_double_removals = SortedList(match_groups[1].double_removals)

    return (
        (match_groups[0].no_removals, pos_no_removals),
        (match_groups[0].no_removals, pos_single_removals[0]),
        (match_groups[0].no_removals, pos_single_removals[1]),
        (match_groups[0].no_removals, pos_double_removals),
        (match_groups[0].single_removals[0], pos_no_removals),
        (match_groups[0].single_removals[0], pos_single_removals[1]),
        (match_groups[0].single_removals[1], pos_no_removals),
        (match_groups[0].single_removals[1], pos_single_removals[0]),
        (match_groups[0].double_removals, pos_no_removals),
    )


def _get_posting_match_frozenset(
        matches: Sequence[PostingMatch]) -> FrozenSet[Tuple[int, int]]:
    return frozenset((id(a), id(b)) for a, b in matches)


def filter_dominated_match_sets(
        match_sets: List[PostingMatchSet]) -> List[PostingMatchSet]:
    """Computes a filtered list of PostingMatchSet objects with dominated match sets
    removed.

    A PostingMatchSet `a` dominates another PostingMatchSet `b` if `a` contains
    all of the matches in `b`.
    """
    match_sets.sort(key=lambda x: -len(x.matches))
    filtered_results = []
    filtered_posting_match_frozensets = [
    ]  # type: List[FrozenSet[Tuple[int,int]]]
    for match_set in match_sets:
        if any(
                all((id(a), id(b)) in existing for a, b in match_set.matches)
                for existing in filtered_posting_match_frozensets):
            continue
        filtered_posting_match_frozensets.append(
            _get_posting_match_frozenset(match_set.matches))
        filtered_results.append(match_set)
    return filtered_results


# [[neg_a, neg_b], [pos_a, pos_b]]
def compute_balanced_match_group(
        matchable_postings: BothSignMatchablePostings, max_residual: Decimal,
        is_cleared: IsClearedFunction) -> Sequence[PostingMatchSet]:
    if any(
            all(not txn_matchable_postings
                for txn_matchable_postings in single_sign_matchable_postings)
            for single_sign_matchable_postings in matchable_postings):
        return []

    match_groups = cast(
        BothSignMatchGroups,
        tuple(
            compute_single_sign_match_groups(single_sign_matchable_postings,
                                             is_cleared, max_residual)
            for single_sign_matchable_postings in matchable_postings))

    # Include the empty match in the result.
    results = [PostingMatchSet([], ())]
    for neg_group, pos_table in get_valid_single_sign_group_combinations(
            match_groups):
        for total, neg_match_set in neg_group:
            for pos_match_set in pos_table.find(-total - max_residual,
                                                -total + max_residual):
                results.append(
                    PostingMatchSet(
                        neg_match_set.matches + pos_match_set.matches,
                        tuple(neg_match_set.removals) + tuple(
                            pos_match_set.removals)))
    return filter_dominated_match_sets(results)


def get_combined_transactions(txns: Tuple[Transaction, Transaction],
                              is_cleared: IsClearedFunction):

    results = []

    weighted_postings = [get_weighted_postings(txn.postings) for txn in txns]

    matchable_posting_groups = [
        get_matchable_posting_groups(txn_weighted_postings, is_cleared)
        for txn_weighted_postings in weighted_postings
    ]

    match_groups = collections.OrderedDict(
        (key.currency, None)  # type: ignore
        for txn_matchable_posting_groups in matchable_posting_groups
        for key in txn_matchable_posting_groups
    )  # type: Dict[str, Sequence[PostingMatchSet]]

    max_residuals = get_max_residuals_from_weights(
        *[[weight for _, weight in txn_weighted_postings]
          for txn_weighted_postings in weighted_postings])

    for currency in match_groups:
        matchable_postings = cast(
            BothSignMatchablePostings,
            tuple(
                tuple(
                    txn_matchable_posting_groups.get(
                        MatchGroupKey(currency, is_positive), [])
                    for txn_matchable_posting_groups in matchable_posting_groups)
                for is_positive in (False, True)))
        match_groups[currency] = compute_balanced_match_group(
            matchable_postings,
            max_residual=max_residuals.get(currency, ZERO),
            is_cleared=is_cleared)

    postings_matched = set()  # type: Set[int]

    for match_sets in itertools.product(*match_groups.values()):
        combined_matches = sum((match_set.matches for match_set in match_sets),
                               [])  # type: PostingMatches
        combined_removals = sum((match_set.removals
                                 for match_set in match_sets), ())
        if not combined_matches:
            continue
        for m in combined_matches:
            postings_matched.update(get_posting_ids_in_match(m))
        results.append(
            combine_transactions_using_match_set(
                txns,
                is_cleared=is_cleared,
                match_set=PostingMatchSet(combined_matches, combined_removals)))
    return results, postings_matched


def get_posting_identifier(posting: Posting) -> Posting:
    if posting.meta:
        meta = [(k, v) for k, v in posting.meta.items() if k not in META_IGNORE]
    else:
        meta = []
    return posting._replace(meta=frozenset(meta))


CandidateIdentifier = Tuple[FrozenSet[int], FrozenSet[Posting]]


def get_candidate_identifier(
        transaction: Transaction,
        used_transaction_ids: Set[int]) -> CandidateIdentifier:
    # Returns a key that can be used to check for duplicate candidate states.
    # The result depends only on the set of postings.
    return (frozenset(used_transaction_ids),
            frozenset(map(get_posting_identifier, transaction.postings)))


def debug_format_transaction(entry, indent=0):
    def fix_meta(meta):
        if meta is None:
            return None
        meta = meta.copy()
        for k in meta:
            if isinstance(meta[k], int):
                meta[k] = Decimal(meta[k])
        return meta

    def fix_meta_in(obj):
        if isinstance(obj, Transaction):
            return obj._replace(
                meta=fix_meta(obj.meta),
                postings=[fix_meta_in(p) for p in obj.postings])
        else:
            return obj._replace(meta=fix_meta(obj.meta))

    entry = fix_meta_in(entry)
    printer = beancount.parser.printer.EntryPrinter()
    formatted = printer(entry)
    return '\n'.join(' ' * indent + line for line in formatted.split('\n'))


def debug_print(msg, level=0):
    indent = level * 4
    for line in msg.split('\n'):
        print(' ' * indent + line)


class IsTransactionMergeablePredicate(object):
    """Callable predicate that determines if two transactions can be merged.

    Two transactions may be merged if they do not contain any conflicting
    metadata fields, and they do not contain opposite postings with known
    accounts, i.e. there does not exist a posting `pa` in transaction `a` and a
    posting `pb` in transaction `b` such that:
        (pa.account == pb.account and
         not is_unknown_account(pa.account) and
         pa.units == -pb.units and
         pa.price == pb.price and
         pa.cost == pb.cost).

    The opposite posting constraint is a heuristic based on the idea that a
    transaction containing two postings that cancel each other out is unlikely
    to be correct.
    """

    def __init__(self, transaction: Transaction) -> None:
        self.transaction = transaction
        self.posting_specs = get_transaction_posting_specs(transaction)

    def __call__(self, b: Transaction) -> bool:
        a = self.transaction
        if not is_metadata_mergeable(a.meta, b.meta):
            return False
        if transaction_has_opposite_posting(b, self.posting_specs):
            return False
        return True


MergedTransaction = NamedTuple('MergedTransaction',
                               [('transaction', Transaction),
                                ('used_transactions', List[Transaction])])

SingleStepMergedTransaction = NamedTuple('SingleStepMergedTransaction',
                                         [('transaction', Transaction),
                                          ('matched_transaction', Transaction)])

def _get_valid_posting_matches(
        transaction_constraint: IsTransactionMergeablePredicate,
        posting: Posting, negate: bool, posting_db: PostingDatabase,
        excluded_transaction_ids: FrozenSet[int]
) -> Iterable[Tuple[Transaction, MatchablePosting]]:
    """Returns the matching transaction, posting pairs.

    Transactions already present in `excluded_transaction_ids` are excluded, as
    are transactions that do not satisfy `transaction_constraint`.
    """
    matches = posting_db.get_posting_matches(
        transaction_constraint.transaction, posting, negate=negate)
    for matching_transaction, mp in matches:
        if id(matching_transaction) in excluded_transaction_ids:
            continue
        if not transaction_constraint(matching_transaction): continue
        yield matching_transaction, mp

def get_unknown_to_opposite_unknown_extensions(
        transaction_constraint: IsTransactionMergeablePredicate,
        posting_db: PostingDatabase,
        excluded_transaction_ids: FrozenSet[int],
        mp: MatchablePosting) -> Iterable[SingleStepMergedTransaction]:
    """Finds extensions that remove both `mp` and an unknown posting of opposite
    weight in the matching transaction.
    """
    for matching_transaction, other_mp in _get_valid_posting_matches(
            transaction_constraint,
            mp.posting,
            negate=True,
            posting_db=posting_db,
            excluded_transaction_ids=excluded_transaction_ids):
        if not is_removal_candidate(other_mp): continue
        yield SingleStepMergedTransaction(
            combine_transactions_using_match_set(
                (transaction_constraint.transaction, matching_transaction),
                match_set=PostingMatchSet(matches=[], removals=(mp, other_mp)),
                is_cleared=posting_db.is_cleared), matching_transaction)


def get_single_step_extended_transactions(
        transaction: Transaction,
        posting_db: PostingDatabase,
        excluded_transaction_ids: FrozenSet[int],
        debug_level=0) -> Iterable[SingleStepMergedTransaction]:
    """Finds valid merges of `transaction` with a single additional transaction.

    This is done by first computing the set of `matchable_postings` by calling
    `get_matchable_postings_from_transaction`.  Then the merged transactions are
    computed in two steps:

    1. For each transaction in the `posting_db` containing a posting that
       matches at least one of the `matchable_postings`, outputs the list of
       merged transactions computed by calling `get_combined_transactions`.

    2. For each matchable posting `mp` that satisfies `is_removal_candidate` and
       was not matched in any of the merged results computed by step 1, outputs
       the list of merged transactions computed by calling
       `get_unknown_to_opposite_unknown_extentsions`.
    """

    matching_transactions = collections.OrderedDict(
    )  # type: Dict[int, Transaction]
    matchable_postings = list(
        get_matchable_postings_from_transaction(transaction,
                                                posting_db.is_cleared))
    transaction_constraint = IsTransactionMergeablePredicate(transaction)
    for mp in matchable_postings:
        for orig_matching_transaction, _ in _get_valid_posting_matches(
                transaction_constraint,
                mp.posting,
                negate=False,
                posting_db=posting_db,
                excluded_transaction_ids=excluded_transaction_ids,
        ):
            matching_transactions[id(
                orig_matching_transaction)] = orig_matching_transaction

    postings_matched = set()  # type: Set[int]

    if DEBUG:
        debug_print(
            'EXTEND_CANDIDATE\nSource transaction:\n%s' %
            debug_format_transaction(transaction, 2),
            level=debug_level)
        debug_print(
            'Matching transactions: (%d)' % (len(matching_transactions), ),
            level=debug_level)
    for matching_transaction in matching_transactions.values():
        if DEBUG:
            debug_print(
                debug_format_transaction(matching_transaction, 2),
                level=debug_level)
        combined_transactions, new_postings_matched = get_combined_transactions(
            (transaction, matching_transaction),
            is_cleared=posting_db.is_cleared)
        if DEBUG:
            debug_print(
                '   got %d matches' % (len(combined_transactions)),
                level=debug_level)
        postings_matched.update(new_postings_matched)
        for new_transaction in combined_transactions:
            yield SingleStepMergedTransaction(new_transaction, matching_transaction)

    for mp in matchable_postings:
        # Only search for a match between an unknown account posting and another
        # unknown account posting with the opposite amount if we weren't able to
        # match the non-negated amount.
        if id(mp.posting) in postings_matched: continue
        if not is_removal_candidate(mp): continue
        yield from get_unknown_to_opposite_unknown_extensions(
            transaction_constraint=transaction_constraint,
            posting_db=posting_db,
            excluded_transaction_ids=excluded_transaction_ids,
            mp=mp)


def get_extended_transactions(
        initial_transaction: Transaction,
        posting_db: PostingDatabase) -> List[MergedTransaction]:
    """Finds valid merges of `initial_transaction`.

    Performs a depth-first search over the space of merged transactions.  The
    child states, corresponding to merging a single additional transaction with
    the existing merged transaction, are obtained by calling
    `get_single_step_extended_transactions`.

    :returns: The list of merged transactions, ordered by
        `merged_transaction_sort_key`.
    """
    used_transaction_ids = set()  # type: Set[int]
    used_transactions = []  # type: List[Transaction]

    results = [] # type: List[Tuple[Transaction, List[Transaction]]]

    previously_seen_states = set()  # type: Set[CandidateIdentifier]

    def maybe_extend_candidate(transaction: Transaction,
                               ref_transaction: Transaction, level: int):
        # Check if we have already seen this state.
        if ref_transaction is not None:
            used_transaction_ids.add(id(ref_transaction))
            used_transactions.append(ref_transaction)

        state_id = get_candidate_identifier(transaction, used_transaction_ids)
        if state_id not in previously_seen_states:
            previously_seen_states.add(state_id)

            if ref_transaction is not None:

                if len(used_transactions) > 1:
                    results.append((transaction, used_transactions.copy()))

            do_extend_candidate(transaction, level)

        if ref_transaction is not None:
            used_transaction_ids.remove(id(ref_transaction))
            del used_transactions[-1]

    def do_extend_candidate(transaction: Transaction, level: int):
        for new_transaction, matching_transaction in get_single_step_extended_transactions(
                transaction=transaction,
                posting_db=posting_db,
                excluded_transaction_ids=cast(FrozenSet[int],
                                              used_transaction_ids),
                debug_level=level):
            maybe_extend_candidate(new_transaction, matching_transaction,
                                   level + 1)

    maybe_extend_candidate(initial_transaction, initial_transaction, level=0)

    results.sort(key=lambda x: merged_transaction_sort_key(x[0]))
    return [
        MergedTransaction(normalize_transaction(entry), used_transactions)
        for entry, used_transactions in results
    ]
