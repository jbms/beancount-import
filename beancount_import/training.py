import collections
import datetime
import re
from typing import Callable, Any, Dict, Iterable, List, NamedTuple, Sequence, Union, Optional, Tuple

from beancount.core.data import Directive, Entries, Transaction, Posting
from beancount.core.amount import Amount
from .matching import is_unknown_account, FIXME_ACCOUNT
from .posting_date import get_posting_date

if False:
    from .source import Source  # For typing only.

DEFAULT_IGNORE_ACCOUNT_FOR_CLASSIFICATION_PATTERN = '^Income.*:Capital-Gains(?::|$)'

ExampleKeyValuePairs = Dict[str, Union[str, Sequence[str]]]

# Function called with a metadata value to generate key-value pairs.
ExampleKeyExtractor = Callable[[Any, ExampleKeyValuePairs], None]

PredictionInput = NamedTuple('PredictionInput',
                             [('source_account', str),
                              ('amount', Amount),
                              ('date', datetime.date),
                              ('key_value_pairs', ExampleKeyValuePairs)])


def get_features(example: PredictionInput) -> Dict[str, bool]:
    features = collections.defaultdict(lambda: False)  # type: Dict[str, bool]
    features['account:%s' % example.source_account] = True

    # For now, skip amount and date.

    for key, values in example.key_value_pairs.items():
        if isinstance(values, str):
            values = (values, )
        for value in values:
            words = []
            for w in value.split():
                w = w.strip('-.').lower()
                if len(w) > 0:
                    words.append(w)
            for start_i in range(len(words)):
                for end_i in range(start_i + 1, len(words) + 1):
                    features['%s:%s' % (key, ' '.join(
                        words[start_i:end_i]))] = True
    return features


class TrainingExamples(object):
    def __init__(self):
        self.training_examples = []

    def add(self, example: PredictionInput, target_account: str):
        self.training_examples.append((get_features(example), target_account))


class MockTrainingExamples(object):
    def __init__(self):
        self.examples = []  # type: List[Tuple[PredictionInput, str]]

    def add(self, prediction_input: PredictionInput,
            target_account: str) -> None:
        self.examples.append((prediction_input, target_account))


TrainingExamplesInterface = Union[TrainingExamples, MockTrainingExamples]


def get_unknown_account_postings(transaction: Transaction) -> List[Posting]:
    return [
        posting for posting in transaction.postings
        if is_unknown_account(posting.account)
    ]


def get_unknown_account_group_numbers(transaction: Transaction) -> List[int]:
    num_groups = 0
    group_numbers = []
    existing_groups = {}  # type: Dict[str, int]
    for posting in transaction.postings:
        if not is_unknown_account(posting.account):
            continue
        if posting.account == FIXME_ACCOUNT:
            group_number = num_groups
        else:
            group_number = existing_groups.setdefault(posting.account,
                                                      num_groups)
        if group_number == num_groups:
            num_groups += 1
        group_numbers.append(group_number)
    return group_numbers


def get_unknown_account_names(transaction: Transaction) -> List[str]:
    return [
        posting.account for posting in transaction.postings
        if is_unknown_account(posting.account)
    ]


class FeatureExtractor(object):
    def __init__(
            self,
            account_source_map: Dict[str, 'Source'],
            sources: Iterable['Source'],
            ignore_account_pattern:
            str = DEFAULT_IGNORE_ACCOUNT_FOR_CLASSIFICATION_PATTERN,
    ) -> None:
        self.account_source_map = account_source_map
        self.ignore_account_pattern = ignore_account_pattern
        self.example_posting_key_extractors = dict(
        )  # type: Dict[str, ExampleKeyExtractor]
        self.example_transaction_key_extractors = dict(
        )  # type: Dict[str, ExampleKeyExtractor]
        for source in sources:
            for t in ('example_posting_key_extractors',
                      'example_transaction_key_extractors'):
                for key, extractor in getattr(source, t).items():
                    if extractor is None:

                        def default_extractor(x, key_value_pairs, key=key):
                            key_value_pairs.setdefault(key, []).append(x)

                        extractor = default_extractor
                    getattr(self, t)[key] = extractor

    def _ignore_posting_for_automatic_classification(self, posting):
        return re.match(self.ignore_account_pattern,
                        posting.account) is not None

    def get_postings_for_automatic_classification(self, postings):
        return [
            posting for posting in postings
            if not self._ignore_posting_for_automatic_classification(posting)
        ]

    def extract_examples(self, entries: Entries,
                         training_examples: TrainingExamplesInterface) -> None:
        example_posting_key_extractors = self.example_posting_key_extractors
        example_transaction_key_extractors = self.example_transaction_key_extractors
        for entry in entries:
            if not isinstance(entry, Transaction): continue

            transaction_key_value_pairs = dict()  # type: ExampleKeyValuePairs
            if entry.meta:
                for k in entry.meta:
                    extractor = example_transaction_key_extractors.get(k, None)
                    if extractor is None: continue
                    extractor(entry.meta[k], transaction_key_value_pairs)

            # First attempt to extract direct training examples
            got_example = False
            for posting in entry.postings:
                meta = posting.meta
                if not meta: continue
                if is_unknown_account(posting.account): continue
                key_value_pairs = dict()  # type: ExampleKeyValuePairs
                for k in meta:
                    extractor = example_posting_key_extractors.get(k, None)
                    if extractor is None: continue
                    extractor(meta[k], key_value_pairs)
                if key_value_pairs:
                    got_example = True
                    key_value_pairs = dict(transaction_key_value_pairs,
                                           **key_value_pairs)
                    training_examples.add(
                        PredictionInput(
                            source_account='',
                            amount=posting.units,
                            date=entry.date,
                            key_value_pairs=key_value_pairs),
                        target_account=posting.account,
                    )
            if got_example: continue
            non_ignored_postings = self.get_postings_for_automatic_classification(
                entry.postings)
            if len(non_ignored_postings) != 2: continue
            for posting_i, posting in enumerate(non_ignored_postings):
                if posting.meta is None: continue
                target_account = non_ignored_postings[1 - posting_i].account
                if is_unknown_account(target_account): continue
                source = self.account_source_map.get(posting.account)
                if source is None: continue
                key_value_pairs = source.get_example_key_value_pairs(
                    entry, posting)
                if not key_value_pairs: continue
                key_value_pairs = dict(transaction_key_value_pairs,
                                       **key_value_pairs)
                training_examples.add(
                    PredictionInput(
                        source_account=posting.account,
                        key_value_pairs=key_value_pairs,
                        date=get_posting_date(entry, posting),
                        amount=posting.units),
                    target_account=target_account)

    def extract_unknown_account_group_features(
            self, transaction: Transaction) -> List[Optional[PredictionInput]]:
        group_numbers = get_unknown_account_group_numbers(transaction)
        example_posting_key_extractors = self.example_posting_key_extractors
        example_transaction_key_extractors = self.example_transaction_key_extractors

        transaction_key_value_pairs = dict()  # type: ExampleKeyValuePairs
        if transaction.meta:
            for k in transaction.meta:
                extractor = example_transaction_key_extractors.get(k, None)
                if extractor is None: continue
                extractor(transaction.meta[k], transaction_key_value_pairs)

        def get_direct_posting_prediction(postings: List[Posting]):
            key_value_pairs = {}  # type: ExampleKeyValuePairs
            for posting in postings:
                meta = posting.meta
                if not meta: continue
                for key, value in meta.items():
                    extractor = example_posting_key_extractors.get(key)
                    if extractor is None: continue
                    extractor(value, key_value_pairs)
            if not key_value_pairs: return None
            return PredictionInput(
                source_account='',
                amount=posting.units,
                date=transaction.date,
                key_value_pairs=key_value_pairs)

        def get_indirect_posting_prediction() -> Optional[PredictionInput]:
            non_ignored_postings = self.get_postings_for_automatic_classification(
                transaction.postings)
            if len(non_ignored_postings) != 2: return None
            source_posting = (non_ignored_postings[0] if is_unknown_account(
                non_ignored_postings[1].account) else non_ignored_postings[1])
            cur_source = self.account_source_map.get(source_posting.account)
            if cur_source is None: return None
            key_value_pairs = cur_source.get_example_key_value_pairs(
                transaction, source_posting)  # type: ExampleKeyValuePairs
            return PredictionInput(
                source_account=source_posting.account,
                amount=source_posting.units,
                date=get_posting_date(transaction, source_posting),
                key_value_pairs=key_value_pairs)

        group_postings = [[] for _ in range(1 + max(group_numbers, default=-1))
                          ]  # type: List[List[Tuple[int, Posting]]]
        for unknown_i, (posting, group_number)  in enumerate(zip(get_unknown_account_postings(transaction), group_numbers)):
            group_postings[group_number].append((unknown_i, posting))
        group_predictions = [
            get_direct_posting_prediction([posting for _, posting in postings])
            for postings in group_postings
        ]
        if group_numbers == [0] and group_predictions == [None]:
            group_predictions[0] = get_indirect_posting_prediction()
        return group_predictions
