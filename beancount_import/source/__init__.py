"""Defines the interface of "data sources".

A data source is a class that inherits from `Source`, and is responsible for
importing transactions, balance, and price information from an external data
source (e.g. an OFX or CSV file).  The imported transactions must balance, but
may contain postings to unknown accounts, indicated by an account name/prefix of
`Expenses:FIXME`.

Additionally, it may declare itself "authoritative" for a set of accounts, in
which case it is responsible for:

1. determining whether postings to those accounts are considered to be
   "cleared".

2. optionally computing features for predicting an unknown account posting
   opposite to a posting from one of those accounts.

Additionally, it may specify that certain metadata keys on postings to unknown
accounts, and on the parent transaction, may also be used for prediction.

A data source module must define a top-level `load` function with a signature:

    def load(spec: dict, log_status: LogFunction) -> Source
        ...

that is called with a dictionary `spec` of configuration options specified by
the user, as well as a logging function `log_status`.  It must return an
instance of a class that inherits from `Source`.

Mint source example
-------------------

For example, the `beancount_import.source.mint` source declares itself
authoritative for all of the accounts for which it is configured to import data,
and generates transactions of the following form:

    2016-08-10 * "STARBUCKS STORE 12345"
      Liabilities:Credit-Card  -2.45 USD
        date: 2016-08-10
        source_desc: "STARBUCKS STORE 12345"
      Expenses:FIXME            2.45 USD

To determine whether the "Liabilities:Credit-Card -2.45 USD" posting is cleared,
the `beancount_import.reconciler` module determines which source, if any, is
authoritative for the account, and then calls its `is_posting_cleared` method.
In this case, it would query the mint source, which would return `True`, due to
the presence of the `date` and `source_desc` metadata fields.

To predict the unknown account, the `reconciler` module would likewise determine
which source, if any, is authoritative for the account of the opposite posting,
and then call its `get_example_key_value_pairs` method to obtain features to use
for prediction.  In this case, the mint source would return:

    {'desc': 'STARBUCKS STORE 12345'}

Amazon source example
---------------------

There is an alternative method used for predicting unknown accounts that is
useful for more complicated transactions involving more than 2 postings.  For
example, the `amazon` source generates transactions of the form:

    2016-02-07 * "Amazon.com" "Order"
      amazon_account: "name@domain.com"
      amazon_order_id: "166-7926740-5141621"
      Expenses:FIXME:A   11.87 USD
        amazon_item_condition: "New"
        amazon_item_description: "Casio Men's W916-8AV Alarm Chronograph Watch, classic"
        amazon_item_quantity: 1
        amazon_seller: "Amazon.com LLC"
        shipped_date: 2016-02-08
      Expenses:FIXME:A    1.13 USD
        amazon_invoice_description: "Sales Tax"
      Expenses:FIXME     -1.06 USD
        amazon_posttax_adjustment: "Rewards Points"
      Expenses:FIXME    -11.94 USD
        amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
        transaction_date: 2016-02-08

The `amazon` source does not declare itself authoritative for any accounts, but
it does specify that the `amazon_account`, `amazon_item_description`,
`amazon_posttax_adjustment`, and `amazon_credit_card_description` metadata
fields are used for unknown account prediction.

The two postings with an unknown account of `Expenses:FIXME:A` will be predicted
together, using the combined set of features extracted based on the metadata of
both postings (in this case only the first posting contributes features).

The other `Expenses:FIXME` accounts will be predicted individually.
"""

import datetime
from typing import Iterable, NamedTuple, List, Dict, Mapping, Any, Tuple, Union, Callable, Optional
import importlib

from beancount.core.data import Transaction, Entries, Directive, Posting, Meta

if False:
    from ..journal_editor import JournalEditor  # For type annotations only.

from ..training import TrainingExamples, PredictionInput, ExampleKeyValuePairs

ImportResult = NamedTuple('ImportResult', [
    ('date', datetime.date),
    ('entries', Entries),
    ('info', Optional[Mapping[str, Any]]),
])

InvalidSourceReference = NamedTuple('InvalidSourceReference', [
    ('num_extras', int),
    ('transaction_posting_pairs', List[Tuple[Transaction, Optional[Posting]]])
])


def invalid_source_reference_sort_key(
        x: InvalidSourceReference) -> List[datetime.date]:
    return [p[0].date for p in x.transaction_posting_pairs]


class SourceResults:
    def __init__(self):
        self.pending = []  # type: List[ImportResult]
        self.accounts = set()  # type: Set[str]
        self.skip_training_accounts = set() # type: Set[str]
        self.invalid_references = []  # type: List[InvalidSourceReference]
        self.messages = []  # type: List[Tuple[str, str, Optional[Meta]]]
        self.seen_messages = set(
        )  # type: Set[Tuple[str, str, FrozenSet[Tuple[str, Any]]]]

    def add_pending_entry(self, entry: ImportResult):
        """Adds a generated ImportResult."""
        self.pending.append(entry)

    def add_pending_entries(self, entries: Iterable[ImportResult]):
        """Calls `add_pending_entry` for each entry in `entries`."""
        self.pending.extend(entries)

    def add_account(self, account: str) -> None:
        """Indicates that the source is authoritative for `account`."""
        self.accounts.add(account)

    def add_skip_training_account(self, account: str) -> None:
        """Ignore postings for `account` when building training examples.

        This applies to `account` and all of its subaccounts.

        As described by the this module, typical training requires exactly
        two postings, one the source and the second the target. When there are
        more than two postings, such as for fees and capital gains, this
        method allows those auxiliary accounts to be ignored so that training
        example extraction can still work. The resulting set is passed to
        training.FeatureExtractor.
        """
        self.skip_training_accounts.add(account)

    def add_accounts(self, accounts: Iterable[str]):
        """Calls `add_account` for each account in `accounts`."""
        self.accounts.update(accounts)

    def add_invalid_reference(self, r: InvalidSourceReference):
        self.invalid_references.append(r)

    def add_invalid_references(self, r: Iterable[InvalidSourceReference]):
        """Calls `add_invalid_reference` for each element in `r`."""
        self.invalid_references.extend(r)

    def add_message(self,
                    severity: str,
                    message: str,
                    source: Optional[Meta] = None):
        """Indicates that an error or warning occurred while importing data.

        :param source: If specified, indicates that the error relates to this
            metadata, which should have `filename` and `lineno` fields.
        """
        key = (severity, message, frozenset(source.items())
               if source else frozenset())
        if key in self.seen_messages: return
        self.seen_messages.add(key)
        self.messages.append((severity, message, source))

    def add_warning(self, message: str, source: Optional[Meta] = None):
        """Indicates that a warning occurred while importing data.

        :param source: If specified, indicates that the message relates to this
            metadata, which should have `filename` and `lineno` fields.
        """
        self.add_message('warning', message, source)

    def add_error(self, message: str, source: Optional[Meta] = None):
        """Indicates that an error occurred while importing data.

        :param source: If specified, indicates that the message relates to this
            metadata, which should have `filename` and `lineno` fields.
        """
        self.add_message('error', message, source)


ExampleKeyExtractorFunction = Callable[[Posting, Dict[str, str]], None]
ExampleKeyExtractor = Optional[ExampleKeyExtractorFunction]


class AssociatedData:
    """Represents source data associated with a directive/posting."""

    def __init__(self,
                 description: str,
                 type: str,
                 path: Optional[str] = None,
                 meta: Optional[Tuple[str, Any]] = None,
                 link: Optional[str] = None,
                 posting: Optional[Posting] = None):
        """Initializes the associated data object.

        :param description: A textual description of the data.
        :param type: Mime type of the data.
        :param path: Optional.  Local filesystem path to the data.
        :param meta: Optional.  Key value metadata pair indicating the
                     association.  Mutually exclusive with link.
        :param link: Optional.  Transaction link value indicating the
                     association.  Mutually exclusive with meta.
        :param posting: Optional.  Posting to which this data is associated.
        """
        self.description = description
        self.type = type
        self.path = path
        self.meta = meta
        self.link = link
        self.posting = posting


class Source:
    """Represents a data source with a particular set of data files.

    The Source object is created once at startup by the
    `beancount_import.reconciler` module, before the journal has been loaded,
    and then the `prepare` method is called once the journal has been loaded to
    fill in a SourceResults object.  The journal may be reloaded multiple times,
    due to manual/external modifications to it, in which case the `Source`
    object is reused but the `prepare` method is called again.

    Depending on the format of the data, for efficiency it may be useful to load
    all of the data into memory when the Source object is first constructed, and
    then rely on this in-memory representation in `prepare`.

    Alternatively, if the contents of the journal can be used to greatly reduce
    the amount of data that has to be read/parsed, then it may be useful to only
    load data as needed inside of `prepare`.

    The derived class __init__ method may also add keys to the
    `example_posting_key_extractors` and `example_transaction_key_extractors`
    member variables.  The associated value may either be the value `None`, in
    which the corresponding metadata value will be used directly, or a function
    that generates key-value features from the metadata value.
    """

    def __init__(self, log_status: Callable[[str], None], **kwargs) -> None:
        super().__init__()
        self.log_status = log_status
        self.example_posting_key_extractors = dict(
        )  # type: Dict[str, ExampleKeyExtractor]
        self.example_transaction_key_extractors = dict(
        )  # type: Dict[str, ExampleKeyExtractor]

    @property
    def name(self) -> str:
        """Returns the name of the source, e.g. "mint" or "ofx".

        This is displayed to the user in the UI.
        """
        raise NotImplementedError

    def prepare(self, journal: 'JournalEditor', results: SourceResults) -> None:
        """Processes `journal`, adding entries to `results`.

        Pending entries not already imported into the journal should be added by
        calling `result.add_pending_entry` or `result.add_pending_entries`.

        Entries in the journal that have metadata that should associate them
        with particular source data, but that source data is not found, should
        be indicated by calling `result.add_invalid_reference` or
        `result.add_invalid_references`.

        Accounts for which this source is authoritative should be indicated by
        calling `results.add_account` or `result.add_accounts`.

        Errors can be indicated by calling `result.add_error`.
        """
        raise NotImplementedError

    def is_posting_cleared(self, posting: Posting):
        """Returns `True` if `posting` is cleared.

        This will only be called for postings with an account for which this
        source is authoritative.

        """
        del posting
        return False

    def get_example_key_value_pairs(self, transaction: Transaction,
                                    posting: Posting) -> ExampleKeyValuePairs:
        """Extracts training example key/value pairs for `posting`.

        This will only be called for postings with an account for which this
        source is authoritative.
        """
        del transaction
        del posting
        return {}

    def get_associated_data(self,
                            entry: Directive) -> Optional[List[AssociatedData]]:
        """Returns any associated data for this directive."""
        del entry
        return None


LogFunction = Callable[[str], None]
SourceSpec = Dict[str, Any]


def load_source(source_spec: SourceSpec, log_status: LogFunction) -> Source:
    """Loads a Source from a specification.

    The `source_spec` must be a dictionary containing a `module` key specifying
    the full name of the source module to load.

    The remaining items in the dictionary are passed directly to the `load`
    function defined in the specified `module`.
    """
    source_spec = source_spec.copy()
    m = importlib.import_module(source_spec.pop('module'))
    return m.load(source_spec, log_status=log_status)  # type: ignore
