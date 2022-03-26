"""Venmo.com transaction and balance source.

Data format
===========

To use, first download transaction and balance data into a directory on the
filesystem.  The easiest way to download data from Venmo in the requisite format
is to use the finance_dl.venmo module.

You might have a directory structure like:

    financial/
      data/
        venmo/
          account_id/
            transactions.csv
            balances.csv


The `transactions.csv` file should be a CSV file containing all downloaded
transactions, in the normal CSV download format provided by Venmo.  See the
`testdata/source/venmo` directory for an example.

The `balances.csv` file should be of the form:

    "Start Date","End Date","Start Balance","End Balance"
    "2017-10-02","2017-12-30","$0.00","$1528.25"
    "2017-12-31","2018-01-20","$1528.25","$0.00"
    "2018-01-21","2018-01-26","unknown","unknown"

The `transactions.csv` file is required, but the `balances.csv` file is
optional.

The Venmo website does not provide a way to directly download a single CSV file
containing all transactions.  It only provides links to separately download CSV
statements for individual periods.  It also does not provide a way to download a
CSV file containing balance information.  That needs to be scraped or manually
created from the web interface itself.

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the venmo source:

    dict(module='beancount_import.source.venmo',
         directory=os.path.join(journal_dir, 'data', 'venmo', 'account_id'),
         assets_account='Assets:Venmo',
    )

where `journal_dir` refers to the financial/ directory.

Imported transaction format
===========================

If you recieve a payment, or make a payment from your Venmo balance, a single
transaction of the following form is generated:

    2017-09-06 * "Sally Smith" "Rent"
      Assets:Venmo     1150.00 USD
        date: 2017-09-06
        venmo_description: "Rent"
        venmo_payer: "Sally Smith"
        venmo_payment_id: "0454063333607815882"
        venmo_type: "Payment"
      Expenses:FIXME  -1150.00 USD

If you make a payment using an external funding source (e.g. debit card or bank
account), two linked transactions are generated, one representing the transfer
from the bank account to the venmo balance, and the other representing the
payment:

    2017-04-25 * "Tom Johnson" "Transfer" ^venmo.2394198259925614643
      Assets:Venmo     220.00 USD
        date: 2017-04-25
        venmo_account_description: "Visa Debit *1559"
        venmo_description: "Tutoring"
        venmo_payee: "Tom Johnson"
        venmo_transfer_id: "2394198259925614643"
        venmo_type: "Payment"
      Expenses:FIXME  -220.00 USD

    2017-04-25 * "Tom Johnson" "Tutoring" ^venmo.2394198259925614643
      Assets:Venmo    -220.00 USD
        date: 2017-04-25
        venmo_description: "Tutoring"
        venmo_payee: "Tom Johnson"
        venmo_payment_id: "2394198259925614643"
        venmo_type: "Payment"
      Expenses:FIXME   220.00 USD

If you transfer funds from your Venmo balance to a bank account, a single
transaction is generated:

    2017-09-06 * "Venmo" "Transfer"
      Assets:Venmo    -1150.00 USD
        date: 2017-09-06
        venmo_account_description: "My Bank *8967"
        venmo_transfer_id: "355418184"
        venmo_type: "Standard Transfer"
      Expenses:FIXME   1150.00 USD

The `venmo_payment_id` and `venmo_transfer_id` metadata fields are used to
associate transactions in the Beancount journal with rows in the
`transactions.csv` file.

For transfer transactions (transactions with a `venmo_transfer_id` metadata
field), the `venmo_type` and `venmo_account_description` metadata fields provide
features for predicting the unknown account.

For payment transactions (transactions with a `venmo_payment_id` metadata
field), the `venmo_type`, `venmo_description`, and `venmo_payee`/`venmo_payer`
metadata fields provide features for predicting the unknown account.
"""

import datetime
import os
import collections
import csv
import re
from typing import Dict, Tuple, List, Union, Sequence

import dateutil.parser
from beancount.core.data import Transaction, Posting, Balance, EMPTY_SET, Open
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D, ZERO

from .. import amount_parsing
from . import ImportResult, Source, SourceResults, InvalidSourceReference
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

VENMO_TRANSFER_KEY = 'venmo_transfer_id'
VENMO_PAYMENT_KEY = 'venmo_payment_id'
VENMO_TYPE_KEY = 'venmo_type'
VENMO_ACCOUNT_DESCRIPTION_KEY = 'venmo_account_description'
VENMO_DESCRIPTION_KEY = 'venmo_description'
VENMO_PAYEE_KEY = 'venmo_payee'
VENMO_PAYER_KEY = 'venmo_payer'

CSV_ID_KEY = "ID"
CSV_DATETIME_KEY = "Datetime"
CSV_TYPE_KEY = 'Type'
CSV_STATUS_KEY = 'Status'
CSV_NOTE_KEY = 'Note'
CSV_FROM_KEY = 'From'
CSV_TO_KEY = 'To'
CSV_AMOUNT_TOTAL_KEY = 'Amount (total)'
CSV_AMOUNT_FEE_KEY = 'Amount (fee)'
CSV_FUNDING_SOURCE_KEY = 'Funding Source'
CSV_DESTINATION_KEY = 'Destination'

transaction_field_names = [
    CSV_ID_KEY,
    CSV_DATETIME_KEY,
    CSV_TYPE_KEY,
    CSV_STATUS_KEY,
    CSV_NOTE_KEY,
    CSV_FROM_KEY,
    CSV_TO_KEY,
    CSV_AMOUNT_TOTAL_KEY,
    CSV_AMOUNT_FEE_KEY,
    CSV_FUNDING_SOURCE_KEY,
    CSV_DESTINATION_KEY,
]

BALANCE_START_DATE_KEY = 'Start Date'
BALANCE_END_DATE_KEY = 'End Date'
BALANCE_START_BALANCE_KEY = 'Start Balance'
BALANCE_END_BALANCE_KEY = 'End Balance'
balance_field_names = [
    BALANCE_START_DATE_KEY,
    BALANCE_END_DATE_KEY,
    BALANCE_START_BALANCE_KEY,
    BALANCE_END_BALANCE_KEY,
]

RawTransaction = dict
RawBalance = dict

def parse_csv_date(x: str):
    return dateutil.parser.parse(x, ignoretz=True).replace(tzinfo=datetime.timezone.utc)

def parse_balance_date(x: str):
    return datetime.datetime.strptime(x, '%Y-%m-%d').date()

def add_line_and_filename(x: dict, filename: str, line: int) -> Dict[str, Union[str,int]]:
    x = {k.strip(): v for k, v in x.items()}
    x.update(filename=filename, line=line)
    return x


def get_info(raw: Union[RawTransaction, RawBalance]):
    return dict(
        type='text/csv',
        filename=raw['filename'],
        line=raw['line'],
    )

def load_csv(path: str, field_names: List[str]) -> List[Dict[str, Union[str,int]]]:
    path = os.path.abspath(path)
    with open(path, 'r', newline='', encoding='utf-8') as f:
        csv_reader = csv.DictReader(f)
        assert csv_reader.fieldnames is not None
        assert set(field_names).issubset(set(x.strip() for x in csv_reader.fieldnames))
        return [add_line_and_filename(x, path, line_i + 1) for line_i, x in enumerate(csv_reader)]

def load_transactions(path: str):
    return load_csv(path, transaction_field_names)

def load_balances(path: str):
    return load_csv(path, balance_field_names)

def get_venmo_account_map(accounts: Dict[str, Open]):
    venmo_account_mapping = dict()
    for entry in accounts.values():
        for key in entry.meta:
            if key.startswith('venmo_account_name'):
                venmo_account_mapping[entry.meta[key]] = entry.account
    return venmo_account_mapping



class VenmoSource(Source):
    def __init__(self, directory: str, assets_account: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.assets_account = assets_account
        transactions_path = os.path.join(directory, 'transactions.csv')
        self.log_status('venmo: loading %s ' % transactions_path)
        self.raw_transactions = load_transactions(transactions_path)
        balances_path = os.path.join(directory, 'balances.csv')
        if os.path.exists(balances_path):
            self.log_status('venmo: loading %s ' % balances_path)
            self.raw_balances = load_balances(balances_path)
        else:
            self.raw_balances = []

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting):
        result = dict()
        def maybe_add_key(key):
            x = posting.meta.get(key)
            if x is not None:
                result[key] = x
        maybe_add_key(VENMO_TYPE_KEY)
        if VENMO_TRANSFER_KEY in posting.meta:
            maybe_add_key(VENMO_ACCOUNT_DESCRIPTION_KEY)
        if VENMO_PAYMENT_KEY in posting.meta:
            maybe_add_key(VENMO_DESCRIPTION_KEY)
            maybe_add_key(VENMO_PAYEE_KEY)
            maybe_add_key(VENMO_PAYER_KEY)
        return result

    def is_posting_cleared(self, posting: Posting):
        if posting.meta is None:
            return False
        return VENMO_TRANSFER_KEY in posting.meta or VENMO_PAYMENT_KEY in posting.meta

    def prepare(self, journal: JournalEditor, results: SourceResults):
        matched_transfer_postings = dict(
        )  # type: Dict[str, List[Tuple[Transaction,Posting]]]
        matched_payment_postings = dict(
        )  # type: Dict[str, List[Tuple[Transaction,Posting]]]

        for entry in journal.all_entries:
            if not isinstance(entry, Transaction):
                continue
            for posting in entry.postings:
                if posting.meta is None:
                    continue
                if posting.account != self.assets_account:
                    continue
                venmo_transfer_id = posting.meta.get(VENMO_TRANSFER_KEY)
                venmo_payment_id = posting.meta.get(VENMO_PAYMENT_KEY)
                if venmo_transfer_id is not None:
                    matched_transfer_postings.setdefault(venmo_transfer_id, []).append((entry, posting))
                if venmo_payment_id is not None:
                    matched_payment_postings.setdefault(venmo_payment_id, []).append((entry, posting))

        valid_ids = set()

        for raw_txn in self.raw_transactions:
            venmo_id = raw_txn[CSV_ID_KEY]
            t = raw_txn[CSV_TYPE_KEY]
            valid_ids.add(venmo_id)
            has_transfer = False
            has_payment = False
            if t == 'Standard Transfer':
                has_transfer = True
                has_payment = False
            elif t in ['Charge', 'Payment', 'Merchant Transaction']:
                has_transfer = raw_txn[CSV_FUNDING_SOURCE_KEY] != 'Venmo balance' and raw_txn[CSV_DESTINATION_KEY] != 'Venmo balance'
                has_payment = True
            else:
                raise RuntimeError('Unknown transaction type: %r' % (t,))
            for has, matched_postings, make in ((has_transfer, matched_transfer_postings, self.make_transfer_transaction),
                                                (has_payment, matched_payment_postings, self.make_payment_transaction)):
                existing = matched_postings.get(venmo_id)
                if existing is not None:
                    num_needed = 1 if has else 0
                    if len(existing) > num_needed:
                        results.add_invalid_reference(InvalidSourceReference(len(existing) - num_needed, existing))
                elif has:
                    txn = make(raw_txn, has_transfer and has_payment)
                    results.add_pending_entry(
                        ImportResult(
                            date=txn.date, entries=[txn], info=get_info(raw_txn)))
        for raw_balance in self.raw_balances:
            start_amount_text = raw_balance[BALANCE_START_BALANCE_KEY]
            if start_amount_text != 'unknown':
                start_amount = amount_parsing.parse_amount(start_amount_text)
                start_date = parse_balance_date(raw_balance[BALANCE_START_DATE_KEY])
                results.add_pending_entry(
                    ImportResult(
                        date=start_date,
                        entries=[
                            Balance(
                                date=start_date,
                                meta=None,
                                account=self.assets_account,
                                amount=start_amount,
                                tolerance=None,
                                diff_amount=None,
                            )
                        ],
                        info=get_info(raw_balance),
                    ))
            end_amount_text = raw_balance[BALANCE_END_BALANCE_KEY]
            if end_amount_text != 'unknown':
                end_amount = amount_parsing.parse_amount(end_amount_text)
                end_date = parse_balance_date(raw_balance[BALANCE_END_DATE_KEY]) + datetime.timedelta(days=1)
                results.add_pending_entry(
                    ImportResult(
                        date=end_date,
                        entries=[
                            Balance(
                                date=end_date,
                                meta=None,
                                account=self.assets_account,
                                amount=end_amount,
                                tolerance=None,
                                diff_amount=None,
                            )
                        ],
                        info=get_info(raw_balance),
                    ))
        results.add_account(self.assets_account)

    def _make_transaction(self, raw_txn: RawTransaction, link: bool, is_transfer: bool):
        amount = original_amount = amount_parsing.parse_amount(raw_txn[CSV_AMOUNT_TOTAL_KEY])
        txn_type = raw_txn[CSV_TYPE_KEY]
        is_payment_txn = txn_type in ['Payment', 'Charge', 'Merchant Transaction']
        if is_transfer and is_payment_txn:
            amount = -amount
        txn_time = parse_csv_date(raw_txn[CSV_DATETIME_KEY])
        assets_posting = Posting(
                    account=self.assets_account,
                    units=amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=collections.OrderedDict([
                        (VENMO_TRANSFER_KEY if is_transfer else VENMO_PAYMENT_KEY, raw_txn[CSV_ID_KEY]),
                        ('date', txn_time.date()),
                        (VENMO_TYPE_KEY, txn_type),
                    ]),
                )
        # CSV_NOTE_KEY field empty for Merchan Transactions, fall back on TO which contains merchant name
        note = re.sub(r'\s+', ' ', raw_txn[CSV_NOTE_KEY if txn_type != 'Merchant Transaction' else CSV_TO_KEY])
        payee = 'Venmo'
        if is_payment_txn:
            if original_amount.number > ZERO:
                payee = assets_posting.meta[VENMO_PAYER_KEY] = raw_txn[CSV_FROM_KEY if txn_type == 'Payment' else CSV_TO_KEY]
            else:
                payee = assets_posting.meta[VENMO_PAYEE_KEY] = raw_txn[CSV_TO_KEY if txn_type == 'Payment' else CSV_FROM_KEY]
        if note:
            assets_posting.meta[VENMO_DESCRIPTION_KEY] = note
        if is_transfer:
            assets_posting.meta[VENMO_ACCOUNT_DESCRIPTION_KEY] = raw_txn[CSV_DESTINATION_KEY] or raw_txn[CSV_FUNDING_SOURCE_KEY]

        links = EMPTY_SET
        if link:
            links = frozenset(['venmo.%s' % raw_txn[CSV_ID_KEY]])
        return Transaction(
            meta=None,
            date=txn_time.date(),
            flag=FLAG_OKAY,
            payee=payee,
            narration='Transfer' if is_transfer else note,
            tags=EMPTY_SET,
            links=links,
            postings=[
                assets_posting,
                Posting(
                    account=FIXME_ACCOUNT,
                    units=-amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=None,
                ),
            ],
        )


    def make_transfer_transaction(self, raw_txn: RawTransaction, link: bool):
        return self._make_transaction(raw_txn, link, True)

    def make_payment_transaction(self, raw_txn: RawTransaction, link: bool):
        return self._make_transaction(raw_txn, link, False)

    @property
    def name(self):
        return 'venmo'

def load(spec, log_status):
    return VenmoSource(log_status=log_status, **spec)
