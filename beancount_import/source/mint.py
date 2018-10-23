"""Mint.com transaction source.

This imports transactions from Mint.com CSV export files.

Data format
===========

To use, first download Mint.com transactions and balance information as CSV
files stored on the filesystem.  The easiest way to download data from Mint in
the requisite format is to use the finance_dl.mint module.

You might have a directory structure like:

    financial/
      data/
        mint/
          mint.csv
          balances.*.csv

The `mint.csv` file should be in the CSV download format directly provided by
the Mint website, except that it should not include any pending transactions.
Pending transactions may change or be removed and therefore don't work properly
with this data source.  Unfortunately Mint does not provide a way to download
only non-pending transactions, nor does it include in the CSV file any
indication of which transactions are pending, so if you choose to manually
download the CSV file from the Mint website, you will have to manually delete
the pending transactions.  (The finance_dl.mint module uses additional data
provided only in JSON format to exclude pending transactions.)

The `balances.*.csv` files are optional and should be of the form:

    "Name","Currency","Balance","Last Updated","State","Last Transaction"
    "My Checking","USD","1234.56","1472595089000","OK","07/29/2016"

with one line for each account.  The Mint webiste does not provide a way to
download balance information directly.  (These balance files are created by the
finance_dl.mint module from data provided in JSON format.)

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Mint source:

    dict(module='beancount_import.source.mint',
         directory=os.path.join(journal_dir, 'data', 'mint', 'mint.csv'),
         balances_directory=os.path.join(journal_dir, 'data', 'mint'),
    )

where `journal_dir` refers to the financial/ directory.  Specifying the
`balances_directory` key is optional.  If not specified, balance information
won't be imported.

Associating Mint accounts with Beancount accounts
=================================================

This data source only imports transactions from accounts known to Mint with
which a Beancount account has been explicitly associated using the `mint_id`
metadata field of the account open directive.  The `mint_id` corresponds to the
"Account Name" field in the CSV file.  As this "Account Name" excludes the
institution name, it is possible that the "Account Name" values are not unique,
in which case you can change them using the Mint.com web interface, before
re-downloading the transactions.  For example:

    1900-01-01 open Liabilities:Credit-Card  USD
      mint_id: "My Credit Card"

    1900-01-01 open Assets:Checking  USD
      mint_id: "My Checking"

    1900-01-01 open Liabilities:Amazon-Store-Card  USD
      mint_id: "Amazon Store Card"

Imported transaction format:
============================

Each row in the transactions CSV file corresponds to a single imported
transaction of the form:

    2016-08-10 * "STARBUCKS STORE 12345"
      Liabilities:Credit-Card  -2.45 USD
        date: 2016-08-10
        source_desc: "STARBUCKS STORE 12345"
      Expenses:FIXME            2.45 USD

Transaction identification
--------------------------

The `date` and `source_desc` metadata fields (along with the account and amount)
associate postings in the journal with corresponding rows in the transactions
CSV file.  These fields correspond to the "Date" and "Original Description"
fields in the transactions CSV file, respectively.  It is possible for multiple
real transactions to have an identical combination of account, amount, "Date",
and "Original Description" (corresponding to multiple identical rows in the
transactions CSV file), but that is handled appropriately: this data source will
simply generate a separate transaction for each such row.

The transactions CSV export format provided by Mint and consumed by this data
source does not include a unique transaction identifier, except in the case that
Mint has (erroneously) included a unique identifier provided by the financial
institution in the "Original Description" field.  Internally, Mint does expose a
unique transaction identifier through the undocumented JSON API, but this data
source does not attempt to use them.

Unknown account prediction
--------------------------

The `source_desc` metadata field provides features for predicting the unknown
account.  The transactions CSV format includes additional "Description" and
"Category" fields that are synthesized by Mint from the original data, and
potentially provide some information that could be useful for predicting the
unknown account.  However, this data source does not rely on those fields, as
they are not stable (meaning they may change on a subsequent download).

Handling duplicate transactions
-------------------------------

Mint sometimes incorrectly creates duplicate transactions.  This is different,
but indistinguishable, from the case of two real transactions with the same
account, amount, date, and description.  After verifying that it is really a
duplicate, there are two ways you can deal with this:

 - You can manually add the duplicate description as an additional
   `source_desc1` (or `source_desc2` or `source_desc3`) metadata field to the
   existing posting to which it corresponds.  This is likely to be the easiest
   method.  For example:

    2016-08-10 * "STARBUCKS STORE 12345"
      Liabilities:Credit-Card  -2.45 USD
        date: 2016-08-10
        source_desc: "STARBUCKS STORE 12345"
        source_desc1: "STARBUCKS STORE 12345"
      Expenses:Coffee            2.45 USD


 - If you are using the finance_dl.mint module to download data, you can mark
   the transaction as a duplicate through the Mint.com web interface, and then
   re-download the transactions.  The finance_dl.mint module automatically
   excludes transactions that have been marked as duplicates.
"""

from typing import List, Union, Optional, Set
import csv
import datetime
import collections
import re
import os

from beancount.core.data import Transaction, Posting, Balance, EMPTY_SET
from beancount.core.amount import Amount
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D, ZERO

from . import description_based_source
from . import ImportResult, SourceResults
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

# account may be either the mint_id or the journal account name
MintEntry = collections.namedtuple(
    'MintEntry',
    ['account', 'date', 'amount', 'source_desc', 'filename', 'line'])
RawBalance = collections.namedtuple(
    'RawBalance', ['account', 'date', 'amount', 'filename', 'line'])


def get_info(raw_entry: Union[MintEntry, RawBalance]) -> dict:
    return dict(
        type='text/csv',
        filename=raw_entry.filename,
        line=raw_entry.line,
    )


mint_date_format = '%m/%d/%Y'


def load_transactions(filename: str, currency: str = 'USD') -> List[MintEntry]:
    expected_field_names = [
        'Date', 'Description', 'Original Description', 'Amount',
        'Transaction Type', 'Category', 'Account Name', 'Labels', 'Notes'
    ]

    try:
        entries = []
        filename = os.path.abspath(filename)
        with open(filename, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            if reader.fieldnames != expected_field_names:
                raise RuntimeError(
                    'Actual field names %r != expected field names %r' %
                    (reader.fieldnames, expected_field_names))
            for line_i, row in enumerate(reader):
                account = row['Account Name']
                transaction_type = row['Transaction Type']
                number = D(row['Amount'])
                if number == ZERO:
                    # Skip zero-dollar transactions.
                    # Some banks produce these, e.g. for an annual fee that is waived.
                    continue
                if transaction_type == 'debit':
                    number = -number
                elif transaction_type != 'credit':
                    raise RuntimeError('Unknown transaction type: %r in row %r'
                                       % (transaction_type, row))
                try:
                    date = datetime.datetime.strptime(row['Date'],
                                                      mint_date_format).date()
                except Exception as e:
                    raise RuntimeError('Invalid date: %r' % row['Date']) from e

                entries.append(
                    MintEntry(
                        account=account,
                        date=date,
                        source_desc=row['Original Description'],
                        amount=Amount(number=number, currency=currency),
                        filename=filename,
                        line=line_i + 1))
        entries.reverse()
        entries.sort(key=lambda x: x.date)  # sort by date
        return entries

    except Exception as e:
        raise RuntimeError('CSV file has incorrect format', filename) from e


def load_balances(filename: str) -> List[RawBalance]:
    expected_field_names = [
        'Name', 'Currency', 'Balance', 'Last Updated', 'State',
        'Last Transaction'
    ]
    balances = []
    filename = os.path.abspath(filename)
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if reader.fieldnames != expected_field_names:
            raise RuntimeError(
                'Actual field names %r != expected field names %r' %
                (reader.fieldnames, expected_field_names))
        for line_i, row in enumerate(reader):
            date_str = row['Last Transaction'].strip()
            if not date_str:
                continue
            date = datetime.datetime.strptime(date_str, mint_date_format).date()
            balances.append(
                RawBalance(
                    account=row['Name'],
                    date=date,
                    amount=Amount(D(row['Balance']), row['Currency']),
                    filename=filename,
                    line=line_i + 1))
        return balances


def _get_key_from_posting(entry: Transaction, posting: Posting,
                          source_postings: List[Posting], source_desc: str,
                          posting_date: datetime.date):
    del entry
    del source_postings
    return (posting.account, posting_date, posting.units, source_desc)


def _get_key_from_csv_entry(x: MintEntry):
    return (x.account, x.date, x.amount, x.source_desc)


def _make_import_result(mint_entry: MintEntry) -> ImportResult:
    transaction = Transaction(
        meta=None,
        date=mint_entry.date,
        flag=FLAG_OKAY,
        payee=None,
        narration=mint_entry.source_desc,
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=[
            Posting(
                account=mint_entry.account,
                units=mint_entry.amount,
                cost=None,
                price=None,
                flag=None,
                meta=collections.OrderedDict(
                    source_desc=mint_entry.source_desc,
                    date=mint_entry.date,
                )),
            Posting(
                account=FIXME_ACCOUNT,
                units=-mint_entry.amount,
                cost=None,
                price=None,
                flag=None,
                meta=None,
            ),
        ])
    return ImportResult(
        date=mint_entry.date, info=get_info(mint_entry), entries=[transaction])


class MintSource(description_based_source.DescriptionBasedSource):
    def __init__(self,
                 filename: str,
                 balances_directory: Optional[str] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.filename = filename
        self.balances_directory = balances_directory

        # In these entries, account refers to the mint_id, not the journal account.
        self.log_status('mint: loading %s' % filename)
        self.mint_entries = load_transactions(filename)

        self.balances = [] # type: List[RawBalance]
        if balances_directory:
            for balance_filename in os.listdir(balances_directory):
                m = re.match(r'^balances\.(.*)\.csv$', balance_filename)
                if m is None:
                    continue
                balances_path = os.path.join(balances_directory,
                                             balance_filename)
                self.log_status('mint: loading %s' % balances_path)
                self.balances.extend(load_balances(balances_path))

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        account_to_mint_id, mint_id_to_account = description_based_source.get_account_mapping(
            journal.accounts, 'mint_id')
        missing_accounts = set()  # type: Set[str]

        def get_converted_mint_entries(entries):
            for raw_mint_entry in entries:
                account = mint_id_to_account.get(raw_mint_entry.account)
                if not account:
                    missing_accounts.add(raw_mint_entry.account)
                    continue
                match_entry = raw_mint_entry._replace(account=account)
                yield match_entry

        description_based_source.get_pending_and_invalid_entries(
            raw_entries=get_converted_mint_entries(self.mint_entries),
            journal_entries=journal.all_entries,
            account_set=account_to_mint_id.keys(),
            get_key_from_posting=_get_key_from_posting,
            get_key_from_raw_entry=_get_key_from_csv_entry,
            make_import_result=_make_import_result,
            results=results)

        for mint_account in missing_accounts:
            results.add_warning(
                'No Beancount account associated with Mint account %r.' %
                (mint_account, ))

        for raw_balance in get_converted_mint_entries(self.balances):
            date = raw_balance.date + datetime.timedelta(days=1)
            results.add_pending_entry(
                ImportResult(
                    date=date,
                    info=get_info(raw_balance),
                    entries=[
                        Balance(
                            account=raw_balance.account,
                            date=date,
                            meta=None,
                            amount=raw_balance.amount,
                            tolerance=None,
                            diff_amount=None)
                    ]))

    @property
    def name(self):
        return 'mint'


def load(spec, log_status):
    return MintSource(log_status=log_status, **spec)
