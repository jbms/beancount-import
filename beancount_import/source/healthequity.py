"""Import source for healthequity.com HSA accounts.

Data format
===========

To use, first download transaction and balance data into a directory on the
filesystem.  The easiest way to download data from Healthequity in the requisite
format is to use the finance_dl.healthequity module.

You might have a directory structure like:

    financial/
      data/
        healthequity/
          account_id/
            cash-transactions-contribution.csv
            cash-transactions-distribution.csv
            cash-transactions-other.csv
            investment-transactions.csv
            %Y-%m-%dT%H%M%S%z.balances.csv

The `cash-transactions-*.csv` files should contain the downloaded transaction
history from the HealthEquity website downloaded separately for each of the
three transaction types: "Contribution", "Distribution", and "Other".  The "All
Transaction Types" download option does not provide as much information, and
must not be used.  The download provided by the HealthEquity website is in an
HTML table format, and must be converted to the following CSV format:

    "Date","Transaction","Amount","Cash Balance"
    "2016-01-15","Employer Contribution (Tax year: 2016)","$800.00","$800.00"
    "2016-03-11","Employer Contribution (Tax year: 2015)","$1,600.00","$2,100.03"

The `investment-transactions.csv` file should contain the downloaded "Fund
Activity", and likewise must be converted from the HTML table format to the
following CSV format:

    "Date","Fund","Category","Description","Price","Amount","Shares","Total Shares","Total Value"
    "2016-01-19","VIIIX","Buy","Investment: VIIIX","$171.92","$300.00","1.745","1.745","$300.00"
    "2016-03-11","VIIIX","Buy","Investment: VIIIX","$185.58","$1,600.03","8.622","10.367","$1,923.86"
    "2016-03-16","VIIIX","Dividend","Investment: VIIIX","$185.71","$10.40","0.056","10.423","$1,935.70"
    "2016-06-16","VIIIX","Dividend","Investment: VIIIX","$191.57","$9.77","0.051","10.474","$2,006.49"

The `*.balances.csv` files are optional and must be in the following CSV format:

    "Fund","Name","Shares (#)","Closing Price","Closing Value"
    "VIIIX","VANGUARD INSTITUTIONAL INDEX INSTL PL","10.474","199.17","2,086.11"

This data must be scraped from the website, as there is no option to download
data in this format.

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the healthequity source:

    dict(module='beancount_import.source.healhequity',
         directory=os.path.join(journal_dir, 'data', 'healthequity'),
    )

where `journal_dir` refers to the financial/ directory.  Note that `directory`
should not specify the individual `account_id` directory, but rather should
specify its parent directory.

Assocating HealthEquity accounts with Beancount accounts
========================================================

Specify the association between a given HealthEquity `account_id` (corresponding
to the sub-directory name) with a Beancount account prefix as follows:

    2016-01-01 open Assets:HSA:HealthEquity
      healthequity_account_id: "1234567"
      dividend_account: "Income:HealthEquity:Dividends"
      capital_gains_account: "Income:HealthEquity:Capital-Gains"

Imported transaction format
===========================

Cash transactions
-----------------

Cash transactions (derived from the `cash-transactions-*.csv` files) have the
following form:

    2016-01-01 * "Employer Contribution (Tax year: 2016)"
      Assets:HSA:HealthEquity:Cash   800.00 USD
        date: 2016-01-15
        healthequity_transaction_type: "Contribution"
        source_desc: "Employer Contribution (Tax year: 2016)"
      Expenses:FIXME                           -800.00 USD

    2016-01-31 * "Interest for Jan-16"
      Assets:HSA:HealthEquity:Cash   0.01 USD
        date: 2016-01-31
        healthequity_transaction_type: "Other"
        source_desc: "Interest for Jan-16"
      Expenses:FIXME                           -0.01 USD

    2016-01-16 * "Investment: VIIIX"
      Assets:HSA:HealthEquity:Cash  -300.00 USD
        date: 2016-01-16
        healthequity_transaction_type: "Other"
        source_desc: "Investment: VIIIX"
      Expenses:FIXME                            300.00 USD

The second posting is always to an unknown account, which may be predicted based
on the `source_desc` and `healthequity_transaction_type` metadata fields.  The
unknown account in the last transaction actually corresponds to the associated
investment account.  For simplicity, that is handled by the transaction matching
mechanism of Beancount-import, rather than by the source directly.

Investment transactions
-----------------------

Investment transactions (derived from the `investment-transactions.csv` file)
have the following form:

    2016-01-19 * "Buy"
      Assets:HSA:HealthEquity:VIIIX    1.745 VIIIX {171.92 USD}
        date: 2016-01-19
        source_desc: "Buy"
      Assets:HSA:HealthEquity:Cash   -300.00 USD

    2016-03-16 * "Dividend"
      Assets:HSA:HealthEquity:VIIIX   0.056 VIIIX {185.71 USD}
        date: 2016-03-16
        source_desc: "Dividend"
      Income:HealthEquity:Dividends:VIIIX       -10.40 USD

Investment transactions always have all accounts fully specified.
"""

from typing import NamedTuple, Optional, Union, Dict, List, Tuple
import csv
import collections
import datetime
import os
import re
from beancount.core.amount import Amount
from beancount.core.data import Transaction, Posting, Balance, Commodity, Price, EMPTY_SET, Open, Meta
from beancount.core.number import D, MISSING, ZERO
from beancount.core.flags import FLAG_OKAY
from beancount.core.position import Cost, CostSpec

from ..journal_editor import JournalEditor
from . import description_based_source
from . import ImportResult, SourceResults, LogFunction
from ..posting_date import POSTING_DATE_KEY
from ..matching import FIXME_ACCOUNT

from ..amount_parsing import parse_amount, parse_number

CashTransaction = NamedTuple('CashTransaction', [
    ('account', str),
    ('date', datetime.date),
    ('description', str),
    ('type', str),
    ('units', Amount),
    ('balance', Amount),
    ('filename', str),
    ('line', int),
])
FundTransaction = NamedTuple('FundTransaction', [
    ('account', str),
    ('date', datetime.date),
    ('description', str),
    ('memo', str),
    ('price', Amount),
    ('units', Amount),
    ('amount', Amount),
    ('balance', Amount),
    ('filename', str),
    ('line', int),
])

ImportedBalance = NamedTuple('ImportedBalance', [
    ('account', str),
    ('date', datetime.date),
    ('units', Amount),
    ('price', Amount),
    ('market_value', Amount),
    ('filename', str),
    ('line', int),
])

RawTransaction = Union[CashTransaction, FundTransaction]
RawEntry = Union[RawTransaction, ImportedBalance]


def get_info(raw_entry: RawEntry):
    return dict(
        type='text/csv',
        filename=raw_entry.filename,
        line=raw_entry.line,
    )


TRANSACTION_TYPE_KEY = 'healthequity_transaction_type'

date_format = '%Y-%m-%d'


def load_cash_transactions(filename: str, account: str,
                           transaction_type: str) -> List[CashTransaction]:
    expected_field_names = ['Date', 'Transaction', 'Amount', 'Cash Balance']
    transactions = []
    filename = os.path.abspath(filename)
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if reader.fieldnames != expected_field_names:
            raise RuntimeError(
                'Actual field names %r != expected field names %r' %
                (reader.fieldnames, expected_field_names))
        for line_i, row in enumerate(reader):
            amount = parse_amount(row['Amount'])
            transactions.append(
                CashTransaction(
                    account=account,
                    type=transaction_type,
                    units=amount,
                    date=datetime.datetime.strptime(row['Date'],
                                                    date_format).date(),
                    description=row['Transaction'],
                    balance=parse_amount(row['Cash Balance']),
                    filename=filename,
                    line=line_i + 1,
                ))
    return transactions


def load_fund_transactions(filename: str,
                           account: str) -> List[FundTransaction]:
    expected_field_names = [
        'Date', 'Fund', 'Category', 'Description', 'Price', 'Amount', 'Shares',
        'Total Shares', 'Total Value'
    ]
    transactions = []  # type: List[FundTransaction]
    filename = os.path.abspath(filename)
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if reader.fieldnames != expected_field_names:
            raise RuntimeError(
                'Actual field names %r != expected field names %r' %
                (reader.fieldnames, expected_field_names))
        for line_i, row in enumerate(reader):
            transactions.append(
                FundTransaction(
                    account=account,
                    date=datetime.datetime.strptime(row['Date'],
                                                    date_format).date(),
                    description=row['Category'],
                    memo=row['Description'],
                    price=parse_amount(row['Price']),
                    amount=parse_amount(row['Amount']),
                    units=Amount(parse_number(row['Shares']), row['Fund']),
                    balance=Amount(
                        parse_number(row['Total Shares']), row['Fund']),
                    filename=filename,
                    line=line_i + 1,
                ))
    return transactions


def load_balances(filename: str, date: datetime.date,
                  account: str) -> List[ImportedBalance]:
    expected_field_names = [
        'Fund', 'Name', 'Shares (#)', 'Closing Price', 'Closing Value'
    ]
    balances = []  # type: List[ImportedBalance]
    filename = os.path.abspath(filename)
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if reader.fieldnames != expected_field_names:
            raise RuntimeError(
                'Actual field names %r != expected field names %r' %
                (reader.fieldnames, expected_field_names))
        for line_i, row in enumerate(reader):
            balances.append(
                ImportedBalance(
                    account=account,
                    date=date,
                    units=Amount(parse_number(row['Shares (#)']), row['Fund']),
                    price=Amount(parse_number(row['Closing Price']), 'USD'),
                    market_value=Amount(
                        parse_number(row['Closing Value']), 'USD'),
                    filename=filename,
                    line=line_i + 1,
                ))
        return balances


def load_account(
        account_name: str, account_directory: str, log_status: LogFunction
) -> Tuple[List[CashTransaction], List[FundTransaction], List[ImportedBalance]]:
    cash_transactions = []  # type: List[CashTransaction]
    for transaction_type in ['Contribution', 'Distribution', 'Other']:
        cash_transactions_path = os.path.join(
            account_directory,
            'cash-transactions-%s.csv' % transaction_type.lower())
        log_status('healthequity: loading %s' % cash_transactions_path)
        cash_transactions.extend(
            load_cash_transactions(
                cash_transactions_path,
                account_name,
                transaction_type=transaction_type))

    investment_transactions_path = os.path.join(account_directory,
                                                'investment-transactions.csv')
    log_status('healthequity: loading %s' % investment_transactions_path)
    investment_transactions = load_fund_transactions(
        investment_transactions_path, account_name)
    balances = []  # type: List[ImportedBalance]
    for filename in os.listdir(account_directory):
        m = re.match(r'^(.*)\.balances\.csv$', filename)
        if m is None:
            continue
        t = datetime.datetime.strptime(m.group(1), '%Y-%m-%dT%H%M%S%z')
        # FIXME: make this date more precise somehow
        date = t.date()
        balances_path = os.path.join(account_directory, filename)
        log_status('healthequity: loading %s' % balances_path)
        balances.extend(load_balances(balances_path, date, account_name))
    return cash_transactions, investment_transactions, balances


MatchKey = NamedTuple(
    'MatchKey', [('account', str),
                 ('date', datetime.date),
                 ('description', str), ('transaction_type', Optional[str]),
                 ('cost', Optional[Amount]), ('units', Optional[Amount])])


def get_key_from_raw_entry(entry: RawTransaction) -> MatchKey:
    transaction_type = entry.type if isinstance(entry,
                                                CashTransaction) else None
    cost = None
    if isinstance(entry, FundTransaction):
        if entry.units.number > ZERO:
            cost = entry.price
    return MatchKey(entry.account, entry.date, entry.description,
                    transaction_type, cost, entry.units)


def get_key_from_posting(entry: Transaction, posting: Posting,
                         source_postings: List[Posting], source_desc: str,
                         posting_date: datetime.date) -> MatchKey:
    del entry
    del source_postings
    transaction_type = posting.meta and posting.meta.get(TRANSACTION_TYPE_KEY)
    if isinstance(posting.cost, CostSpec):
        cost = Amount(posting.cost.number_per, posting.cost.currency)
    elif isinstance(posting.cost, Cost):
        cost = Amount(posting.cost.number, posting.cost.currency)
    else:
        cost = None
    return MatchKey(posting.account, posting_date, source_desc,
                    transaction_type, cost, posting.units)


def make_import_result(csv_entry: RawTransaction, accounts: Dict[str, Open],
                       account_to_id: Dict[str, str],
                       id_to_account: Dict[str, str]) -> ImportResult:
    account_entry = accounts[id_to_account[account_to_id[csv_entry.account]]]

    extra_postings = []
    other_account = FIXME_ACCOUNT
    posting_meta = collections.OrderedDict()  # type: Meta
    posting_meta[description_based_source.SOURCE_DESC_KEYS[
        0]] = csv_entry.description
    posting_meta[POSTING_DATE_KEY] = csv_entry.date

    if isinstance(csv_entry, CashTransaction):
        posting_meta[TRANSACTION_TYPE_KEY] = csv_entry.type
        total_amount = csv_entry.units
        price = None
        cost = None
    elif isinstance(csv_entry, FundTransaction):
        total_amount = csv_entry.amount
        if csv_entry.units.number > ZERO:
            # Buy transaction, specify cost but not price.
            cost = CostSpec(
                number_per=csv_entry.price.number,
                currency=csv_entry.price.currency,
                number_total=None,
                date=None,
                label=None,
                merge=False)
            price = None
            if csv_entry.description == 'Buy':
                other_account = account_entry.account + ':Cash'
            elif csv_entry.description == 'Dividend':
                other_account = account_entry.meta['dividend_account'] + ':' + csv_entry.units.currency
        else:
            # Sell transaction, specify price but not cost.
            price = csv_entry.price
            cost = CostSpec(
                number_per=MISSING,
                number_total=None,
                currency=csv_entry.price.currency,
                date=None,
                label=None,
                merge=False)
            # Add capital gains entry
            extra_postings.append(
                Posting(
                    meta=None,
                    account=account_entry.meta['capital_gains_account'] + ':' +
                    csv_entry.units.currency,
                    units=MISSING,
                    cost=None,
                    price=None,
                    flag=None,
                ))
    else:
        raise ValueError('unexpected entry type %r' % (csv_entry, ))
    entry = Transaction(
        date=csv_entry.date,
        meta=None,
        narration=csv_entry.description,
        flag=FLAG_OKAY,
        payee=None,
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=[])

    entry.postings.append(
        Posting(
            account=csv_entry.account,
            units=csv_entry.units,
            price=price,
            cost=cost,
            flag=None,
            meta=posting_meta))
    entry.postings.extend(extra_postings)
    entry.postings.append(
        Posting(
            meta=None,
            account=other_account,
            units=-total_amount,
            price=None,
            cost=None,
            flag=None))
    return ImportResult(
        date=entry.date, entries=[entry], info=get_info(csv_entry))


class Source(description_based_source.DescriptionBasedSource):
    def __init__(self, directory: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.raw_transactions = [
        ]  # type: List[Union[CashTransaction, FundTransaction]]
        self.raw_balances = []  # type: List[ImportedBalance]
        for account_name in os.listdir(directory):
            cash_transactions, investment_transactions, balances = load_account(
                account_name, os.path.join(directory, account_name),
                self.log_status)
            self.raw_transactions.extend(cash_transactions)
            self.raw_transactions.extend(investment_transactions)
            self.raw_balances.extend(balances)

    def get_example_key_value_pairs(self, transaction: Transaction,
                                    posting: Posting):
        key_values = super().get_example_key_value_pairs(transaction, posting)
        t = posting.meta.get(TRANSACTION_TYPE_KEY)
        if t is not None:
            key_values[TRANSACTION_TYPE_KEY] = t
        return key_values

    def prepare(self, journal: JournalEditor, results: SourceResults):
        account_to_id, id_to_account = description_based_source.get_account_mapping(
            journal.accounts, 'healthequity_account_id')

        def convert_account(entry: RawEntry):
            account_id = entry.account
            if isinstance(entry, CashTransaction):
                suffix = 'Cash'
            else:
                suffix = entry.units.currency
            full_account = id_to_account[account_id] + ':' + suffix
            account_to_id[full_account] = account_id
            return entry._replace(account=full_account)

        balances = [convert_account(entry) for entry in self.raw_balances]
        transactions = [
            convert_account(entry) for entry in self.raw_transactions
        ]

        description_based_source.get_pending_and_invalid_entries(
            raw_entries=transactions,
            journal_entries=journal.all_entries,
            account_set=account_to_id.keys(),
            get_key_from_posting=get_key_from_posting,
            get_key_from_raw_entry=get_key_from_raw_entry,
            make_import_result=lambda x: make_import_result(x, accounts=journal.accounts,
                                                            account_to_id=account_to_id,
                                                            id_to_account=id_to_account),
            results=results)

        balance_entries = collections.OrderedDict(
        )  # type: Dict[Tuple[datetime.date, str, str], ImportResult]

        for entry in transactions:
            date = entry.date + datetime.timedelta(days=1)
            balance_entries[(date, entry.account,
                             entry.balance.currency)] = ImportResult(
                                 date=date,
                                 entries=[
                                     Balance(
                                         date=date,
                                         meta=None,
                                         account=entry.account,
                                         amount=entry.balance,
                                         tolerance=None,
                                         diff_amount=None)
                                 ],
                                 info=get_info(entry))

        for entry in balance_entries.values():
            results.add_pending_entry(entry)

        for balance in balances:
            # Skip outputting recent balances --- just output prices.
            # All transactions provide a balance.

            # output.append(
            #     ImportResult(
            #         date=balance.date,
            #         entries=[Balance(
            #             date=balance.date,
            #             meta=None,
            #             account=balance.account,
            #             amount=balance.units,
            #             tolerance=None,
            #             diff_amount=None)]))
            results.add_pending_entry(
                ImportResult(
                    date=balance.date,
                    info=get_info(balance),
                    entries=[
                        Price(
                            date=balance.date,
                            meta=None,
                            currency=balance.units.currency,
                            amount=balance.price)
                    ]))

    @property
    def name(self):
        return 'healthequity'


def load(spec: dict, log_status: LogFunction):
    return Source(**spec, log_status=log_status)
