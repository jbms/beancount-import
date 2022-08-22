"""OFX transaction source.

This is based on the original ledgerhub OFX parsing by Martin Blais, but has
been heavily modified.

It imports transactions, balance information, and price information.

This is known to work for various types of checking, investment, retirement, and
credit card accounts.  However, to support any particular institution it may be
necessary to make some minor changes.

Data format
===========

To use, first download transaction data into a directory on the filesystem.  You
might have a directory structure like:

    financial/
      data/
        institution/
          account_id/
            *.ofx

where financial/ also contains your beancount journal files, and `institution`
and `account_id` are replaced by the actual institution and account id.

If your institution supports the OFX protocol, you can use the finance_dl.ofx
module to automatically download data in the requisite format.  You can also
manually download OFX-format statements and place them in a directory.
Duplicate transactions, as determined by the FITID field, are automatically
excluded.  Therefore, if downloading manually, you should just ensure that there
are no gaps in the date ranges selected; overlap will not cause any problems.

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the ofx source:

    dict(module='beancount_import.source.ofx',
         ofx_filenames=(
             glob.glob(os.path.join(journal_dir, 'data/institution1/*/*.ofx'))
             + glob.glob(os.path.join(journal_dir, 'data/institution2/*/*.ofx'))
         ),
         cache_filename=os.path.join(journal_dir, 'data/ofx_cache.pickle'),
    )

where `journal_dir` refers to the financial/ directory.

The `cache_filename` key is optional, but is recommended to speed up parsing if
you have a large amount of OFX data.  When using the `cache_filename` option,
adding and deleting OFX files is fine, but if you modify existing OFX files, you
must delete the cahe file manually.

Specifying individual accounts
==============================

The source specification does not itself indicate anything about the mapping to
accounts.  That information is instead specified in the Beancount journal
itself.

OFX files contain three tags that are used to identify the account: `ORG`,
`BROKERID`, and `ACCTID`.  You can associate a given OFX account with a
Beancount account/account prefix as follows:

    2014-01-01 open Assets:Investment:Vanguard
      ofx_org: "Vanguard"
      ofx_broker_id: "vanguard.com"
      account_id: "XXXXXXXXXXXX"
      ofx_account_type: "securities_only"

The `ofx_org`, `ofx_broker_id`, `account_id`, and `ofx_account_type` metadata
fields must all be specified.  If the OFX file does not include an `ORG` or
`BROKERID` tag, specify the empty string `""` as the `ofx_org` or
`ofx_broker_id`, respectively.

The `ofx_account_type` must be one of the following:

  - `"cash_only"`: Indicates that the account holds cash only, no securities.
    Transactions are posted directly to the specified Beancount account, rather
    than to a `:Cash` suffix.  Any transactions involving securities found in
    the OFX file will be assumed to due to a bank deposit sweep program, or
    similar, and are ignored.

 - `"securities_and_cash"`: Indicates that the account holds both securities and
   cash.  All BUY* transactions are funded from a `:Cash`-suffixed account, and
   the proceeds of all SELL* and INCOME transactions are also posted to the
   `:Cash`-suffixed account.

 - `"securities_only"`: Indicates that the account holds securities, and no cash
   (e.g. many types of Vanguard accounts).  All BUY* transactions are funded
   from an unknown, external account (which may be predicted automatically), and
   the proceeds of all SELL* and INCOME transactions are also posted to an
   unknown, external account.

 - `"securities_and_pending_cash"`: Like the `"securities_only"` case, indicates
   that the account holds securities, and no cash.  However, a fictitious
   `:Cash`-suffixed account is still used to fund all BUY* transactions and for
   the proceeds from all SELL* and INCOME transactions (like the
   `"securities_and_cash"` case).  Additionally, for each such transaction, a
   separate transfer transaction between the `:Cash`-suffixed account and an
   unknown external account is also created, in order to restore the balance of
   the `:Cash` account to zero.  In most cases the `"securities_only"` account
   type will be preferable to this account type, but this account type may make
   it easier to manually enter transactions, may be more compatible with
   existing transactions not imported using this module, and may also help avoid
   balance discrepancies in the case that two institutions post a transaction on
   different dates.

Depending on the type of the account and the type of transactions to be
imported, some additional metadata fields must also be specified for the
account.

For all types of accounts, the following optional metadata fields may be
specified:

 - `quantity_round_digits`: Rounds the number of units to this many decimal
   places.  This can be useful for avoiding balance errors in some cases.  For
   example, Vanguard 401k accounts may benefit from a `quantity_round_digits`
   value of 3.

 - `ignore_transaction_regexp`: Specifies a regular expression (in the syntax
   accepted by the Python `re` module) to be matched against the start of the
   narration of each generated transaction.  Transactions that match are
   skipped.  This is useful for dealing with spurious transactions of a
   particular form.

For accounts with non-cash holdings, the following metadata fields may be
specified:

 - `xxx_income_account`, where `xxx` is one of `div`, `interest`, `cgshort`,
   `cglong`, or `misc`: Specifies the income account prefix used for
   transactions with an `INCOMETYPE` of `DIV`, `INTEREST`, `CGSHORT`, `CGLONG`,
   or `MISC`, respectively.  If you specify a `div_income_account` of
   `"Income:MyBank:Dividends"`, and then receive a dividend from a security
   `XYZ`, the income will be posted to `Income:MyBank:Dividends:XYZ`.

 - `capital_gains_account`: Specifies the account prefix used for capital gains
   and losses due to the sale of any security.  If you specify a
   `capital_gains_account` of `"Income:MyBank:Capital-Gains"` and then sell
   shares of a security `XYZ`, the gains or losses will be posted to
   `Income:MyBank:Capital-Gains:XYZ`.

 - `fees_account`: Brokerage fees associated with individual transactions are
   posted to this account.  A typical value would be
   `"Expenses:Investment-Fees:MyBank:Brokerage-Fees"`.

 - `commission_account`: Commission fees associated with individual transactions
   are posted to this account.  A typical value would be
   `"Expenses:Investment-Fees:MyBank:Commission-Fees"`.

Each of the above fields is required if, and only if, an imported transaction
requires it.  For example, you need not specify a `div_income_account` if you
have no dividend transactions.

For 401(k) accounts, several additional metadata fields may also be specified:

 - `xxx_account`, where `xxx` is one of `pretax`, `aftertax`, `match`,
   `profitsharing`, `rollover`: Specifies the account prefix to use for the
   PRETAX, AFTERTAX, MATCH, PROFITSHARING, ROLLOVER sub-accounts, respectively.
   If not specified, defaults to a suffix of `:PreTax`, `:AfterTax`, `:Match`,
   `:ProfitSharing`, `:Rollover`, respectively on the main account.  Using a
   typical `pretax_account` value of
   `"Assets:Retirement:MyBank:Company401k:PreTax"`, shares of security XYZ would
   be held in `Assets:Retirement:MyBank:Company401k:PreTax:XYZ` and
   `Assets:Retirement:MyBank:Company401k:PreTax:Cash` would be used as the cash
   account (if the `account_type` is `securities_and_cash` or
   `securities_and_pending_cash`).

 - `xxx_contribution_account`, where `xxx` is one of `pretax`, `aftertax`,
   `match`, `profitsharing`, `rollover`: Specifies the account from which BUY*
   transactions for the specified account type are funded.  If not specified,
   generated BUY* transactions will leave the funding source unknown
   (i.e. `Expenses:FIXME`).  For example, you may wish to specify a
   `match_contribution_account` of `"Income:MyCompany:Match"`.

IRA account example
-------------------

    2014-01-01 open Assets:Investment:Vanguard
      ofx_org: "Vanguard"
      ofx_broker_id: "vanguard.com"
      account_id: "XXXXXXXXXXXX"
      ofx_account_type: "securities_only"
      div_income_account: "Income:Vanguard:Dividends"
      capital_gains_account: "Income:Vanguard:Capital-Gains"

Investment account with cash holdings example
---------------------------------------------

    2014-01-01 open Assets:Investment:MyBank
      ofx_org: "Vanguard"
      ofx_broker_id: "vanguard.com"
      account_id: "XXXXXXXXXXXX"
      ofx_account_type: "securities_and_cash"
      div_income_account: "Income:MyBank:Dividends"
      interest_income_account: "Income:MyBank:Interest"
      fees_account: "Expenses:Financial:Investment-Fees:MyBank"
      capital_gains_account: "Income:MyBank:Capital-Gains"

401(k) account example
----------------------

    2014-01-01 open Assets:Retirement:Vanguard:Company401k
      ofx_org: "Vanguard"
      ofx_broker_id: "vanguard.com"
      account_id: "XXXXXXX"
      ofx_account_type: "securities_only"
      div_income_account: "Income:Vanguard:Dividends"
      fees_account: "Expenses:Financial:Investment-Fees:Vanguard"
      match_contribution_account: "Income:Company:Match"
      aftertax_account: "Assets:Retirement:Vanguard:Company401k:AfterTax"
      pretax_account: "Assets:Retirement:Vanguard:Company401k:PreTax"
      match_account: "Assets:Retirement:Vanguard:Company401k:Match"
      capital_gains_account: "Income:Vanguard:Capital-Gains"
      quantity_round_digits: 3

Checking, savings, or credit card account
-----------------------------------------

    2014-01-01 open Assets:Checking
      ofx_org: "MyBank"
      ofx_broker_id: "MyBank"
      ofx_account_type: "cash_only"
      account_id: "XXXXXXXX"

Dealing with missing ticker names
---------------------------------

By default, the TICKER tag specified for each security in the OFX file is used
as the currency name and sub-account name within the beancount file.  However,
sometimes the TICKER field is missing or not a valid Beancount currency
identifier.  In that case, you must manually specify the mapping from CUSIP to a
symbol name by creating a commodity directive in your beancount journal as
follows:

    1900-01-01 commodity VANGUARD-92204E878
      cusip: "92204E878"

Dealing with cash-equivalent securities
---------------------------------------

In some accounts, a part or all of the cash holdings may be held under a
cash-equivalent security (with a unit price of 1) corresponding to a Bank
Deposit Sweep Program, or something similar.  By default, this would result in a
bunch of spurious BUY/SELL transactions that convert between cash and this
cash-equivalent security.  To avoid this problem, for any cash-equivalent
security, you can create a commodity directive with a `equivalent_currency`
metadata field specifying the currency to which it is equivalent.  For example:

    1900-01-01 commodity QTSAQ
      equivalent_currency: "USD"

Imported transaction format
===========================

Cash-only transactions
----------------------

    2012-07-27 * "INVBANKTRAN - TRANSFERRED FROM VS X10-08144-1"
      Assets:Savings:Fidelity   115.83 USD
        date: 2012-07-27
        ofx_fitid: "X0000000000000000000002"
        ofx_memo: "TRANSFERRED FROM VS X10-08144-1"
        ofx_type: "INVBANKTRAN"
      Expenses:FIXME                -115.83 USD

The `ofx_fitid` metadata field associates the posting with the corresponding
entry in the OFX file.  The `ofx_name`/`ofx_memo` and `ofx_type` metadata fields
provide features for predicting the unknown `Expenses:FIXME` account.

Buy and sell transactions for `"securities_and_cash"` accounts
--------------------------------------------------------------

For BUY* and SELL* OFX transactions when the `account_type` is
`"securities_and_cash"`, Beancount transactions of the following form are
generated:

    2018-08-01 * "BUYSTOCK"
      Assets:Investment:MyBank:SDVMV         60.01318 SDVMV {67.141053527 USD}
        date: 2018-08-01
        ofx_fitid: "aedf1852aa39a54-623ee.4d104.5"
        ofx_type: "BUYSTOCK"
      Assets:Investment:MyBank:Cash          -4115.86 USD
        ofx_fitid: "aedf1852aa39a54-623ee.4d104.5"
      Expenses:Investment:MyBank:Fees         63.4869 USD
      Expenses:Investment:MyBank:Commission   23.0233 USD

    2018-08-01 * "SELLSTOCK"
      Assets:Investment:MyBank:EEBHF         -41.50416 EEBHF {} @ 83.661853593 USD
        date: 2018-08-01
        ofx_fitid: "4a5141ead2c672e8a559.65-80e.b"
        ofx_type: "SELLSTOCK"
      Income:MyBank:Capital-Gains:EEBHF
      Assets:Investment:MyBank:Cash            3382.60 USD
        ofx_fitid: "4a5141ead2c672e8a559.65-80e.b"
      Expenses:Investment:MyBank:Fees          31.9944 USD
      Expenses:Investment:MyBank:Commission    57.7239 USD

Note that the cost of the shares is not specified in the generated SELL
transaction, because the OFX transaction does not provide lot information.  For
the same reason, the amount for the `Capital-Gains` posting is also left
unspecified.  Instead, the Beancount booking mechanism will determine the cost
and the capital gains automatically.

Buy and sell transactions for `"securities_and_pending_cash"` accounts
----------------------------------------------------------------------

When the `account_type` is `"securities_and_pending_cash"`, exactly the same
Beancount transactions are generated as for an `account_type` of
`"securities_and_cash"`, but in addition a separate transfer transaction is also
created for posting to the `:Cash` account:

    2011-07-15 * "SELLMF - THIS IS A MEMO"
      Assets:Investment:Vanguard:VFINX     -42.123 VFINX {} @ 100 USD
        ofx_fitid: "01234567890.0123.07152011.0"
        date: 2011-07-15
        ofx_memo: "THIS IS A MEMO"
        ofx_type: "SELLMF"
      Income:Vanguard:Capital-Gains:VFINX
      Assets:Investment:Vanguard:Cash      4212.30 USD
        ofx_fitid: "01234567890.0123.07152011.0"

    2011-07-15 * "Transfer due to: SELLMF - THIS IS A MEMO"
      Assets:Investment:Vanguard:Cash  -4212.30 USD
        ofx_fitid: ">01234567890.0123.07152011.0"
        date: 2011-07-15
        ofx_memo: "THIS IS A MEMO"
        ofx_type_transfer: "SELLMF"
      Expenses:FIXME                    4212.30 USD

The `ofx_type_transfer` and `ofx_name`/`ofx_memo` metadata fields provide
features for predicting the unknown `Expenses:FIXME` account.

REINVEST transactions
---------------------

    2018-06-21 * "REINVEST - DIV"
      Assets:Retirement:Vanguard:Roth-IRA:TYCDT  31.704 TYCDT {2.94 USD}
        date: 2018-06-21
        ofx_fitid: "7c9254b784a.a9bd.edcfa27b.b"
        ofx_type: "REINVEST"
      Income:Vanguard:Dividends:TYCDT            -93.21 USD

INCOME transactions
-------------------

For `INCOME` OFX transactions, Beancount transactions of the following form are
generated when the `account_type` is `"securities_and_cash"`:

    2018-07-02 * "INCOME - DIV"
      Assets:Investment:MyBank:Cash   62.65 USD
        date: 2018-07-02
        ofx_fitid: "fd2561ce31fca077e.87f.0"
        ofx_type: "INCOME"
      Income:MyBank:Dividends:URMFO  -62.65 USD

Outgoing TRANSFER transactions
------------------------------

For outgoing `TRANSFER` transactions, Beancount transactions of the following
form are generated:

    2013-09-05 * "TRANSFER - MATCH - Investment Expense"
      Assets:Retirement:Vanguard:Company401k:Match:VANGUARD-92202V351  -0.04241 VANGUARD-92202V351 {} @ 39.37 USD
        date: 2013-09-05
        ofx_fitid: "1234567890123456795AAA"
        ofx_memo: "Investment Expense"
        ofx_type: "TRANSFER"
      Income:Vanguard:Capital-Gains:VANGUARD-92202V351
      Expenses:FIXME                                                       1.67 USD

The `ofx_memo`/`ofx_name` and `ofx_type` metadata fields provide features for
predicting the unknown `Expenses:FIXME` account.

Incoming TRANSFER transactions
------------------------------

For incoming `TRANSFER` transactions, Beancount transactions of the following
form are generated:

    2014-06-30 * "TRANSFER"
      Assets:Investment:ExampleOrg:BAR  6.800992 BAR {1 USD, "FIXME"}
        date: 2014-06-30
        ofx_fitid: "XXXXXXXXX2"
        ofx_type: "TRANSFER"
      Expenses:FIXME                                 -198.69 USD

Because the OFX transaction does not provide lot information, a dummy price and
label of `{1 USD, "FIXME"}` is specified in the transaction.  After adding the
generated transaction to your journal, you will need to manually replace the
posting with one or more postings specifying the correct lots.  In the modified
transaction, the `date` and `ofx_fitid` fields should also be duplicated on all
of the manually created postings, as shown below:

    2014-06-30 * "TRANSFER"
      Assets:Investment:ExampleOrg:BAR  6.000000 BAR {23.57 USD, 2012-03-07}
        date: 2014-06-30
        ofx_fitid: "XXXXXXXXX2"
        ofx_type: "TRANSFER"
      Assets:Investment:ExampleOrg:BAR  0.800992 BAR {29.41 USD, 2012-04-08}
        date: 2014-06-30
        ofx_fitid: "XXXXXXXXX2"
        ofx_type: "TRANSFER"
      Assets:Investment:AcmeOrg:BAR    -6.000000 BAR {23.57 USD, 2012-03-07}
        date: 2014-07-01
        ofx_fitid: "YYYYYYYYY7"
        ofx_type: "TRANSFER"
      Assets:Investment:AcmeOrg:BAR    -0.800992 BAR {29.41 USD, 2012-04-08}
        date: 2014-07-01
        ofx_fitid: "YYYYYYYYY7"
        ofx_type: "TRANSFER"

"""

import pickle
import re
from typing import Set, Tuple, Any, Dict, Union, List, Optional, NamedTuple
import os
import collections
import datetime
import tempfile

import bs4
from atomicwrites import atomic_write
from beancount.core.data import Transaction, Posting, Balance, Commodity, Price, Open, EMPTY_SET
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import D
from beancount.core.number import ZERO
from beancount.core.number import Decimal
from beancount.core.amount import Amount
from beancount.core.position import CostSpec
from beancount.core.number import MISSING

from ..posting_date import get_posting_date, POSTING_DATE_KEY
from . import ImportResult, Source, SourceResults, InvalidSourceReference
from ..journal_editor import JournalEditor
from ..matching import FIXME_ACCOUNT, CHECK_KEY
from ..training import ExampleKeyValuePairs


# find_child and parse_ofx_time were derived from implementation in beancount/ingest/importers/ofx.py{,test}
# Copyright (C) 2016  Martin Blais
# GNU GPLv2
def find_child(node, name, conversion=None):
    """Find a child under the given node and return its value.

    Args:
      node: A <STMTTRN> bs4.element.Tag.
      name: A string, the name of the child node.
      conversion: A callable object used to convert the value to a new data type.
    Returns:
      A string, or None.
    """
    child = node.find(name)
    if not child:
        return None
    if not child.contents:
        value = ''
    else:
        value = child.contents[0].strip()
    if conversion:
        value = conversion(value)
    return value


def parse_ofx_time(date_str):
    """Parse an OFX time string and return a datetime object.

    Args:
      date_str: A string, the date to be parsed.
    Returns:
      A datetime.datetime instance.
    """
    if len(date_str) < 14:
        return datetime.datetime.strptime(date_str[:8], '%Y%m%d')
    return datetime.datetime.strptime(date_str[:14], '%Y%m%d%H%M%S')


RawBalanceEntry = NamedTuple('RawBalanceEntry', [
    ('filename', str),
    ('date', datetime.date),
    ('uniqueid', Optional[str]),
    ('units', Optional[Decimal]),
    ('unitprice', Optional[Decimal]),
    ('inv401ksource', Optional[str]),
])

RawCashBalanceEntry = NamedTuple('RawCashBalanceEntry', [
    ('filename', str),
    ('date', datetime.date),
    ('number', Decimal),
])

RawTransactionEntry = NamedTuple('RawTransactionEntry', [
    ('filename', str),
    ('date', datetime.date),
    ('fitid', str),
    ('trantype', str),
    ('total', Decimal),
    ('incometype', Optional[str]),
    ('inv401ksource', Optional[str]),
    ('memo', Optional[str]),
    ('name', Optional[str]),
    ('trntype', Optional[str]),
    ('uniqueid', Optional[str]),
    ('units', Optional[Decimal]),
    ('unitprice', Optional[Decimal]),
    ('tferaction', Optional[str]),
    ('fees', Optional[Decimal]),
    ('commission', Optional[Decimal]),
    ('checknum', Optional[str]),
])

SecurityInfo = NamedTuple('SecurityInfo', [
    ('uniqueid', str),
    ('name', Optional[str]),
    ('ticker', Optional[str]),
])

def get_info(
        raw: Union[RawBalanceEntry, RawTransactionEntry]) -> Dict[str, Any]:
    return dict(
        type='application/x-ofx',
        filename=raw.filename,
    )


OFX_FITID_KEY = 'ofx_fitid'
FITID_TRANSFER_PREFIX = '>'
OFX_TYPE_KEY = 'ofx_type'
OFX_TYPE_TRANSFER_KEY = 'ofx_type_transfer'
OFX_MEMO_KEY = 'ofx_memo'
OFX_NAME_KEY = 'ofx_name'

CUSIP_KEY = 'cusip'

EQUIVALENT_CURRENCY = 'equivalent_currency'

# Used for generating unique features.
DESC_KEY = 'desc'

OFX_FEATURE_KEYS = [OFX_TYPE_KEY, OFX_TYPE_TRANSFER_KEY, OFX_MEMO_KEY, OFX_NAME_KEY]

cache_version_number = 4

valid_account_types = frozenset([
    'cash_only',
    'securities_only',
    'securities_and_cash',
    'securities_and_pending_cash',
])

inv401k_account_keys = {
    'PRETAX': ('pretax_account', 'PreTax'),
    'AFTERTAX': ('aftertax_account', 'AfterTax'),
    'MATCH': ('match_account', 'Match'),
    'PROFITSHARING': ('profitsharing_account', 'ProfitSharing'),
    'ROLLOVER': ('rollover_account', 'Rollover'),
}

# "Auxiliary" accounts are those given to
# SourceResults.add_skip_training_account because they are neither the source
# nor the target account, and should be ignored while building training
# examples.
AUX_CAPITAL_GAINS_KEY = 'capital_gains'
AUX_FEE_KEYS = ['fees', 'commission']
AUX_ACCOUNT_KEYS = [AUX_CAPITAL_GAINS_KEY] + AUX_FEE_KEYS

def get_aux_account_by_key(account: Open, key: str, results: SourceResults) -> str:
    """Like get_account_by_key. Ensures the account isn't used for training."""
    subaccount = account.meta.get(key)
    if subaccount is  None:
        raise KeyError('%s: must specify %s' % (account.account, key))
    if subaccount not in results.skip_training_accounts:
        raise ValueError('%s is an auxiliary account but was not added to SourceResults skip_traing_accounts; this should be done in PrepareState')
    return subaccount


def get_account_by_key(account: Open, key: str, default_suffix: Optional[str] = None) -> str:
    result = account.meta.get(key)
    if result is not None: return result
    if default_suffix is None:
        raise KeyError('%s: must specify %s' % (account.account, key))
    return account.account + ':' + default_suffix


def normalize_fraction(d: Decimal) -> Decimal:
    normalized = d.normalize()
    sign, digits, exponent = normalized.as_tuple()
    if exponent > 0:
        return Decimal((sign, tuple(digits) + (0, ) * exponent, 0))
    else:
        return normalized


def is_valid_commodity_name(ticker: Optional[str]) -> bool:
    return (ticker is not None and
            re.match(r'^[A-Z][A-Z0-9-]*', ticker) is not None)


def get_securities(soup: bs4.BeautifulSoup) -> List[SecurityInfo]:
    """Extract the list of securities from the OFX file."""

    seclistmsgsrsv = soup.find('seclistmsgsrsv1')
    if not seclistmsgsrsv:
        return []

    securities = []
    for secinfo in seclistmsgsrsv.find_all('secinfo'):
        uniqueid = find_child(secinfo, 'uniqueid')
        secname = find_child(secinfo, 'secname')
        ticker = find_child(secinfo, 'ticker')
        securities.append(
            SecurityInfo(uniqueid=uniqueid, name=secname, ticker=ticker))
    return securities


STOCK_BUY_SELL_TYPES = set(
    ['BUYMF', 'SELLMF', 'SELLSTOCK', 'BUYSTOCK', 'REINVEST'])
SELL_TYPES = set(['SELLMF', 'SELLSTOCK'])

RELATED_ACCOUNT_KEYS = ['aftertax_account', 'pretax_account', 'match_account']

# Tolerance allowed in transaction balancing.  In units of base currency used, e.g. USD.
TOLERANCE = 0.05

class ParsedOfxStatement(object):
    def __init__(self, seen_fitids, filename, securities_map, org, stmtrs):
        filename = os.path.abspath(filename)
        self.filename = filename
        self.securities_map = securities_map
        self.org = org
        account_id = self.account_id = find_child(stmtrs, 'acctid')
        self.broker_id = find_child(stmtrs, 'brokerid') or ''

        self.currency = find_child(stmtrs, 'curdef')
        raw_transactions = self.raw_transactions = []
        raw_balance_entries = self.raw_balance_entries = []
        raw_cash_balance_entries = self.raw_cash_balance_entries = []

        # Set of (date, uniqueid) pairs where there were transactions.

        # We don't emit balance entries for dates where there was activity, because there is no way
        # to represent the fact that the balance includes those transactions, and we don't want to
        # manually modify the balance by subtracting the transactions.
        security_activity_dates = self.security_activity_dates = set()
        cash_activity_dates = self.cash_activity_dates = set()

        self.ofx_id = account_ofx_id = (org, self.broker_id, account_id)

        for invtranlist in stmtrs.find_all(re.compile('invtranlist|banktranlist')):
            for tran in invtranlist.find_all(
                    re.compile(
                        '^(buymf|sellmf|reinvest|buystock|sellstock|buyopt|sellopt|transfer|income|invbanktran|stmttrn)$'
                    )):
                fitid = find_child(tran, 'fitid')
                date = parse_ofx_time(
                    find_child(tran, 'dttrade') or
                    find_child(tran, 'dtposted')).date()
                # We include the date along with the FITID because some financial institutions fail
                # to produce truly unique FITID values.  For example, National Financial Services
                # (Fidelity) sometimes produces duplicates when the amount is the same.
                full_fitid = (account_ofx_id, date, fitid)
                uniqueid = find_child(tran, 'uniqueid')
                if uniqueid is not None:
                    security_activity_dates.add((date, uniqueid))
                cash_activity_dates.add(date)

                if full_fitid in seen_fitids:
                    continue
                seen_fitids.add(full_fitid)

                trantype = tran.name.upper()
                if trantype == 'INVBANKTRAN' or trantype == 'STMTTRN':
                    total = find_child(tran, 'trnamt', D)
                else:
                    total = find_child(tran, 'total', D)

                raw = RawTransactionEntry(
                    trantype=trantype,
                    fitid=fitid,
                    date=date,
                    total=total,
                    incometype=find_child(tran, 'incometype'),
                    inv401ksource=find_child(tran, 'inv401ksource'),
                    memo=find_child(tran, 'memo'),
                    name=find_child(tran, 'name'),
                    trntype=find_child(tran, 'trntype'),
                    uniqueid=uniqueid,
                    units=find_child(tran, 'units', D),
                    unitprice=find_child(tran, 'unitprice', D),
                    tferaction=find_child(tran, 'tferaction'),
                    fees=find_child(tran, 'fees', D),
                    commission=find_child(tran, 'commission', D),
                    checknum=find_child(tran, 'checknum'),
                    filename=filename)
                raw_transactions.append(raw)

        for inv_bal in stmtrs.find_all('invbal'):
            availcash = find_child(inv_bal, 'availcash', D)
            self.availcash = availcash

            for bal in inv_bal.find_all('bal'):
                if find_child(bal, 'value', D) == availcash:
                    date = find_child(bal, 'dtasof', parse_ofx_time)
                    if date is not None:
                        date = date.date()
                        raw_cash_balance_entries.append(
                            RawCashBalanceEntry(
                                date=date, number=availcash, filename=filename))
                        break

        for bal in stmtrs.find_all('ledgerbal'):
            bal_amount_str = find_child(bal, 'balamt')
            if not bal_amount_str.strip(): continue
            bal_amount = D(bal_amount_str)
            date = find_child(bal, 'dtasof', parse_ofx_time).date()
            raw_cash_balance_entries.append(
                RawCashBalanceEntry(
                    date=date, number=bal_amount, filename=filename))


        for invposlist in stmtrs.find_all('invposlist'):
            for invpos in invposlist.find_all('invpos'):
                time_str = find_child(invpos, 'dtpriceasof')
                t = parse_ofx_time(time_str)
                date = t.date()
                raw_balance_entries.append(
                    RawBalanceEntry(
                        date=date,
                        uniqueid=find_child(invpos, 'uniqueid'),
                        units=find_child(invpos, 'units', D),
                        unitprice=find_child(invpos, 'unitprice', D),
                        inv401ksource=find_child(invpos, 'inv401ksource'),
                        filename=filename))

    def get_entries(self, prepare_state):
        account = prepare_state.ofx_id_to_account.get(self.ofx_id)
        results = prepare_state.results
        if account is None:
            results.add_warning(
                'No account matching OFX ORG, BROKERID, ACCTID triplet: %r.  Known accounts: %r' %
                (self.ofx_id, prepare_state.ofx_id_to_account.keys()))
            return

        account_type = account.meta.get('ofx_account_type')
        if account_type not in valid_account_types:
            results.add_error(
                'account %s has invalid ofx_account_type %r, must be one of %r'
                % (account.account, account_type, valid_account_types),
                account.meta)
        quantity_round_digits = account.meta.get('quantity_round_digits', None)
        has_securities = (account_type != 'cash_only')
        has_cash_account = (account_type != 'securities_only')
        has_real_cash_account = (account_type in ('securities_and_cash',
                                                  'cash_only'))
        has_transfer_cash_account = (account_type == 'securities_and_pending_cash')
        ignore_re = account.meta.get('ignore_transaction_regexp')
        if quantity_round_digits is not None:
            quantity_round_digits = int(quantity_round_digits)

        ofx_id = self.ofx_id

        matched_transactions = prepare_state.matched_transactions
        matched_cash_transactions = prepare_state.matched_cash_transactions
        matched_cash_transfer_transactions = prepare_state.matched_cash_transfer_transactions

        securities_map = self.securities_map
        commodities_by_cusip = prepare_state.commodities_by_cusip

        security_activity_dates = self.security_activity_dates
        cash_activity_dates = self.cash_activity_dates

        cash_securities_map = prepare_state.cash_securities_map

        def get_security(unique_id: str) -> Optional[str]:
            commodity = commodities_by_cusip.get(unique_id)
            if commodity is not None:
                return commodity
            if unique_id not in securities_map:
                results.add_error(
                    'Missing id for security %r.  You must specify it manually using a commodity directive with a cusip metadata field.'
                    % (unique_id, ))
                return None
            sec = securities_map[unique_id]
            ticker = sec.ticker
            if ticker is None:
                results.add_error(
                    'Missing ticker for security %r.  You must specify it manually using a commodity directive with a cusip metadata field.'
                    % (unique_id, ))
                return None
            if not is_valid_commodity_name(ticker):
                results.add_error(
                    'Ticker %r for security %r is not a valid commodity name.   You must specify it manually using a commodity directive with a cusip metadata field.'
                    % (ticker, unique_id))
            return ticker

        def get_subaccount(inv401ksource: Optional[str],
                           security: Optional[str] = None) -> str:
            suffix = ''
            if security is not None:
                suffix = ':' + security
            prefix = account.account
            if inv401ksource is not None:
                if (inv401ksource == 'OTHERNONVEST') or (inv401ksource == 'OTHERVEST'):
                    # For balance entries, OTHERNONVEST indicates an aggregate
                    # balance (at least with Vanguard).
                    return prefix
                key, default_suffix = inv401k_account_keys[inv401ksource]
                prefix = get_account_by_key(account, key, default_suffix)
            return prefix + suffix

        def get_subaccount_cash(inv401ksource: Optional[str] = None) -> str:
            return get_subaccount(inv401ksource, 'Cash' if has_securities else None)

        for raw in self.raw_transactions:
            match_key = (ofx_id, raw.date, raw.fitid)
            if has_real_cash_account:
                if has_securities and match_key in matched_transactions:
                    continue
                if match_key in matched_cash_transactions: continue
                security_transaction_exists = False
                cash_transaction_exists = False
            elif has_transfer_cash_account:
                security_transaction_exists = match_key in matched_transactions
                cash_transaction_exists = match_key in matched_cash_transactions
                cash_transfer_transaction_exists = match_key in matched_cash_transfer_transactions
                if (security_transaction_exists and cash_transaction_exists ==
                        cash_transfer_transaction_exists):
                    continue
            else:
                if match_key in matched_transactions: continue
                security_transaction_exists = False
                cash_transaction_exists = False

            total = raw.total

            unitprice = ZERO
            memo = raw.memo and ' '.join(raw.memo.split())
            name = raw.name and ' '.join(raw.name.split())

            if memo == 'Price as of date based on closing price':
                memo = None

            if name and memo and (memo.startswith(name) or memo.endswith(name)):
                # Remove redundant name field.
                name = None

            if name and memo and (name.startswith(memo) or name.startswith(memo)):
                # Remove redundant memo field
                memo = None

            narration = ' - '.join(
                filter(None,
                       (raw.trantype, raw.incometype, raw.inv401ksource, name,
                        memo)))

            if ignore_re and re.match(ignore_re, narration):
                continue
            entry = Transaction(
                meta=None,
                date=raw.date,
                flag=FLAG_OKAY,
                payee=None,
                narration=narration,
                tags=EMPTY_SET,
                links=EMPTY_SET,
                postings=[])

            base_meta = [(OFX_FITID_KEY, raw.fitid)]

            posting_meta = collections.OrderedDict(base_meta)

            posting_meta[POSTING_DATE_KEY] = raw.date
            posting_meta[OFX_TYPE_KEY] = raw.trantype

            if memo:
                posting_meta[OFX_MEMO_KEY] = memo

            if name:
                posting_meta[OFX_NAME_KEY] = name

            if raw.checknum:
                stripped_checknum = raw.checknum.lstrip('0')
                if stripped_checknum:
                    posting_meta[CHECK_KEY] = D(stripped_checknum)

            fee_total = ZERO
            for fee_key in AUX_FEE_KEYS:
                amount = getattr(raw, fee_key, None)
                if amount is not None and amount != ZERO:
                    fee_total += amount
                    entry.postings.append(
                        Posting(
                            account=get_aux_account_by_key(
                                account,
                                fee_key + '_account',
                                results),
                            units=Amount(number=amount, currency=self.currency),
                            cost=None,
                            price=None,
                            flag=None,
                            meta=None))

            cash_transfer_transaction_amount = None
            if raw.trantype == 'INCOME' or raw.trantype == 'INVBANKTRAN' or raw.trantype == 'STMTTRN':
                # Cash-only transaction

                if total == ZERO:
                    # Cash-only transaction with total amount of zero should be ignored.
                    # These are sometimes produced by Fidelity.
                    continue

                cur_amount = Amount(number=round(total, 2), currency=self.currency)

                if not has_cash_account:
                    #if raw.trantype != 'INCOME':
                    raise ValueError('Cash transaction not expected')
                    # entry.postings.append(
                    #     Posting(
                    #         meta=None,
                    #         account=FIXME_ACCOUNT,
                    #         units=cur_amount,
                    #         cost=None,
                    #         price=None,
                    #         flag=None,
                    #     ))
                else:
                    if has_transfer_cash_account:
                        cash_transfer_transaction_amount = cur_amount
                    cash_account_name = get_subaccount_cash(raw.inv401ksource)
                    results.add_account(cash_account_name)
                    entry.postings.append(
                        Posting(
                            meta=posting_meta,
                            account=cash_account_name,
                            units=cur_amount,
                            cost=None,
                            price=None,
                            flag=None,
                        ))
                if raw.trantype == 'INCOME':
                    assert total >= ZERO
                    security = get_security(raw.uniqueid)
                    if security is None:
                        continue

                if raw.trantype == 'INVBANKTRAN' and raw.trntype == 'DEBIT':
                    assert total < ZERO

                # Negate total for the cash posting created later.
                total = -total
            else:
                if not has_securities:
                    continue
                security = get_security(raw.uniqueid)
                if security is None:
                    continue
                if security in cash_securities_map:
                    continue
                units = normalize_fraction(raw.units)
                if quantity_round_digits is not None:
                    units = round(units, quantity_round_digits)
                if raw.unitprice is not None:
                    unitprice = normalize_fraction(raw.unitprice)

                cost_spec = None
                price = None
                is_sale = False
                if raw.trantype in SELL_TYPES or (raw.trantype == 'TRANSFER' and
                                                  units < ZERO):
                    is_sale = True
                    units = -abs(units)
                    # For sell transactions, rely on beancount to determine the matching lot.
                    cost_spec = CostSpec(
                        number_per=MISSING,
                        number_total=None,
                        currency=MISSING,
                        date=None,
                        label=None,
                        merge=False)
                    price = Amount(number=unitprice, currency=self.currency)
                elif raw.trantype == 'TRANSFER' and units > ZERO:
                    # Transfer in.
                    # OFX does not specify the lot information, so it will have to be manually fixed.
                    cost_spec = CostSpec(
                        number_per=D('1'),
                        number_total=None,
                        currency=self.currency,
                        date=None,
                        label='FIXME',
                        merge=False)
                elif raw.trantype == 'TRANSFER' and units == ZERO:
                    # Internal transfer, i.e. from after-tax to roth
                    continue
                else:
                    number_per_fix = unitprice
                    if abs(total + fee_total + (units * unitprice)) >= TOLERANCE:
                    	number_per_fix = normalize_fraction((abs(total)-abs(fee_total))/units)
                    cost_spec = CostSpec(
                        number_per=number_per_fix,
                        number_total=None,
                        currency=self.currency,
                        date=None,
                        label=None,
                        merge=False)

                if raw.tferaction == 'OUT':
                    assert units < ZERO

                security_account_name = get_subaccount(raw.inv401ksource,
                                                       security)
                results.add_account(security_account_name)
                entry.postings.append(
                    Posting(
                        meta=posting_meta,
                        account=security_account_name,
                        units=Amount(number=units, currency=security),
                        cost=cost_spec,
                        price=price,
                        flag=None,
                    ))

                if is_sale:
                    # Add capital gains posting.
                    entry.postings.append(
                        Posting(
                            meta=None,
                            account=get_aux_account_by_key(
                                account,
                                AUX_CAPITAL_GAINS_KEY + '_account',
                                results) + ':' + security,
                            units=MISSING,
                            cost=None,
                            price=None,
                            flag=None,
                        ))

            # Compute total amount.
            if raw.trantype == 'TRANSFER':
                assert total is None
                if unitprice != ZERO:
                    total = -(units * unitprice)
            elif raw.trantype in STOCK_BUY_SELL_TYPES:
                assert total is not None
                if raw.trantype in SELL_TYPES:
                    total = abs(total)
                else:
                    total = -abs(total)

            # Create cash posting.
            external_account_name = None
            external_meta = None

            if total is not None:
                cash_amount = Amount(
                    number=round(total, 2), currency=self.currency)
            else:
                cash_amount = MISSING

            if raw.trantype == 'INCOME' or raw.trantype == 'REINVEST':
                external_account_name = get_account_by_key(
                    account, '%s_income_account' %
                    raw.incometype.lower()) + ':' + security
            elif raw.trantype == 'TRANSFER':
                # Incoming transfers will always have to be manually fixed to include correct lots
                # Try to predict account
                external_account_name = FIXME_ACCOUNT
            elif raw.trantype == 'INVBANKTRAN' or raw.trantype == 'STMTTRN':
                external_account_name = FIXME_ACCOUNT
            elif (raw.trantype == 'BUYMF' or raw.trantype == 'BUYSTOCK') and raw.inv401ksource is not None:
                account_key = '%s_contribution_account' % raw.inv401ksource.lower()
                external_account_name = account.meta.get(account_key)
            if external_account_name is None:
                if has_cash_account:
                    external_account_name = get_subaccount_cash(raw.inv401ksource)
                    results.add_account(external_account_name)
                    external_meta = collections.OrderedDict(base_meta)
                    if has_transfer_cash_account:
                        cash_transfer_transaction_amount = cash_amount
                else:
                    external_account_name = FIXME_ACCOUNT

            entry.postings.append(
                Posting(
                    account=external_account_name,
                    units=cash_amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=external_meta))

            if not security_transaction_exists and not cash_transaction_exists:
                results.add_pending_entry(
                    ImportResult(
                        date=entry.date, entries=[entry], info=get_info(raw)))
            if cash_transfer_transaction_amount is not None and not cash_transfer_transaction_exists:
                assert cash_amount is not MISSING
                transfer_meta = collections.OrderedDict(posting_meta)
                transfer_meta[OFX_FITID_KEY] = FITID_TRANSFER_PREFIX + raw.fitid
                transfer_meta.pop(OFX_TYPE_KEY, None)
                transfer_meta[OFX_TYPE_TRANSFER_KEY] = raw.trantype

                transfer_entry = Transaction(
                    meta=None,
                    date=raw.date,
                    flag=FLAG_OKAY,
                    payee=None,
                    narration='Transfer due to: ' + narration,
                    tags=EMPTY_SET,
                    links=EMPTY_SET,
                    postings=[
                        Posting(
                            account=get_subaccount_cash(raw.inv401ksource),
                            units=-cash_transfer_transaction_amount,
                            cost=None,
                            price=None,
                            flag=None,
                            meta=transfer_meta,
                        ),
                        Posting(
                            account=FIXME_ACCOUNT,
                            units=cash_transfer_transaction_amount,
                            cost=None,
                            price=None,
                            flag=None,
                            meta=None,
                        ),
                    ])
                results.add_pending_entry(
                    ImportResult(
                        date=transfer_entry.date,
                        entries=[transfer_entry],
                        info=get_info(raw)))

        for raw in self.raw_cash_balance_entries:
            entry = Balance(
                date=raw.date,
                meta=None,
                account=get_subaccount_cash(),
                amount=Amount(number=raw.number, currency=self.currency),
                tolerance=None,
                diff_amount=None,
            )
            results.add_pending_entry(
                ImportResult(
                    date=raw.date, entries=[entry], info=get_info(raw)))

        for raw in self.raw_balance_entries:
            security = get_security(raw.uniqueid)
            if security is None:
                continue
            associated_currency = cash_securities_map.get(security)
            if associated_currency is not None:
                if raw.date not in cash_activity_dates:
                    entry = Balance(
                        date=raw.date,
                        meta=None,
                        account=get_subaccount_cash(raw.inv401ksource),
                        amount=Amount(
                            number=round(raw.units + self.availcash, 2),
                            currency=associated_currency),
                        tolerance=None,
                        diff_amount=None,
                    )
                    results.add_pending_entry(
                        ImportResult(
                            date=raw.date, entries=[entry], info=get_info(raw)))
            else:
                security_account_name = get_subaccount(raw.inv401ksource,
                                                       security)
                results.add_account(security_account_name)
                if (raw.date, raw.uniqueid) not in security_activity_dates:
                    entry = Balance(
                        date=raw.date,
                        meta=None,
                        account=security_account_name,
                        amount=Amount(number=raw.units, currency=security),
                        tolerance=None,
                        diff_amount=None,
                    )
                    results.add_pending_entry(
                        ImportResult(
                            date=raw.date, entries=[entry], info=get_info(raw)))
                price_entry = Price(
                    date=raw.date,
                    meta=None,
                    currency=security,
                    amount=Amount(number=raw.unitprice, currency=self.currency))
                results.add_pending_entry(
                    ImportResult(
                        date=raw.date,
                        entries=[price_entry],
                        info=get_info(raw)))


class ParsedOfxFile(object):
    def __init__(self, seen_fitids, filename):
        self.filename = filename
        parsed_statements = self.parsed_statements = []

        with open(filename, 'rb') as f:
            contents = f.read()
        # A byte string passed to BeautifulSoup is assumed to be UTF-8
        soup = bs4.BeautifulSoup(contents, 'html.parser')

        # Get the description of securities used in this file.
        securities_map = {s.uniqueid: s for s in get_securities(soup)}

        org = find_child(soup, 'org') or ''

        # For each statement.
        for stmtrs in soup.find_all(re.compile('.*stmtrs$')):
            parsed_statements.append(
                ParsedOfxStatement(
                    seen_fitids=seen_fitids,
                    filename=filename,
                    securities_map=securities_map,
                    org=org,
                    stmtrs=stmtrs))


def get_account_map(accounts):
    account_to_ofx_id = dict()
    ofx_id_to_account = dict()
    cash_accounts = set()
    for entry in accounts.values():
        meta = entry.meta
        if meta is None:
            continue
        org = entry.meta.get('ofx_org')
        broker_id = entry.meta.get('ofx_broker_id')
        account_id = entry.meta.get('account_id')
        if org is None or broker_id is None or account_id is None:
            continue
        ofx_id = (org, broker_id, account_id)
        account_to_ofx_id[entry.account] = ofx_id
        ofx_id_to_account[ofx_id] = entry
        ofx_account_type = entry.meta.get('ofx_account_type')
        if ofx_account_type == 'cash_only':
            cash_accounts.add(entry.account)
        elif ofx_account_type in ('securities_and_cash', 'securities_and_pending_cash'):
            cash_accounts.add(entry.account + ':Cash')
            for account_key, default_suffix in inv401k_account_keys.values():
                cash_accounts.add(meta.get(account_key, entry.account + ':' + default_suffix) + ':Cash')

        for key in RELATED_ACCOUNT_KEYS:
            other = entry.meta.get(key)
            if other is not None:
                account_to_ofx_id[other] = ofx_id
    return account_to_ofx_id, ofx_id_to_account, cash_accounts


FullFitid = Tuple[str, datetime.date, str]

def prune_valid_duplicates(matches: List[Tuple[Transaction, Posting]]) -> List[Tuple[Transaction, Posting]]:
    """Remove from the list of matches duplicate postings with the account,
    ofx_fitid, and currency.

    These postings may have been manually specified for a TRANSFER transaction.
    """
    seen = set()  # type: Set[Tuple[int, str, str]]

    def should_include(match: Tuple[Transaction, Posting]) -> bool:
        posting = match[1]
        if posting.units is MISSING: return True
        key = (id(match[0]), posting.account, posting.units.currency)
        if key in seen: return False
        seen.add(key)
        return True

    return [x for x in matches if should_include(x)]

class PrepareState(object):
    def __init__(self, source: 'OfxSource', journal: JournalEditor,
                 results: SourceResults) -> None:
        self.source = source
        self.journal = journal
        self.account_to_ofx_id, self.ofx_id_to_account, self.cash_accounts = get_account_map(
            journal.accounts)

        results.add_accounts(self.account_to_ofx_id.keys())
        for account in self.ofx_id_to_account.values():
            for key in AUX_ACCOUNT_KEYS:
                subaccount = account.meta.get(key + '_account')
                if subaccount is not None:
                   results.add_skip_training_account(subaccount)

        self.commodities_by_cusip = dict()  # type: Dict[str, str]
        self.cash_securities_map = dict() # type: Dict[str, str]
        self.matched_transactions = dict(
        )  # type: Dict[FullFitid, List[Tuple[Transaction, Posting]]]
        self.matched_cash_transactions = dict(
        )  # type: Dict[FullFitid, List[Tuple[Transaction, Posting]]]
        self.matched_cash_transfer_transactions = dict(
        )  # type: Dict[FullFitid, List[Tuple[Transaction, Posting]]]
        self.results = results

        self._process_journal_entries()

    def get_accounts_and_entries(self):
        for parsed_file in self.source.parsed_files:
            for statement in parsed_file.parsed_statements:
                statement.get_entries(self)

    def _process_journal_entries(self):
        source_fitids = self.source.source_fitids
        matched_transactions = self.matched_transactions
        cash_accounts = self.cash_accounts
        matched_cash_transactions = self.matched_cash_transactions
        matched_cash_transfer_transactions = self.matched_cash_transfer_transactions
        account_to_ofx_id = self.account_to_ofx_id
        commodities_by_cusip = self.commodities_by_cusip
        results = self.results
        for entry in self.journal.all_entries:
            if isinstance(entry, Transaction):
                last_lineno = None
                for posting in entry.postings:
                    meta = posting.meta
                    if meta is None: continue
                    # Skip duplicated postings due to booking.
                    new_lineno = meta['lineno']
                    if new_lineno is not None and new_lineno == last_lineno:
                        continue
                    last_lineno = new_lineno
                    fitid = meta.get(OFX_FITID_KEY, None)
                    if fitid is None: continue
                    ofx_id = find_ofx_id_for_account(posting.account, account_to_ofx_id)
                    if ofx_id is None:
                        continue
                    results.add_account(posting.account)
                    date = get_posting_date(entry, posting)
                    fitid_transfer = None  # type: Optional[str]
                    if fitid.startswith(FITID_TRANSFER_PREFIX):
                        fitid_transfer = fitid = fitid[len(
                            FITID_TRANSFER_PREFIX):]
                    full_fitid = (ofx_id, date, fitid)
                    if posting.account in cash_accounts:
                        if fitid_transfer is not None:
                            matched = matched_cash_transfer_transactions
                        else:
                            matched = matched_cash_transactions
                    else:
                        if fitid_transfer is not None:
                            results.add_error(
                                'A %s starting with %r must only be specified on Cash accounts.'
                                % OFX_FITID_KEY, FITID_TRANSFER_PREFIX,
                                posting.meta)
                            continue
                        matched = matched_transactions
                    matched.setdefault(full_fitid, []).append((entry, posting))
            elif isinstance(entry, Commodity):
                if CUSIP_KEY in entry.meta:
                    commodities_by_cusip[entry.meta[CUSIP_KEY]] = entry.currency
                if EQUIVALENT_CURRENCY in entry.meta:
                    self.cash_securities_map[entry.currency] = entry.meta[EQUIVALENT_CURRENCY]

        for matched in (matched_transactions, matched_cash_transactions,
                        matched_cash_transfer_transactions):
            for full_fitid, transactions in matched.items():
                excess_number = len(transactions) - (full_fitid in source_fitids)
                if excess_number == 0: continue
                transactions = prune_valid_duplicates(transactions)
                excess_number = len(transactions) - (full_fitid in source_fitids)
                if excess_number == 0: continue
                results.add_invalid_reference(
                    InvalidSourceReference(excess_number, transactions))


def find_ofx_id_for_account(account, account_to_ofx_id):
    """Find the OFX id corresponding to account or one of its parents,
    searching at most two parents. This is particularly needed for 401(k)
    sub-accounts. For example, this will search
    'Assets:Vanguard:401k:PreTax:VGI1' then 'Assets:Vanguard:401k:PreTax' then
    'Assets:Vanguard:401k'.
    """
    for i in range(3):
        if i != 0:
            account = account.rsplit(':', 1)[0]
        ofx_id = account_to_ofx_id.get(account)
        if ofx_id is not None:
            return ofx_id
    return None


class OfxSource(Source):
    def __init__(self,
                 ofx_filenames: List[str],
                 cache_filename: Optional[str] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.ofx_filenames = [os.path.realpath(x) for x in ofx_filenames]
        self.source_fitids = set()  # type: Set[FullFitid]
        self.parsed_files = []  # type: List[ParsedOfxFile]
        cached_ofx_filenames = set()  # type: Set[str]
        if cache_filename is not None:
            # Try to read cache
            try:
                with open(cache_filename, 'rb') as cache_f:
                    cache_data = pickle.load(cache_f)
                    version = cache_data['version']
                    if version != cache_version_number:
                        raise RuntimeError('invalid version')
                    cached_ofx_filenames.update(
                        s.filename for s in cache_data['parsed_files'])
                    if not cached_ofx_filenames.issubset(set(ofx_filenames)):
                        raise RuntimeError('filenames are not subset')
                    parsed_files = cache_data['parsed_files']
                    self.source_fitids.update(cache_data['source_fitids'])
                    self.parsed_files.extend(parsed_files)
            except:
                import traceback
                traceback.print_exc()
                self.log_status('ofx: Not using OFX cache due to an error')

        for filename in ofx_filenames:
            if filename in cached_ofx_filenames:
                continue
            self.log_status('ofx: loading %s' % filename)
            self.parsed_files.append(
                ParsedOfxFile(self.source_fitids, filename))

        if cache_filename is not None:
            cache_data = {
                'version': cache_version_number,
                'source_fitids': self.source_fitids,
                'parsed_files': self.parsed_files
            }
            with atomic_write(cache_filename, mode='wb', overwrite=True) as wcache_f:
                pickle.dump(cache_data, wcache_f)

    def get_example_key_value_pairs(self, transaction: Transaction,
                                    posting: Posting) -> ExampleKeyValuePairs:
        result = dict()  # type: ExampleKeyValuePairs
        if posting.meta is None:
            return result
        meta = posting.meta
        for key in OFX_FEATURE_KEYS:
            value = meta.get(key)
            if value is not None:
                result[key] = value

        if OFX_MEMO_KEY in result and OFX_NAME_KEY not in result:
            result[DESC_KEY] = result[OFX_MEMO_KEY]
        elif OFX_NAME_KEY in result and OFX_MEMO_KEY not in result:
            result[DESC_KEY] = result[OFX_NAME_KEY]

        return result

    def is_posting_cleared(self, posting: Posting):
        if posting.meta is None:
            return False
        return OFX_FITID_KEY in posting.meta

    def prepare(self, journal: JournalEditor, results: SourceResults):
        state = PrepareState(self, journal, results)
        state.get_accounts_and_entries()

    @property
    def name(self):
        return 'ofx'


def load(spec, log_status):
    return OfxSource(log_status=log_status, **spec)


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('ofx_file')
    args = ap.parse_args()
    source_fitids = set()  # type: Set[Any]
    result = ParsedOfxFile(source_fitids, args.ofx_file)
